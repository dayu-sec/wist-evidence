//! 一份证据的索引行 + 成品产物的标识与对象键。
//!
//! 这里**没有正文字段**，也不该有：正文只以对象形式存在 COS 里（§5.1 / §5.2）。

use chrono::{DateTime, Utc};

use super::kind::{Kind, RetentionClass, Sensitivity, Verdict};

/// 写入方对"这份产物有没有兜底副本"的声明（§10.2）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    /// 默认：生产方失败不重试（sink 自带的 3 次重试是投递机制，不是必达承诺）。
    BestEffort,
    /// 生产方声明本地还有副本，或产物已落两处。
    Confirmed,
}

impl Durability {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::BestEffort => "best_effort",
            Self::Confirmed => "confirmed",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "confirmed" => Self::Confirmed,
            _ => Self::BestEffort,
        }
    }
}

/// 索引行状态。`missing` / `purged` 由对账与清理作业写，写入路径只会产生 `stored`。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactState {
    Stored,
    Missing,
    Superseded,
    Purged,
}

impl ArtifactState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Stored => "stored",
            Self::Missing => "missing",
            Self::Superseded => "superseded",
            Self::Purged => "purged",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value {
            "missing" => Self::Missing,
            "superseded" => Self::Superseded,
            "purged" => Self::Purged,
            _ => Self::Stored,
        }
    }
}

/// 索引行。字段与 `migrations/0001_evidence_artifact.sql` 一一对应。
#[derive(Debug, Clone, serde::Serialize)]
pub struct Artifact {
    pub artifact_id: String,
    pub tenant_id: String,
    pub customer_id: String,
    pub kind: Kind,
    pub subtype: Option<String>,
    pub subject_type: String,
    pub subject_id: String,
    pub gateway_id: Option<String>,
    pub agent_id: Option<String>,
    pub command_id: Option<String>,
    pub dedupe_key: String,
    pub schema_id: String,
    pub sensitivity: Sensitivity,
    pub retention_class: RetentionClass,
    pub bucket: String,
    pub object_key: String,
    pub size_bytes: i64,
    pub sha256: String,
    pub verdict: Verdict,
    pub marker_ids: Vec<String>,
    pub source_ref: Option<String>,
    pub source_sha256: Option<String>,
    pub durability: Durability,
    pub reported_at: DateTime<Utc>,
    pub stored_at: DateTime<Utc>,
    pub retention_until: DateTime<Utc>,
    pub legal_hold: bool,
    pub state: ArtifactState,
}

/// 生成 `art_` + 26 位 Crockford Base32（ULID 形状）。
///
/// 为什么不引一个 ULID 依赖：需要的只是「时间有序 + 不透明 + 同一套字符集」。
/// 取 UUID v7 的 128 位（前 48 位是毫秒时间戳），按 ULID 的字母表编码即可；
/// 时钟回拨时 UUID v7 自身保证不重复，这里不必再算单调计数器。
pub fn new_artifact_id() -> String {
    const ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
    let raw = uuid::Uuid::now_v7();
    let mut n = u128::from_be_bytes(*raw.as_bytes());
    let mut buf = [b'0'; 26];
    for slot in buf.iter_mut().rev() {
        *slot = ALPHABET[(n & 0x1f) as usize];
        n >>= 5;
    }
    let mut out = String::with_capacity(30);
    out.push_str("art_");
    // 字母表全是 ASCII，编码结果一定是合法 UTF-8。
    out.push_str(std::str::from_utf8(&buf).expect("crockford base32 is ascii"));
    out
}

/// 对象键：`{tenant}/{kind}/{yyyy}/{mm}/{artifact_id}.{ext}.zst`（§5.2）。
///
/// 租户在最前是为了 STS 的 `{tenant}/*` 前缀策略能直接表达隔离。
/// 键里**不编码查询维度**（subject / topic 都靠索引查）。
pub fn object_key(
    tenant_id: &str,
    kind: Kind,
    artifact_id: &str,
    reported_at: DateTime<Utc>,
) -> Result<String, KeyError> {
    if !is_safe_segment(tenant_id) {
        return Err(KeyError::UnsafeSegment("tenant_id"));
    }
    if !is_safe_segment(artifact_id) {
        return Err(KeyError::UnsafeSegment("artifact_id"));
    }
    Ok(format!(
        "{}/{}/{}/{}/{}.{}.zst",
        tenant_id,
        kind.as_str(),
        reported_at.format("%Y"),
        reported_at.format("%m"),
        artifact_id,
        kind.object_ext()
    ))
}

/// 单个键段的安全判据：**服务端拼键**，拒绝一切能让键跑到别人目录的输入（§15）。
pub fn is_safe_segment(value: &str) -> bool {
    !value.is_empty()
        && value != "."
        && value != ".."
        && !value.contains('/')
        && !value.contains('\\')
        && !value.chars().any(|c| c.is_control())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    UnsafeSegment(&'static str),
}

impl std::fmt::Display for KeyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsafeSegment(field) => write!(f, "unsafe object key segment: {field}"),
        }
    }
}

impl std::error::Error for KeyError {}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    fn at() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 1, 12, 34, 56).unwrap()
    }

    #[test]
    fn artifact_ids_are_ulid_shaped_and_time_ordered() {
        let a = new_artifact_id();
        let b = new_artifact_id();
        assert_eq!(a.len(), 30, "art_ + 26 chars");
        assert!(a.starts_with("art_"));
        assert!(
            a[4..]
                .chars()
                .all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c))
        );
        // UUID v7 前 48 位是时间戳 → 字典序近似时间序。
        assert!(a < b, "{a} should sort before {b}");
    }

    #[test]
    fn object_key_layout_is_tenant_first() {
        let key = object_key("t-001", Kind::InvestigationReport, "art_X", at()).unwrap();
        assert_eq!(key, "t-001/investigation_report/2026/11/art_X.json.zst");
    }

    #[test]
    fn unsafe_key_segments_are_rejected() {
        for tenant in ["", ".", "..", "../other", "a/b", "a\\b", "a\nb"] {
            assert!(
                object_key(tenant, Kind::InvestigationReport, "art_X", at()).is_err(),
                "{tenant:?} must be rejected"
            );
        }
        assert!(
            object_key("t-001", Kind::InvestigationReport, "../x", at()).is_err(),
            "artifact id must be a safe segment too"
        );
    }
}
