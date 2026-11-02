//! 闭集词表：`kind` / `sensitivity` / `verdict` / `retention_class`。
//!
//! 三条纪律（对应设计稿 §4.1 / §5.1）：
//! - **只增不改名**：语义变了就开新 `kind`，不给旧名字换含义；
//! - **敏感度是有序的**：授权按「调用方上限 ≥ 对象档位」比较，所以顺序本身是契约；
//! - **保留期只由 kind × sensitivity 推导**，不由写入方声明（否则一句 `retention=1d`
//!   就能把合规证据删掉）。

use serde::{Deserialize, Serialize};

use super::artifact::Durability;

/// 一份产物是什么。与设计稿 §4.1 的清单一一对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    /// 调查报告（agentd 的 ` IVSREP: ` 帧）。
    InvestigationReport,
    /// 网关体检报告（gwlinkd 的 `wparse run diagnose`）。
    DiagnoseReport,
    /// 命令执行日志 / 任务记录。
    CommandLog,
    /// `ActionPlan` 的原始输出。
    ActionOutput,
    /// 升级前置 / 兼容性检查。
    UpgradePreflight,
    /// 支持包。
    SupportBundle,
    /// 合规基线扫描。
    ComplianceScan,
    /// 分析 / AI 派生物（跟随 `source_ref`，不得长于来源）。
    Derived,
}

impl Kind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvestigationReport => "investigation_report",
            Self::DiagnoseReport => "diagnose_report",
            Self::CommandLog => "command_log",
            Self::ActionOutput => "action_output",
            Self::UpgradePreflight => "upgrade_preflight",
            Self::SupportBundle => "support_bundle",
            Self::ComplianceScan => "compliance_scan",
            Self::Derived => "derived",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "investigation_report" => Self::InvestigationReport,
            "diagnose_report" => Self::DiagnoseReport,
            "command_log" => Self::CommandLog,
            "action_output" => Self::ActionOutput,
            "upgrade_preflight" => Self::UpgradePreflight,
            "support_bundle" => Self::SupportBundle,
            "compliance_scan" => Self::ComplianceScan,
            "derived" => Self::Derived,
            _ => return None,
        })
    }

    /// 数据面帧标记 → `kind`。
    ///
    /// wparse 的 OML 把帧标记原样带出来（`IVSREP` / `OBSFACT` / …），只有**属于证据**的
    /// 那一个才映射；其余帧不该打到这个端点（`None` → 400，而不是猜一个 kind）。
    pub fn from_frame_marker(marker: &str) -> Option<Self> {
        let marker = marker.trim().trim_end_matches(':').to_ascii_uppercase();
        match marker.as_str() {
            "IVSREP" => Some(Self::InvestigationReport),
            _ => None,
        }
    }

    /// 对象键里的扩展名段（正文是什么形态）。
    pub const fn object_ext(self) -> &'static str {
        match self {
            Self::CommandLog | Self::ActionOutput => "log",
            Self::SupportBundle => "tar",
            _ => "json",
        }
    }
}

/// 正文的敏感度档。**有序**：`rank()` 越大越敏感。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Sensitivity {
    Metrics,
    Paths,
    CommandLines,
    Identities,
    SecretMetadata,
    SecurityPosture,
}

impl Sensitivity {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Metrics => "metrics",
            Self::Paths => "paths",
            Self::CommandLines => "commandlines",
            Self::Identities => "identities",
            Self::SecretMetadata => "secret_metadata",
            Self::SecurityPosture => "security_posture",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value.trim().to_ascii_lowercase().as_str() {
            "metrics" => Self::Metrics,
            "paths" => Self::Paths,
            "commandlines" | "command_lines" => Self::CommandLines,
            "identities" => Self::Identities,
            "secret_metadata" => Self::SecretMetadata,
            "security_posture" => Self::SecurityPosture,
            _ => return None,
        })
    }

    /// 授权比较用的序。**这个顺序是契约**：调用方的 `max_sensitivity` 必须 ≥ 对象档位。
    pub const fn rank(self) -> u8 {
        match self {
            Self::Metrics => 0,
            Self::Paths => 1,
            Self::CommandLines => 2,
            Self::Identities => 3,
            Self::SecretMetadata => 4,
            Self::SecurityPosture => 5,
        }
    }
}

/// 结论。只收闭集值；正文里的自由文本摘要不进索引（§5.1）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Ok,
    Warn,
    Fail,
    Unknown,
}

impl Verdict {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ok => "ok",
            Self::Warn => "warn",
            Self::Fail => "fail",
            Self::Unknown => "unknown",
        }
    }

    pub fn parse(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "ok" | "pass" | "normal" => Self::Ok,
            "warn" | "warning" | "tight" => Self::Warn,
            "fail" | "failed" | "error" => Self::Fail,
            _ => Self::Unknown,
        }
    }
}

/// 保留档：写入时定型，清理作业只按 `retention_until` 走。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionClass {
    R7d,
    R30d,
    R90d,
    R180d,
}

impl RetentionClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::R7d => "r7d",
            Self::R30d => "r30d",
            Self::R90d => "r90d",
            Self::R180d => "r180d",
        }
    }

    pub const fn days(self) -> i64 {
        match self {
            Self::R7d => 7,
            Self::R30d => 30,
            Self::R90d => 90,
            Self::R180d => 180,
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "r7d" => Self::R7d,
            "r30d" => Self::R30d,
            "r90d" => Self::R90d,
            "r180d" => Self::R180d,
            _ => return None,
        })
    }

    /// `kind × sensitivity → 保留档`（§8.1）。
    ///
    /// `derived` 跟随来源，不在这里推导：没有 `source_ref` 就不该落库。
    pub fn resolve(kind: Kind, sensitivity: Sensitivity) -> Option<Self> {
        Some(match kind {
            Kind::InvestigationReport => match sensitivity {
                Sensitivity::Metrics => Self::R180d,
                Sensitivity::Paths => Self::R30d,
                Sensitivity::CommandLines => Self::R7d,
                Sensitivity::Identities => Self::R30d,
                Sensitivity::SecretMetadata => Self::R7d,
                Sensitivity::SecurityPosture => Self::R90d,
            },
            Kind::DiagnoseReport => Self::R30d,
            Kind::CommandLog => Self::R30d,
            Kind::ActionOutput => Self::R90d,
            Kind::UpgradePreflight => Self::R30d,
            Kind::SupportBundle => Self::R90d,
            Kind::ComplianceScan => Self::R180d,
            Kind::Derived => return None,
        })
    }
}

/// 写入方对「有没有兜底副本」的声明（§10.2）。
pub fn durability_from_str(value: &str) -> Durability {
    match value.trim().to_ascii_lowercase().as_str() {
        "confirmed" => Durability::Confirmed,
        _ => Durability::BestEffort,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_marker_maps_only_evidence_frames() {
        assert_eq!(
            Kind::from_frame_marker("IVSREP"),
            Some(Kind::InvestigationReport)
        );
        assert_eq!(
            Kind::from_frame_marker("IVSREP:"),
            Some(Kind::InvestigationReport)
        );
        // 非证据帧不能被猜成某个 kind。
        assert_eq!(Kind::from_frame_marker("OBSFACT"), None);
        assert_eq!(Kind::from_frame_marker(""), None);
    }

    #[test]
    fn sensitivity_order_matches_the_authorization_contract() {
        assert!(Sensitivity::Metrics < Sensitivity::Paths);
        assert!(Sensitivity::Paths < Sensitivity::CommandLines);
        assert!(Sensitivity::SecurityPosture.rank() > Sensitivity::Identities.rank());
    }

    #[test]
    fn retention_follows_kind_and_sensitivity() {
        assert_eq!(
            RetentionClass::resolve(Kind::InvestigationReport, Sensitivity::CommandLines),
            Some(RetentionClass::R7d)
        );
        assert_eq!(
            RetentionClass::resolve(Kind::InvestigationReport, Sensitivity::Metrics),
            Some(RetentionClass::R180d)
        );
        assert_eq!(
            RetentionClass::resolve(Kind::DiagnoseReport, Sensitivity::Paths),
            Some(RetentionClass::R30d)
        );
        // 派生物跟随来源，不能自行推导。
        assert_eq!(
            RetentionClass::resolve(Kind::Derived, Sensitivity::Paths),
            None
        );
    }

    #[test]
    fn kind_round_trips_through_text() {
        for kind in [
            Kind::InvestigationReport,
            Kind::DiagnoseReport,
            Kind::CommandLog,
            Kind::ActionOutput,
            Kind::UpgradePreflight,
            Kind::SupportBundle,
            Kind::ComplianceScan,
            Kind::Derived,
        ] {
            assert_eq!(Kind::parse(kind.as_str()), Some(kind));
        }
    }
}
