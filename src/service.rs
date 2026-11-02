//! 写与读的用例层：所有"该不该写 / 该不该给"的判断都在这里，HTTP 层只做搬运。
//!
//! 与设计稿的对应关系：
//! - §6.2 顺序与幂等 —— [`EvidenceService::write`]
//! - §7.2 授权模型 —— [`EvidenceService::metadata`] / [`EvidenceService::presign`]
//! - §5.2 对象元数据 —— 写入时一次性带齐七个自描述字段

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::domain::Artifact;
use crate::domain::artifact::{Durability, new_artifact_id, object_key};
use crate::domain::kind::{Kind, RetentionClass, Sensitivity, Verdict, durability_from_str};
use crate::infra::{Index, IndexError, ObjectMeta, ObjectStore, StoreError};

/// 短签名有效期上限（D8）：预签名 URL 是 bearer token，必须短命 + 单对象。
pub const MAX_PRESIGN_TTL: Duration = Duration::from_secs(60);

/// sink 调用方身份。来自 `Authorization` token → 服务端映射，**不是**请求体自述。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SinkCaller {
    pub tenant_id: String,
    pub customer_id: String,
}

/// wparse `http_sink` 投递的一条记录。
///
/// 形状与网关 `api/ingest.rs` 收到的记录同源：信封字段 + `body`（契约对象的 JSON 文本）。
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SinkRecord {
    /// 帧标记（`IVSREP` / …）。只有证据帧的标记才映射到 `kind`。
    #[serde(default)]
    pub schema: String,
    /// 信封里的 agent（归属依据之一，**与正文自述交叉核对**）。
    #[serde(default)]
    pub agent: String,
    /// 可选：站点 / 网关标识（由数据面配置注入）。
    #[serde(default)]
    pub gateway: Option<String>,
    /// 观测时间（RFC3339）；缺省取服务端收到的时间。
    #[serde(default)]
    pub ts: Option<String>,
    /// 帧序号。P0 不参与判重（判重靠 `command_id`）。
    #[serde(default)]
    pub seq: Option<u64>,
    /// 敏感度档；缺省按 `paths`（与体检报告同档，保守）。
    #[serde(default)]
    pub sensitivity: Option<String>,
    /// `best_effort` / `confirmed`；缺省 `best_effort`。
    #[serde(default)]
    pub durability: Option<String>,
    /// 写入方自报的 sha256（可选）。与计算结果不一致 → `evidence_integrity_failed`。
    #[serde(default)]
    pub sha256: Option<String>,
    /// 契约对象原样 JSON 文本（与 `IVSREP` 帧 body 一致）。
    pub body: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct WriteOutcome {
    pub artifact_id: String,
    pub durability: Durability,
    /// 重投：同键同 `sha256`，没有产生任何副作用。
    pub duplicate: bool,
    /// 同键不同正文：这一行已被新内容替换（旧对象保留待清）。
    pub superseded: bool,
}

#[derive(Debug)]
pub enum WriteError {
    /// 请求本身不合法（400）。
    BadRequest(&'static str),
    /// `kind` 在词表里，但本次部署不允许（403）。
    KindNotAllowed(&'static str),
    /// 身份与内容不一致（403）。
    Forbidden(&'static str),
    /// 正文超过上限（413）。
    TooLarge,
    /// 自报 sha256 与计算结果不一致（422）。
    IntegrityFailed,
    /// 后端失败（5xx）。
    Backend(String),
}

#[derive(Debug)]
pub enum ReadError {
    /// 未知 id、跨租户、无权限、已过期 —— 一律 404，不泄露"存在但不能看"。
    NotFound,
    /// 调用方没有读权限（也是 404，见 §7.2）。
    Forbidden,
    Backend(String),
}

/// 读调用方。P0 由内网 TLS 终结层注入请求头；CA-I 落地后改成证书 claim（§9.5 / §19-10）。
#[derive(Debug, Clone)]
pub struct ReadPrincipal {
    pub tenant_ids: Vec<String>,
    pub scopes: Vec<String>,
    pub max_sensitivity: Sensitivity,
}

impl ReadPrincipal {
    pub fn has_scope(&self, kind: Kind) -> bool {
        self.scopes.iter().any(|scope| {
            scope == "evidence:read"
                || scope == "evidence:admin"
                || scope == &format!("evidence:read:{}", kind.as_str())
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct PresignOutcome {
    pub artifact_id: String,
    pub url: String,
    pub expires_in_seconds: u64,
    pub expires_at: DateTime<Utc>,
}

#[derive(Clone)]
pub struct EvidenceService {
    index: Arc<dyn Index>,
    objects: Arc<dyn ObjectStore>,
    bucket: String,
    allowed_kinds: Vec<Kind>,
    max_body_bytes: usize,
}

impl EvidenceService {
    pub fn new(
        index: Arc<dyn Index>,
        objects: Arc<dyn ObjectStore>,
        bucket: String,
        allowed_kinds: Vec<Kind>,
        max_body_bytes: usize,
    ) -> Self {
        Self {
            index,
            objects,
            bucket,
            allowed_kinds,
            max_body_bytes,
        }
    }

    /// 单条记录写入。批量由 HTTP 层逐条调用 —— 每条都要能独立成败。
    pub async fn write(
        &self,
        caller: &SinkCaller,
        record: SinkRecord,
    ) -> Result<WriteOutcome, WriteError> {
        let kind = Kind::from_frame_marker(&record.schema)
            .ok_or(WriteError::BadRequest("evidence_frame_not_evidence"))?;
        if !self.allowed_kinds.contains(&kind) {
            return Err(WriteError::KindNotAllowed(kind.as_str()));
        }

        if record.body.len() > self.max_body_bytes {
            return Err(WriteError::TooLarge);
        }

        let body = ReportBody::parse(&record.body)?;

        // 身份先于内容：subject 只从信封来，且必须是安全段（服务端拼键）。
        let subject_id = record.agent.trim();
        if !crate::domain::artifact::is_safe_segment(subject_id) {
            return Err(WriteError::BadRequest("evidence_subject_invalid"));
        }

        // 正文里的 tenant_id 只用于交叉核对，永不采信（D7）。
        if let Some(claimed) = body.tenant_id.as_deref()
            && claimed != caller.tenant_id
        {
            return Err(WriteError::Forbidden("evidence_tenant_mismatch"));
        }

        let sensitivity = match record.sensitivity.as_deref() {
            Some(raw) => Sensitivity::parse(raw)
                .ok_or(WriteError::BadRequest("evidence_sensitivity_unknown"))?,
            None => Sensitivity::Paths,
        };
        let retention_class = RetentionClass::resolve(kind, sensitivity)
            .ok_or(WriteError::BadRequest("evidence_retention_undefined"))?;

        let sha256 = sha256_hex(record.body.as_bytes());
        if let Some(claimed) = record.sha256.as_deref()
            && !claimed.eq_ignore_ascii_case(&sha256)
        {
            log::warn!(
                "evidence_integrity_failed tenant={} agent={subject_id} claimed={claimed} computed={sha256}",
                caller.tenant_id
            );
            return Err(WriteError::IntegrityFailed);
        }

        let reported_at = record
            .ts
            .as_deref()
            .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
            .map(|ts| ts.with_timezone(&Utc))
            .unwrap_or_else(Utc::now);

        let dedupe_key = body.command_id.clone();
        let existing = match dedupe_key.as_deref() {
            Some(key) => self
                .index
                .find_by_dedupe(&caller.tenant_id, kind, subject_id, key)
                .await
                .map_err(index_error)?,
            None => None,
        };

        // 同键同 sha256 = 重投：不写对象、不改行（sink 会重试，这条路必须便宜）。
        if let Some(existing) = &existing
            && existing.sha256 == sha256
        {
            log::debug!(
                "evidence_write_duplicate tenant={} artifact={}",
                caller.tenant_id,
                existing.artifact_id
            );
            return Ok(WriteOutcome {
                artifact_id: existing.artifact_id.clone(),
                durability: existing.durability,
                duplicate: true,
                superseded: false,
            });
        }

        // 对象键每次都换新的不透明 id：键不可覆盖（§5.2），否则"旧对象"就没了。
        let object_id = new_artifact_id();
        let key = object_key(&caller.tenant_id, kind, &object_id, reported_at)
            .map_err(|_| WriteError::BadRequest("evidence_object_key_invalid"))?;

        let compressed = zstd::encode_all(record.body.as_bytes(), 3)
            .map_err(|err| WriteError::Backend(format!("compress body: {err}")))?;

        let durability = record
            .durability
            .as_deref()
            .map(durability_from_str)
            .unwrap_or(Durability::BestEffort);

        let meta = object_meta(
            &caller.tenant_id,
            kind,
            subject_id,
            &body.schema_id,
            sensitivity,
            &sha256,
            body.command_id.as_deref(),
        );

        // D6：先对象、后索引。对象写成功但索引失败，只会留下可对账补回的孤儿对象。
        self.objects
            .put(&key, compressed, &meta)
            .await
            .map_err(store_error)?;

        let now = Utc::now();
        let artifact_id = existing
            .as_ref()
            .map(|row| row.artifact_id.clone())
            .unwrap_or_else(new_artifact_id);
        let artifact = Artifact {
            artifact_id: artifact_id.clone(),
            tenant_id: caller.tenant_id.clone(),
            customer_id: caller.customer_id.clone(),
            kind,
            subtype: body.topic.clone(),
            subject_type: "agent".to_string(),
            subject_id: subject_id.to_string(),
            gateway_id: record.gateway.clone(),
            agent_id: Some(subject_id.to_string()),
            command_id: body.command_id.clone(),
            dedupe_key: dedupe_key.clone().unwrap_or_else(|| artifact_id.clone()),
            schema_id: body.schema_id.clone(),
            sensitivity,
            retention_class,
            bucket: self.bucket.clone(),
            object_key: key,
            size_bytes: record.body.len() as i64,
            sha256,
            verdict: body.verdict,
            marker_ids: body.marker_ids,
            source_ref: body.source_ref.clone(),
            source_sha256: None,
            durability,
            reported_at,
            stored_at: now,
            retention_until: now + chrono::Duration::days(retention_class.days()),
            legal_hold: false,
            state: crate::domain::ArtifactState::Stored,
        };

        let superseded = match &existing {
            Some(_) => {
                self.index.update(&artifact).await.map_err(index_error)?;
                true
            }
            None => match self.index.insert(&artifact).await {
                Ok(()) => false,
                Err(IndexError::Conflict) => {
                    // 并发重投：重新读一次，按拿到的那行决定是重投还是替换。
                    let again = match dedupe_key.as_deref() {
                        Some(key) => self
                            .index
                            .find_by_dedupe(&caller.tenant_id, kind, subject_id, key)
                            .await
                            .map_err(index_error)?,
                        None => None,
                    };
                    match again {
                        Some(row) if row.sha256 == artifact.sha256 => {
                            return Ok(WriteOutcome {
                                artifact_id: row.artifact_id,
                                durability: row.durability,
                                duplicate: true,
                                superseded: false,
                            });
                        }
                        Some(_) => {
                            self.index.update(&artifact).await.map_err(index_error)?;
                            true
                        }
                        None => {
                            return Err(WriteError::Backend("index conflict without row".into()));
                        }
                    }
                }
                Err(err) => return Err(index_error(err)),
            },
        };

        Ok(WriteOutcome {
            artifact_id,
            durability,
            duplicate: false,
            superseded,
        })
    }

    /// 元数据读（不含正文）。所有"不能给"的理由统一回 `NotFound`。
    pub async fn metadata(
        &self,
        principal: &ReadPrincipal,
        artifact_id: &str,
    ) -> Result<Artifact, ReadError> {
        let artifact = self.find_visible(principal, artifact_id).await?;
        Ok(artifact)
    }

    /// 短签名：单对象、≤ 60s。
    pub async fn presign(
        &self,
        principal: &ReadPrincipal,
        artifact_id: &str,
        ttl: Duration,
    ) -> Result<PresignOutcome, ReadError> {
        let artifact = self.find_visible(principal, artifact_id).await?;
        let ttl = ttl.min(MAX_PRESIGN_TTL);
        let url = self
            .objects
            .presign_get(&artifact.object_key, ttl)
            .await
            .map_err(|err| ReadError::Backend(err.to_string()))?;
        // P0 的审计就是这一行（落库的 evidence_audit 表在 P1）。
        log::info!(
            "evidence_presign tenant={} artifact={} kind={} ttl_s={}",
            artifact.tenant_id,
            artifact.artifact_id,
            artifact.kind.as_str(),
            ttl.as_secs()
        );
        Ok(PresignOutcome {
            artifact_id: artifact.artifact_id,
            url,
            expires_in_seconds: ttl.as_secs(),
            expires_at: Utc::now() + chrono::Duration::from_std(ttl).unwrap_or_default(),
        })
    }

    async fn find_visible(
        &self,
        principal: &ReadPrincipal,
        artifact_id: &str,
    ) -> Result<Artifact, ReadError> {
        let mut found = None;
        for tenant in &principal.tenant_ids {
            if let Some(row) = self
                .index
                .get(tenant, artifact_id)
                .await
                .map_err(|err| ReadError::Backend(err.to_string()))?
            {
                found = Some(row);
                break;
            }
        }
        let artifact = found.ok_or(ReadError::NotFound)?;
        if !principal.has_scope(artifact.kind) {
            return Err(ReadError::NotFound);
        }
        if principal.max_sensitivity < artifact.sensitivity {
            return Err(ReadError::NotFound);
        }
        if artifact.state != crate::domain::ArtifactState::Stored {
            return Err(ReadError::NotFound);
        }
        if artifact.retention_until <= Utc::now() {
            return Err(ReadError::NotFound);
        }
        Ok(artifact)
    }
}

/// 七个自描述字段：没有它们，"索引是衍生物"就只是口号（§5.2）。
fn object_meta(
    tenant_id: &str,
    kind: Kind,
    subject_id: &str,
    schema_id: &str,
    sensitivity: Sensitivity,
    sha256: &str,
    command_id: Option<&str>,
) -> ObjectMeta {
    let mut meta = vec![
        ("tenant-id".to_string(), tenant_id.to_string()),
        ("kind".to_string(), kind.as_str().to_string()),
        ("subject-id".to_string(), subject_id.to_string()),
        ("schema-id".to_string(), schema_id.to_string()),
        ("sensitivity".to_string(), sensitivity.as_str().to_string()),
        ("sha256".to_string(), sha256.to_string()),
    ];
    if let Some(command_id) = command_id {
        meta.push(("command-id".to_string(), command_id.to_string()));
    }
    meta
}

/// `IVSREP` 正文里我们**只读闭集字段**；自由文本一律不解析、不入索引。
#[derive(Debug, Default, Deserialize)]
struct ReportBody {
    #[serde(default)]
    command_id: Option<String>,
    #[serde(default)]
    report_schema: Option<String>,
    #[serde(default)]
    topic: Option<String>,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    source_ref: Option<String>,
    #[serde(default)]
    summary: Option<SummaryBody>,
    #[serde(default)]
    findings: Vec<FindingBody>,
}

#[derive(Debug, Deserialize)]
struct SummaryBody {
    #[serde(default)]
    overall: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FindingBody {
    #[serde(default)]
    id: Option<String>,
}

impl ReportBody {
    fn parse(raw: &str) -> Result<ParsedBody, WriteError> {
        let body: ReportBody = serde_json::from_str(raw)
            .map_err(|_| WriteError::BadRequest("evidence_body_invalid"))?;
        let schema_id = body
            .report_schema
            .clone()
            .filter(|value| !value.trim().is_empty())
            .ok_or(WriteError::BadRequest("evidence_schema_missing"))?;
        // marker_ids 只收闭集标识（finding.id），去重保序，不携带值。
        let mut marker_ids: Vec<String> = Vec::new();
        for finding in &body.findings {
            if let Some(id) = finding.id.as_deref().map(str::trim)
                && !id.is_empty()
                && !marker_ids.iter().any(|existing| existing == id)
            {
                marker_ids.push(id.to_string());
            }
        }
        let verdict = body
            .summary
            .as_ref()
            .and_then(|summary| summary.overall.as_deref())
            .map(Verdict::parse)
            .unwrap_or(Verdict::Unknown);
        Ok(ParsedBody {
            command_id: body.command_id,
            schema_id,
            topic: body.topic,
            tenant_id: body.tenant_id,
            source_ref: body.source_ref,
            verdict,
            marker_ids,
        })
    }
}

#[derive(Debug)]
struct ParsedBody {
    command_id: Option<String>,
    schema_id: String,
    topic: Option<String>,
    tenant_id: Option<String>,
    source_ref: Option<String>,
    verdict: Verdict,
    marker_ids: Vec<String>,
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

fn index_error(err: IndexError) -> WriteError {
    WriteError::Backend(err.to_string())
}

fn store_error(err: StoreError) -> WriteError {
    WriteError::Backend(err.to_string())
}

/// 配置里的 `[sink.tokens]`：`token = { tenant_id, customer_id }`。
pub type TokenMap = BTreeMap<String, SinkCaller>;
