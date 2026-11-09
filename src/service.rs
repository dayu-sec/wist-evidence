//! 写与读的用例层：所有"该不该写 / 该不该给"的判断都在这里，HTTP 层只做搬运。
//!
//! 与设计稿的对应关系：
//! - §6.2 顺序与幂等 —— [`EvidenceService::write_simple`]
//! - §7.2 授权模型 —— [`EvidenceService::metadata`] / [`EvidenceService::presign`]
//! - §5.2 对象元数据 —— 写入时一次性带齐七个自描述字段

use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::Serialize;

use crate::domain::Artifact;
use crate::domain::artifact::{Durability, new_artifact_id, object_key};
use crate::domain::kind::{Kind, RetentionClass, Sensitivity, Verdict};
use crate::infra::{
    Index, IndexError, ListFilter, ObjectBackend, ObjectMeta, ObjectStore, StoreError,
};

/// 短签名有效期上限（D8）：预签名 URL 是 bearer token，必须短命 + 单对象。
pub const MAX_PRESIGN_TTL: Duration = Duration::from_secs(60);

/// 一次列表最多返回多少行（避免把浏览器/索引拖死）。
pub const MAX_LIST_LIMIT: usize = 200;

/// 写探针的保留标记：探针产物的 `subject_id` 固定是这个值，每日清理按它筛。
pub const PROBE_SUBJECT: &str = "_probe";

/// 写探针的现状：给 `/healthz` 看，也让星象台的「接口可用性」页能区分
/// 「端口通」与「真的写了一笔」。
#[derive(Debug, Clone, Default, Serialize)]
pub struct ProbeStatus {
    /// 上一次探针是否成功；服务刚起、还没探过时是 `null`。
    pub last_ok: Option<bool>,
    /// 上一次探针时间（UTC，RFC3339 / 毫秒时间戳两种都给：前者给人看，后者给前端算年龄）。
    pub last_at: Option<DateTime<Utc>>,
    pub last_at_ms: Option<u64>,
    pub last_latency_ms: Option<u64>,
    /// 连续失败次数（成功即归零）。
    pub consecutive_failures: u32,
    /// 累计清除了多少条过期探针。
    pub purged_total: u64,
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
    /// 正文超过上限（413）。
    TooLarge,
    /// 后端失败（5xx）。
    Backend(String),
}

#[derive(Debug)]
pub enum ReadError {
    /// 未知 id、跨租户、无权限、已过期 —— 一律 404，不泄露"存在但不能看"。
    NotFound,
    /// 调用方没有读权限（也是 404，见 §7.2）。
    Forbidden,
    /// 后端失败（5xx）。
    Backend(String),
    /// 当前对象存储实现不支持这个动作（例如本地目录存储没有预签名）。
    Unsupported(String),
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

#[derive(Debug, Clone, Serialize)]
pub struct ListPage {
    pub artifacts: Vec<Artifact>,
    pub next_cursor: Option<String>,
}

#[derive(Clone)]
pub struct EvidenceService {
    index: Arc<dyn Index>,
    objects: Arc<dyn ObjectStore>,
    bucket: String,
    allowed_kinds: Vec<Kind>,
    max_body_bytes: usize,
    /// `Some` = 写探针已启用（由启动流程调 `enable_probe`）；`None` = 没开。
    probe: Option<Arc<RwLock<ProbeStatus>>>,
}

/// **三字段形状**的写入请求：`cust_id` / `cmd_id` / `content`（`subject` 可选）。
///
/// `kind` / `schema_id` / `sensitivity` 由调用层（HTTP 层）按部署配置填好 ——
/// 数据面只负责说清「谁的、哪次动作、产物是什么」。
#[derive(Debug, Clone)]
pub struct SimpleWrite {
    pub customer_id: String,
    pub command_id: String,
    /// 主体标识：默认就是 `cust_id`，探针用保留值 `_probe`。
    pub subject_id: String,
    pub subject_type: &'static str,
    pub kind: Kind,
    pub schema_id: String,
    pub sensitivity: Sensitivity,
    /// 证据正文（原样字节的 UTF-8 文本）。
    pub content: String,
}

/// 归一化后的写请求：校验与提交都只看这一份。
struct PreparedWrite {
    /// 客户就是租户：它同时是对象键第一段和索引分区键。
    customer_id: String,
    command_id: Option<String>,
    subject_id: String,
    subject_type: &'static str,
    kind: Kind,
    subtype: Option<String>,
    schema_id: String,
    sensitivity: Sensitivity,
    verdict: Verdict,
    marker_ids: Vec<String>,
    source_ref: Option<String>,
    content: String,
    durability: Durability,
    gateway_id: Option<String>,
    reported_at: DateTime<Utc>,
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
            probe: None,
        }
    }

    /// 打开写探针的状态位。幂等；没打开的部署 `/healthz` 不会报 probe 块。
    pub fn enable_probe(&mut self) {
        self.probe = Some(Arc::new(RwLock::new(ProbeStatus::default())));
    }

    /// 读探针现状；没启用时返回 `None`。
    pub fn probe_status(&self) -> Option<ProbeStatus> {
        self.probe
            .as_ref()
            .map(|probe| probe.read().expect("probe status poisoned").clone())
    }

    /// 对象后端类型（自述用）：`/healthz` 的 `objects.kind`，上游据此避开一次无用的签名请求。
    pub fn objects_backend(&self) -> ObjectBackend {
        self.objects.backend()
    }

    /// **三字段写入**：`cust_id` / `cmd_id` / `content`（其余维度由调用层按部署配置填好）。
    ///
    /// 数据面只需要说清三件事：谁的、哪次动作、产物是什么。`kind` / `schema_id` /
    /// `sensitivity` 这些"我们自己的口径"不该让每个接入方都学一遍。
    pub async fn write_simple(&self, request: SimpleWrite) -> Result<WriteOutcome, WriteError> {
        if !self.allowed_kinds.contains(&request.kind) {
            return Err(WriteError::KindNotAllowed(request.kind.as_str()));
        }
        let prepared = PreparedWrite {
            customer_id: request.customer_id,
            command_id: Some(request.command_id),
            subject_id: request.subject_id,
            subject_type: request.subject_type,
            kind: request.kind,
            subtype: None,
            schema_id: request.schema_id,
            sensitivity: request.sensitivity,
            verdict: Verdict::Unknown,
            marker_ids: Vec::new(),
            source_ref: None,
            content: request.content,
            durability: Durability::BestEffort,
            gateway_id: None,
            reported_at: Utc::now(),
        };
        self.commit(prepared).await
    }

    /// 提交路径：校验 → 去重 → 对象 → 索引。
    async fn commit(&self, w: PreparedWrite) -> Result<WriteOutcome, WriteError> {
        if w.content.len() > self.max_body_bytes {
            return Err(WriteError::TooLarge);
        }
        if !crate::domain::artifact::is_safe_segment(&w.subject_id) {
            return Err(WriteError::BadRequest("evidence_subject_invalid"));
        }
        // 客户就是租户：它既是对象键的第一段，也是索引分区键。
        if !crate::domain::artifact::is_safe_segment(&w.customer_id) {
            return Err(WriteError::BadRequest("evidence_customer_invalid"));
        }

        let retention_class = RetentionClass::resolve(w.kind, w.sensitivity)
            .ok_or(WriteError::BadRequest("evidence_retention_undefined"))?;

        let sha256 = sha256_hex(w.content.as_bytes());

        let existing = match w.command_id.as_deref() {
            Some(key) => self
                .index
                .find_by_dedupe(&w.customer_id, w.kind, &w.subject_id, key)
                .await
                .map_err(index_error)?,
            None => None,
        };

        // 同键同 sha256 = 重投：不写对象、不改行（sink 会重试，这条路必须便宜）。
        if let Some(existing) = &existing
            && existing.sha256 == sha256
        {
            log::debug!(
                "evidence_write_duplicate customer={} artifact={}",
                w.customer_id,
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
        let key = object_key(&w.customer_id, w.kind, &object_id, w.reported_at)
            .map_err(|_| WriteError::BadRequest("evidence_object_key_invalid"))?;

        let compressed = zstd::encode_all(w.content.as_bytes(), 3)
            .map_err(|err| WriteError::Backend(format!("compress body: {err}")))?;

        let meta = object_meta(
            &w.customer_id,
            w.kind,
            &w.subject_id,
            &w.schema_id,
            w.sensitivity,
            &sha256,
            w.command_id.as_deref(),
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
            tenant_id: w.customer_id.clone(),
            customer_id: w.customer_id.clone(),
            kind: w.kind,
            subtype: w.subtype.clone(),
            subject_type: w.subject_type.to_string(),
            subject_id: w.subject_id.clone(),
            gateway_id: w.gateway_id.clone(),
            agent_id: Some(w.subject_id.clone()),
            command_id: w.command_id.clone(),
            dedupe_key: w.command_id.clone().unwrap_or_else(|| artifact_id.clone()),
            schema_id: w.schema_id.clone(),
            sensitivity: w.sensitivity,
            retention_class,
            bucket: self.bucket.clone(),
            object_key: key,
            size_bytes: w.content.len() as i64,
            sha256,
            verdict: w.verdict,
            marker_ids: w.marker_ids.clone(),
            source_ref: w.source_ref.clone(),
            source_sha256: None,
            durability: w.durability,
            reported_at: w.reported_at,
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
                    let again = match w.command_id.as_deref() {
                        Some(key) => self
                            .index
                            .find_by_dedupe(&w.customer_id, w.kind, &w.subject_id, key)
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
            durability: w.durability,
            duplicate: false,
            superseded,
        })
    }

    /// 元数据读（不含正文）。所有"不能给"的理由统一回 `NotFound`。
    ///
    /// `key` 可以是证据 id（`art_…`），也可以是调查命令 id（`cmd_id`）——
    /// 两者都能定位到同一行，定位规则见 `find_visible`。
    pub async fn metadata(
        &self,
        principal: &ReadPrincipal,
        key: &str,
    ) -> Result<Artifact, ReadError> {
        let artifact = self.find_visible(principal, key).await?;
        Ok(artifact)
    }

    /// 时间倒序列表。`tenant_id` 必须落在调用方的租户集合里。
    ///
    /// 过滤顺序是「先按索引取，再按可见性筛」：为了少查一次，按 `limit + 1` 取，
    /// 被敏感度/状态筛掉的行不占配额（代价是 `next_cursor` 可能比实际返回更靠后）。
    pub async fn list(
        &self,
        principal: &ReadPrincipal,
        tenant_id: &str,
        filter: &ListFilter,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ListPage, ReadError> {
        if !principal
            .tenant_ids
            .iter()
            .any(|tenant| tenant == tenant_id)
        {
            return Err(ReadError::NotFound);
        }
        let limit = limit.clamp(1, MAX_LIST_LIMIT);
        let rows = self
            .index
            .list(tenant_id, filter, limit + 1, cursor)
            .await
            .map_err(|err| ReadError::Backend(err.to_string()))?;

        let mut artifacts = Vec::with_capacity(rows.len());
        for row in rows.iter().take(limit) {
            // 写探针是自检产物，不该混进人的证据列表（按 id 仍可读）。
            if row.subject_id == PROBE_SUBJECT {
                continue;
            }
            if principal.has_scope(row.kind)
                && principal.max_sensitivity >= row.sensitivity
                && row.state == crate::domain::ArtifactState::Stored
                && row.retention_until > Utc::now()
            {
                artifacts.push(row.clone());
            }
        }

        let offset: usize = cursor.and_then(|raw| raw.parse().ok()).unwrap_or(0);
        let next_cursor = (rows.len() > limit).then(|| (offset + limit).to_string());
        Ok(ListPage {
            artifacts,
            next_cursor,
        })
    }

    /// 代理读正文：不经签名 URL，直接把原文（已解压）交给调用方。
    ///
    /// 这是 `objects.kind = file` 下**唯一**的读正文通路（本地目录没有预签名）；
    /// `s3` 模式下用作“证据桶不可达”时的兼任路径（§7.1）。
    /// 可见性判断与元数据读完全一致：任何一条不满足都是 404。
    pub async fn read_content(
        &self,
        principal: &ReadPrincipal,
        key: &str,
    ) -> Result<(Artifact, Vec<u8>), ReadError> {
        let artifact = self.find_visible(principal, key).await?;
        let stored = self
            .objects
            .get(&artifact.object_key)
            .await
            .map_err(|err| ReadError::Backend(err.to_string()))?
            .ok_or_else(|| {
                // 索引说有、对象没了：这是存储故障/待对账，不能答 404。
                ReadError::Backend(format!(
                    "object {} for artifact {} is missing",
                    artifact.object_key, artifact.artifact_id
                ))
            })?;
        let body = zstd::decode_all(stored.as_slice()).map_err(|err| {
            ReadError::Backend(format!("decompress {}: {err}", artifact.object_key))
        })?;
        // P0 的审计就是这一行（与 presign 同一口径）。
        log::info!(
            "evidence_content tenant={} artifact={} kind={} bytes={}",
            artifact.tenant_id,
            artifact.artifact_id,
            artifact.kind.as_str(),
            body.len()
        );
        Ok((artifact, body))
    }

    /// 短签名：单对象、≤ 60s。`key` 同元数据读：证据 id 或调查命令 id。
    pub async fn presign(
        &self,
        principal: &ReadPrincipal,
        key: &str,
        ttl: Duration,
    ) -> Result<PresignOutcome, ReadError> {
        let artifact = self.find_visible(principal, key).await?;
        let ttl = ttl.min(MAX_PRESIGN_TTL);
        let url = self
            .objects
            .presign_get(&artifact.object_key, ttl)
            .await
            .map_err(|err| match err {
                crate::infra::StoreError::Unsupported(action) => {
                    ReadError::Unsupported(action.to_string())
                }
                other => ReadError::Backend(other.to_string()),
            })?;
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

    /// 记录一次探针结果（由 [`crate::probe::ProbeRunner`] 在真正打完写端点后调用）。
    pub(crate) fn record_probe_result(&self, ok: bool, latency_ms: u64) {
        let Some(probe) = &self.probe else {
            return;
        };
        let now = Utc::now();
        let mut status = probe.write().expect("probe status poisoned");
        status.last_at = Some(now);
        status.last_at_ms = Some(now.timestamp_millis().max(0) as u64);
        status.last_latency_ms = Some(latency_ms);
        if ok {
            status.last_ok = Some(true);
            status.consecutive_failures = 0;
        } else {
            status.last_ok = Some(false);
            status.consecutive_failures = status.consecutive_failures.saturating_add(1);
        }
    }

    /// 清掉超过保留期的探针产物（对象 + 索引行），返回清除条数。
    /// 只动 `subject_id = _probe` 的行，不碰真证据。
    pub async fn purge_probes(
        &self,
        tenant_id: &str,
        retention: Duration,
    ) -> Result<u64, ReadError> {
        let cutoff = Utc::now() - chrono::Duration::from_std(retention).unwrap_or_default();
        let filter = ListFilter {
            kind: None,
            subject_id: Some(PROBE_SUBJECT.to_string()),
            command_id: None,
            since: None,
        };
        let mut purged = 0u64;
        loop {
            let rows = self
                .index
                .list(tenant_id, &filter, MAX_LIST_LIMIT, None)
                .await
                .map_err(|err| ReadError::Backend(err.to_string()))?;
            if rows.is_empty() {
                break;
            }
            let mut deleted = 0u64;
            for row in &rows {
                if row.reported_at >= cutoff {
                    continue;
                }
                // 先对象、后行：对象删失败就留着行，下次还能重试；反过来会留孤儿对象。
                self.objects
                    .delete(&row.object_key)
                    .await
                    .map_err(|err| ReadError::Backend(err.to_string()))?;
                self.index
                    .delete(tenant_id, &row.artifact_id)
                    .await
                    .map_err(|err| ReadError::Backend(err.to_string()))?;
                deleted += 1;
            }
            purged += deleted;
            if deleted == 0 || rows.len() < MAX_LIST_LIMIT {
                break;
            }
        }
        if let Some(probe) = &self.probe {
            probe.write().expect("probe status poisoned").purged_total += purged;
        }
        Ok(purged)
    }

    /// 定位一行可见的产物。
    ///
    /// `key` 先当**证据 id**（`art_…`，列表行里给内部导航用）找；找不到再当
    /// **调查命令 id**（`cmd_id`，发起调查的人手里只有这个）找。两套 id 不混用：
    /// 证据 id 不透明、命令 id 也不应该长成 `art_`，先后顺序只是实现细节。
    async fn find_visible(
        &self,
        principal: &ReadPrincipal,
        key: &str,
    ) -> Result<Artifact, ReadError> {
        let mut found = None;
        for tenant in &principal.tenant_ids {
            if let Some(row) = self
                .index
                .get(tenant, key)
                .await
                .map_err(|err| ReadError::Backend(err.to_string()))?
            {
                found = Some(row);
                break;
            }
        }
        if found.is_none() {
            for tenant in &principal.tenant_ids {
                if let Some(row) = self
                    .index
                    .find_by_command(tenant, key)
                    .await
                    .map_err(|err| ReadError::Backend(err.to_string()))?
                {
                    found = Some(row);
                    break;
                }
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
    match err {
        // 键不合法是服务端拼键的 bug：重试无用，按 400 回（不要把编程错误
        // 伪装成临时故障，让 sink 反复重投到 rescue 里）。
        StoreError::InvalidKey(message) => {
            log::error!("evidence_object_key_invalid: {message}");
            WriteError::BadRequest("evidence_object_key_invalid")
        }
        // 写路径目前没有实现会返回它；真出现也算能力/配置错，不当临时故障。
        StoreError::Unsupported(action) => {
            log::error!("evidence_write_unsupported: {action}");
            WriteError::BadRequest("evidence_write_unsupported")
        }
        StoreError::Backend(message) => WriteError::Backend(message),
    }
}
