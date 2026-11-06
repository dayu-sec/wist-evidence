//! P0 契约测试：幂等、跨租户、键安全、短签名、同键新正文。
//!
//! 这些是设计稿 §18 里能在**不接真 COS / 真 PG** 的前提下验的部分；
//! 真实后端（COS 预签名语义、KMS）留到真机验收。

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use wist_evidence::api::{self, ReadState, SinkState};
use wist_evidence::domain::Kind;
use wist_evidence::infra::{
    FileObjectStore, MemoryIndex, MemoryObjectStore, ObjectBackend, ObjectMeta, ObjectStore,
    StoreError,
};
use wist_evidence::service::EvidenceService;

const CUST: &str = "c-001";

struct Harness {
    sink: Router,
    read: Router,
    index: Arc<MemoryIndex>,
    /// 断言用的具体后端：`None` 表示不是内存后端（file 直接看磁盘）。
    memory: Option<Arc<MemoryObjectStore>>,
    file: Option<Arc<FileObjectStore>>,
    /// 服务本体：写探针 / 清理的测试直接从这一层调，不绕 HTTP。
    service: Arc<EvidenceService>,
}

impl Harness {
    /// 已写入的对象数（两种后端同一口径：一个对象 + 一个 sidecar 只算一个）。
    fn object_count(&self) -> usize {
        match (&self.memory, &self.file) {
            (Some(memory), _) => memory.len(),
            (None, Some(file)) => walk_objects(file.root()),
            (None, None) => 0,
        }
    }

    fn object_meta(&self, key: &str) -> Option<ObjectMeta> {
        match (&self.memory, &self.file) {
            (Some(memory), _) => memory.meta_of(key),
            (None, Some(file)) => {
                let text =
                    std::fs::read_to_string(file.root().join(format!("{key}.meta.json"))).ok()?;
                let parsed: std::collections::BTreeMap<String, String> =
                    serde_json::from_str(&text).ok()?;
                Some(parsed.into_iter().collect())
            }
            (None, None) => None,
        }
    }

    /// 读回对象正文（内存后端直接读，file 后端解压），测试断言用。
    fn stored_body(&self, key: &str) -> Option<Vec<u8>> {
        if let Some(memory) = &self.memory {
            // 对象层存的是 zstd 压缩后的字节，两种后端都要解压后再断言。
            let compressed = memory.bytes_of(key)?;
            return zstd::decode_all(compressed.as_slice()).ok();
        }
        self.object_body(key)
    }

    /// 磁盘上的对象正文（仅 file 后端），已解压。
    fn object_body(&self, key: &str) -> Option<Vec<u8>> {
        let file = self.file.as_ref()?;
        let compressed = std::fs::read(file.root().join(key)).ok()?;
        zstd::decode_all(compressed.as_slice()).ok()
    }
}

fn walk_objects(root: &std::path::Path) -> usize {
    let mut count = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if !path.to_string_lossy().ends_with(".meta.json") {
                count += 1;
            }
        }
    }
    count
}

fn harness_with(
    index: Arc<MemoryIndex>,
    objects: Arc<dyn ObjectStore>,
    memory: Option<Arc<MemoryObjectStore>>,
    file: Option<Arc<FileObjectStore>>,
    probe_enabled: bool,
) -> Harness {
    let mut raw_service = EvidenceService::new(
        index.clone(),
        objects.clone(),
        "wist-evidence-test".to_string(),
        vec![Kind::InvestigationReport],
        4 * 1024 * 1024,
    );
    if probe_enabled {
        raw_service.enable_probe();
    }
    let service = Arc::new(raw_service);

    let sink_state = SinkState {
        service: service.clone(),
        default_kind: Kind::InvestigationReport,
        default_schema: "wist.evidence/v1".to_string(),
        default_sensitivity: wist_evidence::domain::kind::Sensitivity::Paths,
    };

    Harness {
        sink: api::sink_router(sink_state, 4 * 1024 * 1024),
        read: api::read_router(ReadState {
            service: service.clone(),
        }),
        index,
        memory,
        file,
        service,
    }
}

fn harness() -> Harness {
    let memory = Arc::new(MemoryObjectStore::new());
    harness_with(
        Arc::new(MemoryIndex::new()),
        memory.clone(),
        Some(memory),
        None,
        false,
    )
}

/// 默认部署形态（`objects.kind = file`）的端到端 harness。
fn file_harness(root: &std::path::Path) -> Harness {
    let file = Arc::new(FileObjectStore::open(root).expect("open file store"));
    harness_with(
        Arc::new(MemoryIndex::new()),
        file.clone(),
        None,
        Some(file),
        false,
    )
}

fn report(command_id: &str, overall: &str) -> String {
    json!({
        "command": "investigate",
        "topic": "disk",
        "report_schema": "wist.investigation.disk/v1",
        "summary": { "overall": overall },
        "findings": [{ "id": "disk.filesystems" }, { "id": "disk.top_dirs" }],
        "command_id": command_id,
    })
    .to_string()
}

/// 三字段写入记录：`cust_id` / `cmd_id` / `content`（+ 覆盖主体）。
/// `content` 给字符串，这样磁盘上的对象能直接和它逐字节比对。
fn record(cmd_id: &str, overall: &str) -> Value {
    json!({
        "cust_id": CUST,
        "cmd_id": cmd_id,
        "subject": "agent-7f3",
        "content": report(cmd_id, overall),
    })
}

async fn post_record(sink: &Router, record: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/internal/evidence")
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(&record).unwrap()))
        .unwrap();
    let response = sink.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn get_metadata(read: &Router, tenant: &str, artifact_id: &str) -> StatusCode {
    let request = Request::builder()
        .uri(format!("/internal/evidence/{artifact_id}"))
        .header("x-evidence-tenant", tenant)
        .header("x-evidence-scopes", "evidence:read")
        .header("x-evidence-max-sensitivity", "commandlines")
        .body(Body::empty())
        .unwrap();
    read.clone().oneshot(request).await.unwrap().status()
}

async fn list(
    read: &Router,
    tenant: &str,
    max_sensitivity: &str,
    query: &str,
) -> (StatusCode, Value) {
    let request = Request::builder()
        .uri(format!("/internal/evidence{query}"))
        .header("x-evidence-tenant", tenant)
        .header("x-evidence-scopes", "evidence:read")
        .header("x-evidence-max-sensitivity", max_sensitivity)
        .body(Body::empty())
        .unwrap();
    let response = read.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

async fn presign(read: &Router, artifact_id: &str, ttl: u64) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri(format!("/internal/evidence/{artifact_id}:presign"))
        .header("x-evidence-tenant", "c-001")
        .header("x-evidence-scopes", "evidence:read")
        .header("x-evidence-max-sensitivity", "paths")
        .header("content-type", "application/json")
        .body(Body::from(json!({ "ttl_seconds": ttl }).to_string()))
        .unwrap();
    let response = read.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

#[tokio::test]
async fn replaying_the_same_record_is_idempotent() {
    let h = harness();
    let record = record("cmd-1", "warn");

    let (status, first) = post_record(&h.sink, record.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let artifact_id = first["artifacts"][0]["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(first["duplicates"], 0);

    // sink 的 max_retries 会让重复投递成为常态：第二次必须是 200，且不产生新对象 / 新行。
    let (status, second) = post_record(&h.sink, record).await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["duplicates"], 1);
    assert_eq!(second["artifacts"][0]["artifact_id"], artifact_id);
    assert_eq!(second["artifacts"][0]["duplicate"], true);

    assert_eq!(h.index.len(), 1, "one dedupe key = one row");
    assert_eq!(h.object_count(), 1, "replay must not write a second object");
}

#[tokio::test]
async fn cross_tenant_read_is_404() {
    let h = harness();
    let (_, written) = post_record(&h.sink, record("cmd-2", "ok")).await;
    let artifact_id = written["artifacts"][0]["artifact_id"].as_str().unwrap();

    // 同租户能读。
    assert_eq!(
        get_metadata(&h.read, "c-001", artifact_id).await,
        StatusCode::OK
    );
    // 另一个租户：只能得到 404（不泄露"存在但不能看"）。
    assert_eq!(
        get_metadata(&h.read, "c-002", artifact_id).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn unsafe_subject_is_rejected() {
    let h = harness();
    // 空 `subject` 不是错误：缺省回退到 `cust_id`；但不安全段没商量。
    for subject in ["..", "../other", "a/b"] {
        let mut record = record("cmd-3", "ok");
        record["subject"] = json!(subject);
        let (status, body) = post_record(&h.sink, record).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "subject={subject:?} -> {body}"
        );
    }
    assert!(
        h.object_count() == 0,
        "rejected writes must not reach the object store"
    );
}

#[tokio::test]
async fn unsafe_customer_is_rejected() {
    let h = harness();
    for cust in ["..", "../other", "a/b", ""] {
        let mut record = record("cmd-3b", "ok");
        record["cust_id"] = json!(cust);
        let (status, body) = post_record(&h.sink, record).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "cust={cust:?} -> {body}");
        assert_eq!(body["error"]["code"], "evidence_customer_invalid", "{body}");
    }
}

#[tokio::test]
async fn missing_fields_are_400() {
    let h = harness();
    // 三字段都是必填：少了任何一个都不能默一个值应付过去。
    for broken in [
        json!({ "cmd_id": "cmd-x", "content": "x" }),
        json!({ "cust_id": CUST, "content": "x" }),
        json!({ "cust_id": CUST, "cmd_id": "cmd-x" }),
    ] {
        let (status, body) = post_record(&h.sink, broken.clone()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{broken} -> {body}");
    }
    assert_eq!(h.object_count(), 0);
}

#[tokio::test]
async fn presign_is_single_object_and_capped_at_60s() {
    let h = harness();
    let (_, written) = post_record(&h.sink, record("cmd-6", "warn")).await;
    let artifact_id = written["artifacts"][0]["artifact_id"].as_str().unwrap();

    let (status, signed) = presign(&h.read, artifact_id, 600).await;
    assert_eq!(status, StatusCode::OK, "{signed}");
    assert_eq!(signed["expires_in_seconds"], 60, "D8: cap at 60s");
    let url = signed["url"].as_str().unwrap();
    // 单对象：URL 指向的就是这一行的 object_key，没有通配前缀。
    let object_key = h.index.all().into_iter().next().unwrap().object_key;
    assert!(
        url.contains(&object_key),
        "url must point at {object_key}: {url}"
    );
    assert!(
        !url.contains('*'),
        "presign must not widen to a prefix: {url}"
    );
}

#[tokio::test]
async fn same_command_with_new_body_supersedes_the_row() {
    let h = harness();
    let (_, first) = post_record(&h.sink, record("cmd-7", "warn")).await;
    let artifact_id = first["artifacts"][0]["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, second) = post_record(&h.sink, record("cmd-7", "fail")).await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["artifacts"][0]["artifact_id"], artifact_id);
    assert_eq!(second["artifacts"][0]["superseded"], true);

    assert_eq!(h.index.len(), 1, "one dedupe key = one row");
    assert_eq!(
        h.object_count(),
        2,
        "old object is kept for cleanup, not overwritten"
    );
}

#[tokio::test]
async fn object_metadata_carries_the_rebuild_fields() {
    let h = harness();
    post_record(&h.sink, record("cmd-8", "ok")).await;

    let artifact = h.index.all().into_iter().next().unwrap();
    let meta = h.object_meta(&artifact.object_key).unwrap();
    for field in [
        "tenant-id",
        "kind",
        "subject-id",
        "schema-id",
        "sensitivity",
        "sha256",
        "command-id",
    ] {
        assert!(
            meta.iter().any(|(name, _)| name == field),
            "missing object meta {field}: {meta:?}"
        );
    }
}

#[tokio::test]
async fn list_returns_newest_first_and_respects_visibility() {
    let h = harness();
    post_record(&h.sink, record("cmd-a", "warn")).await;
    post_record(&h.sink, record("cmd-b", "ok")).await;

    let (status, page) = list(&h.read, "c-001", "paths", "").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let artifacts = page["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 2);
    assert!(page["next_cursor"].is_null());

    // 类型筛选
    let (_, filtered) = list(&h.read, "c-001", "paths", "?kind=diagnose_report").await;
    assert_eq!(filtered["artifacts"].as_array().unwrap().len(), 0);

    // 跨租户：身份里没有 t-002 的数据 → 空列表（不是 403，避免泄露存在性）。
    let (status, other) = list(&h.read, "c-002", "paths", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other["artifacts"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_hides_artifacts_above_the_caller_sensitivity() {
    let h = harness();
    post_record(&h.sink, record("cmd-c", "warn")).await; // sensitivity=paths

    let (status, page) = list(&h.read, "c-001", "metrics", "").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["artifacts"].as_array().unwrap().len(), 0);

    let (_, visible) = list(&h.read, "c-001", "paths", "").await;
    assert_eq!(visible["artifacts"].as_array().unwrap().len(), 1);
}

// ── objects.kind = file：默认部署形态的端到端契约 ─────────────────────────────
//
// 上面那些用例跑在内存后端上（它只为测试存在）。现场与测试栈用的是 file 后端，
// 所以「写下去的东西能不能在磁盘上被读回来」必须单独验一遍。

struct TempDirGuard(std::path::PathBuf);

impl TempDirGuard {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "wist-evidence-contract-{name}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test]
async fn file_backend_stores_body_and_metadata_on_disk() {
    let dir = TempDirGuard::new("write");
    let h = file_harness(dir.path());

    let (status, body) = post_record(&h.sink, record("cmd-file-1", "warn")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(h.object_count(), 1);

    let artifact = h.index.all().into_iter().next().unwrap();
    // 正文落盘、能解压回原文 —— 这是「本地文件可核对」的全部意义。
    let stored = h.object_body(&artifact.object_key).expect("object on disk");
    assert_eq!(
        String::from_utf8(stored).unwrap(),
        report("cmd-file-1", "warn")
    );

    // 七个自描述字段都在 sidecar 里，且与索引行一致（索引可从对象重建）。
    let meta = h.object_meta(&artifact.object_key).expect("sidecar");
    let fields: Vec<&str> = meta.iter().map(|(name, _)| name.as_str()).collect();
    for expected in [
        "tenant-id",
        "kind",
        "subject-id",
        "schema-id",
        "sensitivity",
        "sha256",
        "command-id",
    ] {
        assert!(
            fields.contains(&expected),
            "missing {expected} in {fields:?}"
        );
    }
    let sha256 = meta
        .iter()
        .find(|(name, _)| name == "sha256")
        .map(|(_, value)| value.clone())
        .unwrap();
    assert_eq!(sha256, artifact.sha256);
}

#[tokio::test]
async fn file_backend_replay_writes_one_object() {
    let dir = TempDirGuard::new("replay");
    let h = file_harness(dir.path());
    let record = record("cmd-file-2", "ok");

    let (_, first) = post_record(&h.sink, record.clone()).await;
    let (status, second) = post_record(&h.sink, record).await;

    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["duplicates"], 1);
    assert_eq!(
        second["artifacts"][0]["artifact_id"], first["artifacts"][0]["artifact_id"],
        "same dedupe key must reuse the artifact"
    );
    assert_eq!(h.object_count(), 1, "replay must not write a second file");
}

#[tokio::test]
async fn file_backend_supersede_keeps_the_old_object() {
    let dir = TempDirGuard::new("supersede");
    let h = file_harness(dir.path());

    post_record(&h.sink, record("cmd-file-3", "warn")).await;
    let (status, _) = post_record(&h.sink, record("cmd-file-3", "fail")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.index.len(), 1, "one dedupe key = one row");
    assert_eq!(h.object_count(), 2, "键不可覆盖：旧对象的文件要留着待清");
}

#[tokio::test]
async fn file_backend_presign_returns_501() {
    let dir = TempDirGuard::new("presign");
    let h = file_harness(dir.path());
    let (_, body) = post_record(&h.sink, record("cmd-file-4", "ok")).await;
    let artifact_id = body["artifacts"][0]["artifact_id"].as_str().unwrap();

    let (status, payload) = presign(&h.read, artifact_id, 60).await;

    assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{payload}");
    assert_eq!(payload["error"]["code"], "evidence_presign_unsupported");
    let message = payload["error"]["message"].as_str().unwrap_or_default();
    assert!(message.contains("objects.kind = s3"), "{message}");
    // 501 不能长得像一次成功的签发。
    assert!(payload.get("expires_in_seconds").is_none(), "{payload}");
    // 也不能把宿主路径漏出去。
    assert!(
        !message.contains(dir.path().to_string_lossy().as_ref()),
        "message leaks objects.dir: {message}"
    );
}

#[tokio::test]
async fn file_backend_presign_of_a_missing_artifact_is_404_not_501() {
    let dir = TempDirGuard::new("presign-missing");
    let h = file_harness(dir.path());

    let (status, payload) = presign(&h.read, "art_does_not_exist", 60).await;

    // 可见性判断在前：不存在 / 无权看 一律 404，不能被 501 抢答。
    assert_eq!(status, StatusCode::NOT_FOUND, "{payload}");
    assert_eq!(payload["error"]["code"], "evidence_not_found");
}

// ── 存储故障 / 非法键的错误分级 ──────────────────────────────────────────────

/// 恒失败的后端：用来验「先对象后索引」与错误码分级。
struct FailingStore {
    error: fn() -> StoreError,
}

#[async_trait::async_trait]
impl ObjectStore for FailingStore {
    fn backend(&self) -> ObjectBackend {
        ObjectBackend::Memory
    }

    async fn put(&self, _key: &str, _bytes: Vec<u8>, _meta: &ObjectMeta) -> Result<(), StoreError> {
        Err((self.error)())
    }

    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Err((self.error)())
    }

    async fn presign_get(
        &self,
        _key: &str,
        _ttl: std::time::Duration,
    ) -> Result<String, StoreError> {
        Err((self.error)())
    }

    async fn delete(&self, _key: &str) -> Result<(), StoreError> {
        Ok(())
    }
}

fn failing_harness(error: fn() -> StoreError) -> Harness {
    harness_with(
        Arc::new(MemoryIndex::new()),
        Arc::new(FailingStore { error }),
        None,
        None,
        false,
    )
}

#[tokio::test]
async fn backend_failure_is_502_and_leaves_no_index_row() {
    let h = failing_harness(|| StoreError::Backend("disk full".to_string()));

    let (status, body) = post_record(&h.sink, record("cmd-fail-1", "ok")).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{body}");
    assert_eq!(body["error"]["code"], "evidence_write_failed");
    // 细节只进日志：后端原文（含宿主路径 / errno）不能回到数据面。
    let rendered = body.to_string();
    assert!(
        !rendered.contains("disk full"),
        "response leaks backend detail: {rendered}"
    );
    // D6：先对象、后索引 —— 对象失败不能留下行。
    assert_eq!(h.index.len(), 0, "no row may survive a failed object write");
}

#[tokio::test]
async fn invalid_object_key_is_400_not_502() {
    let h = failing_harness(|| StoreError::InvalidKey("unsafe key".to_string()));

    let (status, body) = post_record(&h.sink, record("cmd-fail-2", "ok")).await;

    // 拼键错误是编程错误：重试不会变好，不能伪装成 5xx 让 sink 反复重投。
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "evidence_object_key_invalid");
    assert_eq!(h.index.len(), 0);
}

/// 写入正常、只有签名失败的后端：用来验读路径的 5xx 也不回显后端原文。
struct PresignFailsStore;

#[async_trait::async_trait]
impl ObjectStore for PresignFailsStore {
    fn backend(&self) -> ObjectBackend {
        ObjectBackend::Memory
    }

    async fn put(&self, _key: &str, _bytes: Vec<u8>, _meta: &ObjectMeta) -> Result<(), StoreError> {
        Ok(())
    }

    async fn get(&self, _key: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(None)
    }

    async fn presign_get(
        &self,
        _key: &str,
        _ttl: std::time::Duration,
    ) -> Result<String, StoreError> {
        Err(StoreError::Backend(
            "sign /var/lib/wist-evidence/objects/secret: permission denied".to_string(),
        ))
    }

    async fn delete(&self, _key: &str) -> Result<(), StoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn read_backend_failure_is_502_without_leaking_details() {
    let h = harness_with(
        Arc::new(MemoryIndex::new()),
        Arc::new(PresignFailsStore),
        None,
        None,
        false,
    );
    let (status, body) = post_record(&h.sink, record("cmd-leak-1", "ok")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let artifact_id = body["artifacts"][0]["artifact_id"].as_str().unwrap();

    let (status, payload) = presign(&h.read, artifact_id, 60).await;

    assert_eq!(status, StatusCode::BAD_GATEWAY, "{payload}");
    assert_eq!(payload["error"]["code"], "evidence_read_failed");
    let rendered = payload.to_string();
    assert!(
        !rendered.contains("/var/lib/wist-evidence"),
        "read response leaks host path: {rendered}"
    );
    assert!(
        !rendered.contains("permission denied"),
        "read response leaks errno detail: {rendered}"
    );
}

// ── 代理读正文：`GET /internal/evidence/{id}/content`（§7.2）────────────────

async fn get_content(
    read: &Router,
    tenant: &str,
    artifact_id: &str,
) -> (StatusCode, String, Vec<u8>) {
    let request = Request::builder()
        .uri(format!("/internal/evidence/{artifact_id}/content"))
        .header("x-evidence-tenant", tenant)
        .header("x-evidence-scopes", "evidence:read")
        .header("x-evidence-max-sensitivity", "commandlines")
        .body(Body::empty())
        .unwrap();
    let response = read.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let bytes = response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes()
        .to_vec();
    (status, content_type, bytes)
}

#[tokio::test]
async fn content_returns_the_original_body() {
    let dir = TempDirGuard::new("content");
    let h = file_harness(dir.path());
    let (_, body) = post_record(&h.sink, record("cmd-content-1", "warn")).await;
    let artifact_id = body["artifacts"][0]["artifact_id"].as_str().unwrap();

    let (status, content_type, bytes) = get_content(&h.read, "c-001", artifact_id).await;

    assert_eq!(status, StatusCode::OK);
    assert!(
        content_type.starts_with("application/json"),
        "{content_type}"
    );
    // 对象层是 zstd 压缩的；代理读要回**原文**，否则前端拿到的是二进制。
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        report("cmd-content-1", "warn")
    );
}

#[tokio::test]
async fn content_respects_tenant_and_sensitivity() {
    let dir = TempDirGuard::new("content-visibility");
    let h = file_harness(dir.path());
    let (_, body) = post_record(&h.sink, record("cmd-content-2", "ok")).await;
    let artifact_id = body["artifacts"][0]["artifact_id"].as_str().unwrap();

    // 跨租户：404，不泄露「存在但不能看」。
    let (status, _, bytes) = get_content(&h.read, "c-002", artifact_id).await;
    assert_eq!(
        status,
        StatusCode::NOT_FOUND,
        "{}",
        String::from_utf8_lossy(&bytes)
    );

    // 不存在：同样 404。
    let (status, _, _) = get_content(&h.read, "c-001", "art_missing").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn content_of_a_vanished_object_is_502_not_404() {
    let dir = TempDirGuard::new("content-vanished");
    let h = file_harness(dir.path());
    let (_, body) = post_record(&h.sink, record("cmd-content-3", "ok")).await;
    let artifact_id = body["artifacts"][0]["artifact_id"].as_str().unwrap();

    // 索引说有、对象被删掉：这是存储故障/待对账，答 404 会让人以为证据从未存在。
    let artifact = h.index.all().into_iter().next().unwrap();
    std::fs::remove_file(dir.path().join(&artifact.object_key)).expect("remove object");

    let (status, _, bytes) = get_content(&h.read, "c-001", artifact_id).await;
    assert_eq!(
        status,
        StatusCode::BAD_GATEWAY,
        "{}",
        String::from_utf8_lossy(&bytes)
    );
    // 5xx 不回显后端细节。
    let rendered = String::from_utf8_lossy(&bytes);
    assert!(
        !rendered.contains(dir.path().to_string_lossy().as_ref()),
        "{rendered}"
    );
}

// ── 写探针：每 5 分钟**打一次自己的写端点**，每天清掉过期的 ────────────────────

use wist_evidence::probe::{ProbeRunner, ProbeTarget};

fn probe_harness() -> Harness {
    let memory = Arc::new(MemoryObjectStore::new());
    harness_with(
        Arc::new(MemoryIndex::new()),
        memory.clone(),
        Some(memory),
        None,
        true,
    )
}

/// 把写端点真起来（探针要走 HTTP，不能直接调用例层）。
async fn spawn_router(router: Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind probe target");
    let address = listener.local_addr().expect("probe target addr");
    tokio::spawn(async move {
        let _ = axum::serve(listener, router).await;
    });
    address
}

async fn probe_runner(h: &Harness) -> ProbeRunner {
    let address = spawn_router(h.sink.clone()).await;
    ProbeRunner::new(
        h.service.clone(),
        ProbeTarget {
            url: format!("http://{address}/internal/evidence"),
        },
        wist_evidence::probe::PROBE_CUSTOMER,
        std::time::Duration::from_secs(300),
        std::time::Duration::from_secs(24 * 3600),
    )
}

/// 打一个没人监听的端口：用来验「写端点不可达」也会被如实记录。
async fn unreachable_probe_runner(h: &Harness) -> ProbeRunner {
    // 先拿一个空闲端口再释放它，拿到的基本不会被别人抢走。
    let probe = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral");
    let address = probe.local_addr().expect("addr");
    drop(probe);
    ProbeRunner::new(
        h.service.clone(),
        ProbeTarget {
            url: format!("http://{address}/internal/evidence"),
        },
        wist_evidence::probe::PROBE_CUSTOMER,
        std::time::Duration::from_secs(300),
        std::time::Duration::from_secs(24 * 3600),
    )
}

async fn get_healthz(router: &Router) -> Value {
    let request = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

#[tokio::test]
async fn probe_writes_through_the_http_sink() {
    let h = probe_harness();
    let runner = probe_runner(&h).await;

    runner.tick().await.expect("probe should be accepted");

    // 真走了一遍 HTTP：路由、序列化、对象与索引都验了。
    let row = h
        .index
        .all()
        .into_iter()
        .find(|row| row.subject_id == "_probe")
        .expect("probe row");
    assert_eq!(row.kind, Kind::InvestigationReport);
    // 三字段形状没带敏感度 → 取部署默认（paths）。
    assert_eq!(row.sensitivity.as_str(), "paths");
    assert_eq!(row.customer_id, "c-probe");
    assert_eq!(row.subject_type, "customer");
    assert_eq!(h.object_count(), 1);

    let status = h.service.probe_status().expect("probe status");
    assert_eq!(status.last_ok, Some(true));
    assert_eq!(status.consecutive_failures, 0);
    assert!(status.last_at_ms.is_some());
    assert!(status.last_latency_ms.is_some());
}

#[tokio::test]
async fn probe_failure_is_recorded_without_breaking_anything() {
    let h = probe_harness();
    // 写端点不可达：探针必须把这次失败记下来而不是崩掉。
    let runner = unreachable_probe_runner(&h).await;

    let error = runner
        .tick()
        .await
        .expect_err("unreachable endpoint must be a probe failure");
    assert!(error.contains("unreachable"), "{error}");

    let status = h.service.probe_status().expect("probe status");
    assert_eq!(status.last_ok, Some(false));
    assert_eq!(status.consecutive_failures, 1);
    assert_eq!(h.index.len(), 0, "a rejected probe writes nothing");
}

#[tokio::test]
async fn repeated_probes_accumulate_until_purged() {
    let h = probe_harness();
    let runner = probe_runner(&h).await;

    for _ in 0..3 {
        runner.tick().await.expect("probe");
    }
    // 每次都是新对象（真实的写入不会覆盖历史）—— 所以必须有每日清理。
    assert_eq!(h.index.len(), 3);
    assert_eq!(h.object_count(), 3);

    // 保留 1 小时：还新鲜，一条都不删。
    let purged = h
        .service
        .purge_probes(
            wist_evidence::probe::PROBE_CUSTOMER,
            std::time::Duration::from_secs(3600),
        )
        .await
        .expect("purge");
    assert_eq!(purged, 0);
    assert_eq!(h.index.len(), 3);

    // 保留 0：刚写的也算过期 → 全清（对象一起没）。
    let purged = h
        .service
        .purge_probes(
            wist_evidence::probe::PROBE_CUSTOMER,
            std::time::Duration::ZERO,
        )
        .await
        .expect("purge");
    assert_eq!(purged, 3);
    assert_eq!(h.index.len(), 0);
    assert_eq!(h.object_count(), 0);
}

#[tokio::test]
async fn purge_only_touches_probes() {
    let h = probe_harness();
    let runner = probe_runner(&h).await;

    post_record(&h.sink, record("cmd-real-1", "warn")).await;
    runner.tick().await.expect("probe");

    let purged = h
        .service
        .purge_probes(
            wist_evidence::probe::PROBE_CUSTOMER,
            std::time::Duration::ZERO,
        )
        .await
        .expect("purge");

    assert_eq!(purged, 1, "only the probe may be purged");
    assert_eq!(h.index.len(), 1);
    assert_eq!(
        h.index
            .all()
            .into_iter()
            .next()
            .unwrap()
            .command_id
            .as_deref(),
        Some("cmd-real-1"),
        "real evidence must survive the probe purge"
    );
}

#[tokio::test]
async fn probe_artifacts_are_hidden_from_the_list() {
    let h = probe_harness();
    let runner = probe_runner(&h).await;

    post_record(&h.sink, record("cmd-list-1", "ok")).await;
    runner.tick().await.expect("probe");

    let (status, body) = list(&h.read, "c-001", "commandlines", "").await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let artifacts = body["artifacts"].as_array().expect("artifacts");
    // 探针每 5 分钟一条，不能污染人的列表。
    assert_eq!(artifacts.len(), 1, "{body}");
    assert_eq!(artifacts[0]["command_id"], "cmd-list-1");
}

#[tokio::test]
async fn healthz_exposes_probe_status() {
    let h = probe_harness();
    let runner = probe_runner(&h).await;
    runner.tick().await.expect("probe");

    // 两张监听都要能看到（compose 与 console 各打一张）。
    for router in [&h.sink, &h.read] {
        let body = get_healthz(router).await;
        assert_eq!(body["status"], "ok");
        assert_eq!(body["probe"]["last_ok"], true, "{body}");
        assert!(body["probe"]["last_at_ms"].is_u64(), "{body}");
    }
}

#[tokio::test]
async fn healthz_has_no_probe_block_when_disabled() {
    let h = harness();
    let body = get_healthz(&h.sink).await;
    assert_eq!(body["status"], "ok");
    assert!(body.get("probe").is_none(), "{body}");
}

/// `/healthz` 必须自述对象后端：console 据此直接走代理读，不再先撞一次 501。
#[tokio::test]
async fn healthz_reports_the_object_backend() {
    let dir = std::env::temp_dir().join(format!("wist-evidence-healthz-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let file = file_harness(&dir);
    // 两张监听同一口径（compose 打写端口、console 打读端口）。
    for router in [&file.sink, &file.read] {
        let body = get_healthz(router).await;
        assert_eq!(body["objects"]["kind"], "file", "{body}");
    }

    let memory = harness();
    let body = get_healthz(&memory.read).await;
    assert_eq!(body["objects"]["kind"], "memory", "{body}");

    let _ = std::fs::remove_dir_all(&dir);
}

// ── 三字段形状：cust_id / cmd_id / content ──────────────────────────────────
// ── 三字段形状：cust_id / cmd_id / content ──────────────

fn simple_record(cust_id: &str, cmd_id: &str) -> Value {
    json!({
        "cust_id": cust_id,
        "cmd_id": cmd_id,
        "content": { "summary": { "overall": "warn" }, "detail": "任意证据内容" },
    })
}

#[tokio::test]
async fn simple_record_is_stored_with_deployment_defaults() {
    let h = harness();

    let (status, body) = post_record(&h.sink, simple_record("c-002", "cmd-simple-1")).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 1);

    let row = h.index.all().into_iter().next().expect("row");
    // 三字段之外的维度取部署默认值；客户与主体都来自 `cust_id`。
    // 租户就是客户：两个字段是同一个值。
    assert_eq!(row.tenant_id, "c-002");
    assert_eq!(row.customer_id, "c-002");
    assert_eq!(row.subject_id, "c-002");
    assert_eq!(row.subject_type, "customer");
    assert_eq!(row.command_id.as_deref(), Some("cmd-simple-1"));
    assert_eq!(row.kind, Kind::InvestigationReport);
    assert_eq!(row.schema_id, "wist.evidence/v1");
    assert_eq!(row.sensitivity.as_str(), "paths");
    assert_eq!(row.verdict.as_str(), "unknown");
    // 内容原样落库（对象里能读回同样的 JSON）。
    let stored = h.stored_body(&row.object_key).expect("object stored");
    let parsed: Value = serde_json::from_slice(&stored).expect("json");
    assert_eq!(parsed["detail"], "任意证据内容");
}

#[tokio::test]
async fn simple_record_accepts_a_text_content() {
    let h = harness();
    let record = json!({
        "cust_id": "c-002",
        "cmd_id": "cmd-simple-2",
        "content": "raw log line 1\nraw log line 2",
    });

    let (status, body) = post_record(&h.sink, record).await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let row = h.index.all().into_iter().next().expect("row");
    let stored = h.stored_body(&row.object_key).expect("object stored");
    assert_eq!(
        String::from_utf8(stored).unwrap(),
        "raw log line 1\nraw log line 2"
    );
}

#[tokio::test]
async fn simple_record_is_idempotent_on_cust_and_cmd() {
    let h = harness();
    let record = simple_record("c-002", "cmd-simple-3");

    post_record(&h.sink, record.clone()).await;
    let (status, body) = post_record(&h.sink, record).await;

    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["duplicates"], 1, "{body}");
    assert_eq!(h.index.len(), 1);
    assert_eq!(h.object_count(), 1);
}

/// 真实数据面记录的完整形状：三字段之外还带一堆 OML / sink 标签输出的字段
/// （`schema` / `agent` / `seq` / `event_id` / `wp_oml_name` …）。
///
/// 钉两件：多出来的键必须被忽略（`SimpleRecord` 不开 `deny_unknown_fields`，
/// 否则一帧合法证据会被 400 打进 rescue）；同一个 `cust_id` + `cmd_id` + 正文重投
/// 必须是幂等的（sink 的 `max_retries` 会让重投成为常态）。
#[tokio::test]
async fn a_full_data_plane_record_is_idempotent_and_extra_fields_are_ignored() {
    let h = harness();
    let record = json!({
        "schema": "v1",
        "agent": "agent-1",
        "ts": "2026-10-30T10:00:05Z",
        "seq": 7,
        "event_id": "1234567890",
        "wp_oml_name": "agent_uplink_ivsrep",
        "biz": "agent-ivsrep",
        "cust_id": "c-002",
        "cmd_id": "cmd-full-1",
        // 与 OML `content = read(body)` 同形：正文是**字符串**，不是对象。
        "content": "{\"command_id\":\"cmd-full-1\",\"report_schema\":\"wist.investigation.disk/v1\"}",
    });

    let (status, first) = post_record(&h.sink, record.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let artifact_id = first["artifacts"][0]["artifact_id"]
        .as_str()
        .expect("artifact id")
        .to_string();
    assert_eq!(first["duplicates"], 0, "{first}");

    let (status, second) = post_record(&h.sink, record).await;
    assert_eq!(status, StatusCode::OK, "{second}");
    assert_eq!(second["duplicates"], 1, "{second}");
    assert_eq!(second["artifacts"][0]["artifact_id"], artifact_id);
    assert_eq!(h.index.len(), 1, "多出来的键不该影响去重");
    assert_eq!(h.object_count(), 1);
}

#[tokio::test]
async fn array_body_writes_every_record() {
    let h = harness();
    let batch = json!([
        simple_record("c-002", "cmd-batch-1"),
        simple_record("c-002", "cmd-batch-2"),
        simple_record("c-003", "cmd-batch-3"),
    ]);

    let (status, body) = post_record(&h.sink, batch).await;

    // `batch_size > 1` 是数据面的常态，不能只收单对象。
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["count"], 3);
    assert_eq!(body["duplicates"], 0);
    assert_eq!(h.index.len(), 3);
    assert_eq!(h.object_count(), 3);
}
