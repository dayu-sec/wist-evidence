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
    FileObjectStore, MemoryIndex, MemoryObjectStore, ObjectMeta, ObjectStore, StoreError,
};
use wist_evidence::service::{EvidenceService, SinkCaller, TokenMap};

const TOKEN: &str = "tok_dev";

struct Harness {
    sink: Router,
    read: Router,
    index: Arc<MemoryIndex>,
    /// 断言用的具体后端：`None` 表示不是内存后端（file 直接看磁盘）。
    memory: Option<Arc<MemoryObjectStore>>,
    file: Option<Arc<FileObjectStore>>,
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
) -> Harness {
    let service = Arc::new(EvidenceService::new(
        index.clone(),
        objects.clone(),
        "wist-evidence-test".to_string(),
        vec![Kind::InvestigationReport],
        4 * 1024 * 1024,
    ));

    let mut tokens = TokenMap::new();
    tokens.insert(
        TOKEN.to_string(),
        SinkCaller {
            tenant_id: "t-001".to_string(),
            customer_id: "c-001".to_string(),
        },
    );
    tokens.insert(
        "tok_other".to_string(),
        SinkCaller {
            tenant_id: "t-002".to_string(),
            customer_id: "c-002".to_string(),
        },
    );

    Harness {
        sink: api::sink_router(
            SinkState {
                service: service.clone(),
                tokens: Arc::new(tokens),
            },
            4 * 1024 * 1024,
        ),
        read: api::read_router(ReadState { service }),
        index,
        memory,
        file,
    }
}

fn harness() -> Harness {
    let memory = Arc::new(MemoryObjectStore::new());
    harness_with(
        Arc::new(MemoryIndex::new()),
        memory.clone(),
        Some(memory),
        None,
    )
}

/// 默认部署形态（`objects.kind = file`）的端到端 harness。
fn file_harness(root: &std::path::Path) -> Harness {
    let file = Arc::new(FileObjectStore::open(root).expect("open file store"));
    harness_with(Arc::new(MemoryIndex::new()), file.clone(), None, Some(file))
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

fn sink_record(command_id: &str, overall: &str) -> Value {
    json!({
        "schema": "IVSREP",
        "agent": "agent-7f3",
        "ts": "2026-11-01T00:00:00Z",
        "seq": 1,
        "sensitivity": "paths",
        "body": report(command_id, overall),
    })
}

async fn post_record(sink: &Router, token: &str, record: Value) -> (StatusCode, Value) {
    let request = Request::builder()
        .method("POST")
        .uri("/internal/evidence")
        .header("authorization", format!("Bearer {token}"))
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
        .header("x-evidence-tenant", "t-001")
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
    let record = sink_record("cmd-1", "warn");

    let (status, first) = post_record(&h.sink, TOKEN, record.clone()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let artifact_id = first["artifacts"][0]["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(first["duplicates"], 0);

    // sink 的 max_retries 会让重复投递成为常态：第二次必须是 200，且不产生新对象 / 新行。
    let (status, second) = post_record(&h.sink, TOKEN, record).await;
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
    let (_, written) = post_record(&h.sink, TOKEN, sink_record("cmd-2", "ok")).await;
    let artifact_id = written["artifacts"][0]["artifact_id"].as_str().unwrap();

    // 同租户能读。
    assert_eq!(
        get_metadata(&h.read, "t-001", artifact_id).await,
        StatusCode::OK
    );
    // 另一个租户：只能得到 404（不泄露"存在但不能看"）。
    assert_eq!(
        get_metadata(&h.read, "t-002", artifact_id).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn unsafe_subject_is_rejected() {
    let h = harness();
    for agent in ["", "..", "../other", "a/b"] {
        let mut record = sink_record("cmd-3", "ok");
        record["agent"] = json!(agent);
        let (status, body) = post_record(&h.sink, TOKEN, record).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "agent={agent:?} -> {body}");
    }
    assert!(
        h.object_count() == 0,
        "rejected writes must not reach the object store"
    );
}

#[tokio::test]
async fn unknown_frame_marker_is_400() {
    let h = harness();
    let mut record = sink_record("cmd-4", "ok");
    record["schema"] = json!("OBSFACT");
    let (status, body) = post_record(&h.sink, TOKEN, record).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "evidence_frame_not_evidence");
}

#[tokio::test]
async fn tenant_claim_in_body_must_match_the_token() {
    let h = harness();
    let mut record = sink_record("cmd-5", "ok");
    let mut body: Value = serde_json::from_str(record["body"].as_str().unwrap()).unwrap();
    body["tenant_id"] = json!("t-002");
    record["body"] = json!(body.to_string());

    let (status, response) = post_record(&h.sink, TOKEN, record).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{response}");
    assert_eq!(response["error"]["code"], "evidence_tenant_mismatch");
}

#[tokio::test]
async fn presign_is_single_object_and_capped_at_60s() {
    let h = harness();
    let (_, written) = post_record(&h.sink, TOKEN, sink_record("cmd-6", "warn")).await;
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
    let (_, first) = post_record(&h.sink, TOKEN, sink_record("cmd-7", "warn")).await;
    let artifact_id = first["artifacts"][0]["artifact_id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, second) = post_record(&h.sink, TOKEN, sink_record("cmd-7", "fail")).await;
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
    post_record(&h.sink, TOKEN, sink_record("cmd-8", "ok")).await;

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
    post_record(&h.sink, TOKEN, sink_record("cmd-a", "warn")).await;
    post_record(&h.sink, TOKEN, sink_record("cmd-b", "ok")).await;

    let (status, page) = list(&h.read, "t-001", "paths", "").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    let artifacts = page["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), 2);
    assert!(page["next_cursor"].is_null());

    // 类型筛选
    let (_, filtered) = list(&h.read, "t-001", "paths", "?kind=diagnose_report").await;
    assert_eq!(filtered["artifacts"].as_array().unwrap().len(), 0);

    // 跨租户：身份里没有 t-002 的数据 → 空列表（不是 403，避免泄露存在性）。
    let (status, other) = list(&h.read, "t-002", "paths", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(other["artifacts"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn list_hides_artifacts_above_the_caller_sensitivity() {
    let h = harness();
    post_record(&h.sink, TOKEN, sink_record("cmd-c", "warn")).await; // sensitivity=paths

    let (status, page) = list(&h.read, "t-001", "metrics", "").await;
    assert_eq!(status, StatusCode::OK, "{page}");
    assert_eq!(page["artifacts"].as_array().unwrap().len(), 0);

    let (_, visible) = list(&h.read, "t-001", "paths", "").await;
    assert_eq!(visible["artifacts"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn basic_auth_username_is_the_site_token() {
    // wparse 的 http 连接器只有 `username` / `password`：Basic 的 username 就是站点 token。
    let h = harness();
    use base64::Engine as _;
    let encoded = base64::engine::general_purpose::STANDARD.encode("tok_dev:");
    let request = Request::builder()
        .method("POST")
        .uri("/internal/evidence")
        .header("authorization", format!("Basic {encoded}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&sink_record("cmd-basic-1", "ok")).unwrap(),
        ))
        .unwrap();
    let response = h.sink.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let wrong = base64::engine::general_purpose::STANDARD.encode("tok_nope:");
    let request = Request::builder()
        .method("POST")
        .uri("/internal/evidence")
        .header("authorization", format!("Basic {wrong}"))
        .header("content-type", "application/json")
        .body(Body::from(
            serde_json::to_vec(&sink_record("cmd-basic-2", "ok")).unwrap(),
        ))
        .unwrap();
    let response = h.sink.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn missing_token_is_401() {
    let h = harness();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/evidence")
        .header("content-type", "application/json")
        .body(Body::from(json!([]).to_string()))
        .unwrap();
    let response = h.sink.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
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

    let (status, body) = post_record(&h.sink, TOKEN, sink_record("cmd-file-1", "warn")).await;
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
    let record = sink_record("cmd-file-2", "ok");

    let (_, first) = post_record(&h.sink, TOKEN, record.clone()).await;
    let (status, second) = post_record(&h.sink, TOKEN, record).await;

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

    post_record(&h.sink, TOKEN, sink_record("cmd-file-3", "warn")).await;
    let (status, _) = post_record(&h.sink, TOKEN, sink_record("cmd-file-3", "fail")).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(h.index.len(), 1, "one dedupe key = one row");
    assert_eq!(h.object_count(), 2, "键不可覆盖：旧对象的文件要留着待清");
}

#[tokio::test]
async fn file_backend_presign_returns_501() {
    let dir = TempDirGuard::new("presign");
    let h = file_harness(dir.path());
    let (_, body) = post_record(&h.sink, TOKEN, sink_record("cmd-file-4", "ok")).await;
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
}

fn failing_harness(error: fn() -> StoreError) -> Harness {
    harness_with(
        Arc::new(MemoryIndex::new()),
        Arc::new(FailingStore { error }),
        None,
        None,
    )
}

#[tokio::test]
async fn backend_failure_is_502_and_leaves_no_index_row() {
    let h = failing_harness(|| StoreError::Backend("disk full".to_string()));

    let (status, body) = post_record(&h.sink, TOKEN, sink_record("cmd-fail-1", "ok")).await;

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

    let (status, body) = post_record(&h.sink, TOKEN, sink_record("cmd-fail-2", "ok")).await;

    // 拼键错误是编程错误：重试不会变好，不能伪装成 5xx 让 sink 反复重投。
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "evidence_object_key_invalid");
    assert_eq!(h.index.len(), 0);
}

/// 写入正常、只有签名失败的后端：用来验读路径的 5xx 也不回显后端原文。
struct PresignFailsStore;

#[async_trait::async_trait]
impl ObjectStore for PresignFailsStore {
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
}

#[tokio::test]
async fn read_backend_failure_is_502_without_leaking_details() {
    let h = harness_with(
        Arc::new(MemoryIndex::new()),
        Arc::new(PresignFailsStore),
        None,
        None,
    );
    let (status, body) = post_record(&h.sink, TOKEN, sink_record("cmd-leak-1", "ok")).await;
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
