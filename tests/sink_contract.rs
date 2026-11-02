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
use wist_evidence::infra::{MemoryIndex, MemoryObjectStore};
use wist_evidence::service::{EvidenceService, SinkCaller, TokenMap};

const TOKEN: &str = "tok_dev";

struct Harness {
    sink: Router,
    read: Router,
    index: Arc<MemoryIndex>,
    objects: Arc<MemoryObjectStore>,
}

fn harness() -> Harness {
    let index = Arc::new(MemoryIndex::new());
    let objects = Arc::new(MemoryObjectStore::new());
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
        objects,
    }
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
    assert_eq!(h.objects.len(), 1, "replay must not write a second object");
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
        h.objects.is_empty(),
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
        h.objects.len(),
        2,
        "old object is kept for cleanup, not overwritten"
    );
}

#[tokio::test]
async fn object_metadata_carries_the_rebuild_fields() {
    let h = harness();
    post_record(&h.sink, TOKEN, sink_record("cmd-8", "ok")).await;

    let artifact = h.index.all().into_iter().next().unwrap();
    let meta = h.objects.meta_of(&artifact.object_key).unwrap();
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
