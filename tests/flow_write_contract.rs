//! `/internal/flows` 的契约测试：批量写入、幂等、未登记客户、字段校验、上限。
//!
//! 用内存后端（`MemoryFlowStore`）：验的是**端点的判定与状态码**，
//! 分区创建与 upsert 的 SQL 留到真机（PG）验收。

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use wist_evidence::api::{self, SinkState};
use wist_evidence::domain::kind::Sensitivity;
use wist_evidence::domain::{Kind, MAX_RECORDS_PER_WRITE, RECORD_SCHEMA_V1};
use wist_evidence::infra::{Backend, Backends, MemoryFlowStore, MemoryIndex, MemoryObjectStore};

const CUST: &str = "c-001";

fn sink(flows: Arc<MemoryFlowStore>) -> Router {
    let service = Arc::new(wist_evidence::service::EvidenceService::with_backends(
        Arc::new(Backends::new(
            [(
                CUST.to_string(),
                Backend {
                    index: Arc::new(MemoryIndex::new()),
                    objects: Arc::new(MemoryObjectStore::new()),
                    flows,
                    bucket: "wist-evidence-test".to_string(),
                },
            )]
            .into_iter()
            .collect::<BTreeMap<_, _>>(),
        )),
        vec![Kind::InvestigationReport],
        4 * 1024 * 1024,
    ));
    api::sink_router(
        SinkState {
            service,
            default_kind: Kind::InvestigationReport,
            default_schema: "wist.evidence/v1".to_string(),
            default_sensitivity: Sensitivity::Paths,
        },
        4 * 1024 * 1024,
    )
}

/// 一条 `wist.host_flow/v1` 记录（最简形状：只带端点会读的字段）。
fn record(start: i64, dst_port: &str) -> Value {
    json!({
        "record_schema": RECORD_SCHEMA_V1,
        "source": "host_ebpf",
        "coverage": "host_partial",
        "capture": "tc_egress",
        "collector_instance_id": "agent-host-1",
        "host_id": "host-1",
        "ifname": "eth0",
        "ifindex": 2,
        "local_addrs": ["10.118.0.14"],
        "direction": "out",
        "protocol": "6",
        "srcaddr": "10.118.0.14",
        "dstaddr": "1.1.1.1",
        "srcport": "50000",
        "dstport": dst_port,
        "start": start,
        "end": start + 60,
        "window_seconds": 60,
        "packets": 13,
        "bytes": 1460,
        "wire_bytes_source": "unavailable",
        "truncated": false,
        "dropped": 0,
    })
}

async fn post_flows(app: &Router, cust_id: &str, records: Vec<Value>) -> (StatusCode, Value) {
    let body = json!({ "cust_id": cust_id, "records": records }).to_string();
    let request = Request::builder()
        .method("POST")
        .uri("/internal/flows")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .expect("request");
    let response = app.clone().oneshot(request).await.expect("response");
    let status = response.status();
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
async fn a_batch_is_accepted_and_resending_it_changes_nothing() {
    let app = sink(Arc::new(MemoryFlowStore::new()));
    let batch = vec![record(1_789_470_120, "443"), record(1_789_470_120, "80")];

    let (status, body) = post_flows(&app, CUST, batch.clone()).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], 2);

    // 幂等：同键重发被覆盖（accepted = 0），不是新增两行。
    let (status, body) = post_flows(&app, CUST, batch).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["accepted"], 0);
}

#[tokio::test]
async fn an_unregistered_customer_is_a_client_error() {
    // 未登记客户没有落点：重试不会变好，所以是 4xx 而不是 5xx。
    let app = sink(Arc::new(MemoryFlowStore::new()));
    let (status, body) = post_flows(&app, "c-unknown", vec![record(1_789_470_120, "443")]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "flow_customer_unregistered");
}

#[tokio::test]
async fn a_bad_record_rejects_the_whole_batch() {
    // 写一半会让重试变成部分覆盖：整批拒，并把每条的原因带回去。
    let store = Arc::new(MemoryFlowStore::new());
    let app = sink(store);
    let mut bad = record(1_789_470_120, "443");
    bad["end"] = json!(1_789_470_000); // 窗口长度不自洽

    let (status, body) = post_flows(&app, CUST, vec![record(1_789_470_120, "80"), bad]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "flow_record_invalid");
    let failures = body["failures"].as_array().expect("failures");
    assert_eq!(failures.len(), 1, "{failures:?}");
    assert!(
        failures[0]
            .as_str()
            .unwrap_or_default()
            .contains("window_length_mismatch"),
        "{failures:?}"
    );
}

#[tokio::test]
async fn an_unknown_record_schema_is_rejected() {
    let app = sink(Arc::new(MemoryFlowStore::new()));
    let mut v2 = record(1_789_470_120, "443");
    v2["record_schema"] = json!("wist.host_flow/v2");
    let (status, body) = post_flows(&app, CUST, vec![v2]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(
        body["failures"][0]
            .as_str()
            .unwrap_or_default()
            .contains("unsupported_record_schema"),
        "{body}"
    );
}

#[tokio::test]
async fn an_empty_batch_is_rejected() {
    let app = sink(Arc::new(MemoryFlowStore::new()));
    let (status, body) = post_flows(&app, CUST, Vec::new()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "flow_records_empty");
}

#[tokio::test]
async fn an_oversized_batch_is_rejected_instead_of_truncated() {
    // 静默截断是最坏的失败形状：调用方以为全写进去了。
    let app = sink(Arc::new(MemoryFlowStore::new()));
    let batch: Vec<Value> = (0..=MAX_RECORDS_PER_WRITE)
        .map(|index| record(1_789_470_120, &(index.to_string())))
        .collect();
    let (status, body) = post_flows(&app, CUST, batch).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["error"]["code"], "flow_batch_too_large");
}

/// `/healthz` 要能自述流记录的落点类型：现场靠它判断"是不是只落了内存"。
#[tokio::test]
async fn healthz_reports_the_flow_backend() {
    let app = sink(Arc::new(MemoryFlowStore::new()));
    let request = Request::builder()
        .method("GET")
        .uri("/healthz")
        .body(Body::empty())
        .expect("request");
    let response = app.oneshot(request).await.expect("response");
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = response
        .into_body()
        .collect()
        .await
        .expect("body")
        .to_bytes();
    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["cells"][0]["flows"], "memory", "{value}");
}
