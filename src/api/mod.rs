//! HTTP 面：两张监听、两套信任模型。
//!
//! - 写（[`sink`]）：数据面 `http_sink` 直投，明文（内网可信；mTLS 在 P1）；
//! - 读（[`read`]）：内部服务调用，mTLS + scope（P0 由内网 TLS 终结层往请求头注入身份，
//!   CA-I 落地后改成证书 claim，见设计稿 §19-10）。
//!
//! 两张面**不能共用端口**：混在一起就必须让写端点容忍一段"没有证书"的流量，
//! 而那正是最容易被放大的授权漏洞。

pub mod read;
pub mod sink;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::domain::{Kind, Sensitivity};
use crate::service::EvidenceService;

/// 写端点的状态：服务 + 三字段形状要用的部署默认值。
#[derive(Clone)]
pub struct SinkState {
    pub service: Arc<EvidenceService>,
    pub default_kind: Kind,
    pub default_schema: String,
    pub default_sensitivity: Sensitivity,
}

/// 读端点的状态：只需要服务（身份来自请求头 / 将来的证书）。
#[derive(Clone)]
pub struct ReadState {
    pub service: Arc<EvidenceService>,
}

pub fn sink_router(state: SinkState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/healthz", get(sink_healthz))
        .route("/internal/evidence", post(sink::write_records))
        // 上限比正文上限宽一点：外层记录把正文当字符串放，转义会膨胀（与网关 ingest 同口径）。
        .layer(DefaultBodyLimit::max(
            max_body_bytes.saturating_add(64 * 1024),
        ))
        .with_state(state)
}

pub fn read_router(state: ReadState) -> Router {
    Router::new()
        .route("/healthz", get(read_healthz))
        .route("/internal/evidence", get(read::list_evidence))
        // `{key}` 可以是证据 id（`art_…`）或调查命令 id（`cmd_id`）：
        // 发起调查的人手里只有后一个（见 `EvidenceService::find_visible`）。
        .route(
            "/internal/evidence/{key}",
            get(read::metadata).post(read::presign_at),
        )
        // 代理读正文（§7.2）：本地文件模式没有签名 URL，这是唯一的读正文通路。
        .route("/internal/evidence/{key}/content", get(read::content))
        .with_state(state)
}

async fn sink_healthz(State(state): State<SinkState>) -> Json<serde_json::Value> {
    Json(health_body(&state.service))
}

async fn read_healthz(State(state): State<ReadState>) -> Json<serde_json::Value> {
    Json(health_body(&state.service))
}

/// 两张监听共用：进程活着就是 `status = ok`；开了写探针时多一个 `probe` 块，
/// 让上游能区分「端口能连上」与「真的能写进去」。
///
/// `objects.kind` 是**部署自述**：读端据此判断正文怎么打开（`s3` = 签名 URL，
/// `file` = 只能代理读），不必先发一个必然 501 的 `:presign`。
fn health_body(service: &EvidenceService) -> serde_json::Value {
    let mut body = json!({
        "status": "ok",
        "objects": { "kind": service.objects_backend().as_str() },
    });
    if let Some(probe) = service.probe_status() {
        body["probe"] = serde_json::to_value(probe).unwrap_or(serde_json::Value::Null);
    }
    body
}
