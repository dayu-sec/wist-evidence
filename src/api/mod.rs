//! HTTP 面：两张监听、两套信任模型。
//!
//! - 写（[`sink`]）：数据面 `http_sink` 直投，明文 + 站点 token；
//! - 读（[`read`]）：内部服务调用，mTLS + scope（P0 由内网 TLS 终结层往请求头注入身份，
//!   CA-I 落地后改成证书 claim，见设计稿 §19-10）。
//!
//! 两张面**不能共用端口**：混在一起就必须让写端点容忍一段"没有证书"的流量，
//! 而那正是最容易被放大的授权漏洞。

pub mod read;
pub mod sink;

use std::sync::Arc;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use crate::service::{EvidenceService, TokenMap};

/// 写端点的状态：需要 token 表 + 服务。
#[derive(Clone)]
pub struct SinkState {
    pub service: Arc<EvidenceService>,
    pub tokens: Arc<TokenMap>,
}

/// 读端点的状态：只需要服务（身份来自请求头 / 将来的证书）。
#[derive(Clone)]
pub struct ReadState {
    pub service: Arc<EvidenceService>,
}

pub fn sink_router(state: SinkState, max_body_bytes: usize) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/internal/evidence", post(sink::write_records))
        // 上限比正文上限宽一点：外层记录把正文当字符串放，转义会膨胀（与网关 ingest 同口径）。
        .layer(DefaultBodyLimit::max(
            max_body_bytes.saturating_add(64 * 1024),
        ))
        .with_state(state)
}

pub fn read_router(state: ReadState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/internal/evidence", get(read::list_evidence))
        .route(
            "/internal/evidence/{artifact_id}",
            get(read::metadata).post(read::presign_at),
        )
        .with_state(state)
}

async fn healthz() -> Json<serde_json::Value> {
    Json(json!({ "status": "ok" }))
}
