//! 写端点：主机流记录的窗口行（`POST /internal/flows`）。
//!
//! 为什么不复用 `/internal/evidence`：那条路径是「一份产物一个对象 + 索引行」，
//! 流记录是每主机每 60s 每五元组一条 —— 按对象存会造出百万级小对象
//! （见 `wist-design/doc/design/center/agent-flow-storage.md` §3 D4）。
//!
//! 三条与证据写端点一致的纪律：
//!
//! - 只有明文 HTTP（数据面 `http_sink` 没有 TLS 参数），租户来自 body 的 `cust_id`；
//! - **不静默丢**：字段不合法、客户没登记、条数超限都是 4xx 并把明细回给调用方；
//! - 后端错回 5xx（细节只进日志），让上游重试并最终落成可见的 rescue 文件。

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::domain::{FlowRecordIn, MAX_RECORDS_PER_WRITE};

use super::SinkState;

/// 一次写入的请求体。**数组里逐条**而不是单条一条请求：一拍最多 4096 条，
/// 一条一请求等于把 per-request 开销乘上 4096。
#[derive(Debug, Deserialize)]
pub struct FlowWriteRequest {
    /// 客户 / 租户：与 `[[storage.cell]].cust_id` 完全一致（网关按登记表填）。
    pub cust_id: String,
    pub records: Vec<FlowRecordIn>,
}

pub async fn write_flows(
    State(state): State<SinkState>,
    Json(request): Json<FlowWriteRequest>,
) -> Response {
    let cust_id = request.cust_id.trim().to_string();
    if cust_id.is_empty() {
        return bad_request("flow_cust_id_missing", "cust_id is required", Vec::new());
    }

    let Some(store) = state.service.backends().flows_for(&cust_id) else {
        // 未登记的客户没有落点：这是配置/接入错，重试不会变好。
        return bad_request(
            "flow_customer_unregistered",
            &format!(
                "cust_id {cust_id} has no [[storage.cell]] in this deployment: \
                 check the cell list and that it matches the gateway-bound tenant_id"
            ),
            Vec::new(),
        );
    };

    if request.records.is_empty() {
        return bad_request(
            "flow_records_empty",
            "records must not be empty",
            Vec::new(),
        );
    }
    if request.records.len() > MAX_RECORDS_PER_WRITE {
        // **截断是最坏的失败形状**：调用方以为写完了。超过上限就整批拒。
        return bad_request(
            "flow_batch_too_large",
            &format!(
                "{} records exceed the per-request limit of {MAX_RECORDS_PER_WRITE}",
                request.records.len()
            ),
            Vec::new(),
        );
    }

    let mut failures = Vec::new();
    for (index, record) in request.records.iter().enumerate() {
        if let Err(code) = record.validate() {
            failures.push(format!("record #{index} ({}): {code}", record.describe()));
        }
    }
    if !failures.is_empty() {
        // 整批不写：写一半会让"重试"变成部分覆盖，排查时对不上。
        return bad_request(
            "flow_record_invalid",
            "one or more records failed validation",
            failures,
        );
    }

    match store.insert_batch(&cust_id, &request.records).await {
        Ok(outcome) => (
            StatusCode::OK,
            Json(json!({
                "accepted": outcome.accepted,
                // 本次新建的分区数：只在第一次见到某一天时非 0，巡检时有用。
                "partitions": outcome.partitions,
            })),
        )
            .into_response(),
        Err(error) => {
            // 细节（SQL / 连接串片段 / 表名）只进日志，不回给数据面。
            log::error!("flow_write_failed: cust_id={cust_id} {error}");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({
                    "error": {
                        "code": "flow_write_failed",
                        "message": "internal error while storing the flow records",
                    }
                })),
            )
                .into_response()
        }
    }
}

fn bad_request(code: &str, message: &str, failures: Vec<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({
            "error": { "code": code, "message": message },
            "failures": failures,
        })),
    )
        .into_response()
}
