//! 写端点：**三字段**（`cust_id` / `cmd_id` / `content`）+ 可选 `subject`。
//!
//! 三条来自数据面的事实（设计稿 §6.1）：
//! - 只有明文 HTTP，没有证书 → 认证先放宽，将来换 HTTPS 双向认证（mTLS）；
//! - 非 2xx 会重试并最终落 rescue 文件 → 只有"真的没存下"才回 5xx，重投一律 200；
//! - `batch_size` 可能 > 1 → 同时接受单对象与数组。
//!
//! 身份只有 `cust_id` 一个维度：租户就是客户，不再有第二层身份，也不需要 token 映射。

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use serde_json::json;

use crate::service::{SimpleWrite, WriteError};

use super::SinkState;

pub async fn write_records(State(state): State<SinkState>, body: Bytes) -> Response {
    let records = match parse_records(&body) {
        Ok(records) => records,
        Err(error) => return error.into_response_for_write(),
    };

    let mut outcomes = Vec::with_capacity(records.len());
    for record in records {
        let subject = record
            .subject
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .unwrap_or(&record.cust_id)
            .to_string();
        let result = state
            .service
            .write_simple(SimpleWrite {
                customer_id: record.cust_id,
                command_id: record.cmd_id,
                subject_id: subject,
                subject_type: "customer",
                kind: state.default_kind,
                schema_id: state.default_schema.clone(),
                sensitivity: state.default_sensitivity,
                content: record.content.into_text(),
            })
            .await;
        match result {
            Ok(outcome) => outcomes.push(outcome),
            Err(error) => return error.into_response_for_write(),
        }
    }

    let duplicates = outcomes.iter().filter(|outcome| outcome.duplicate).count();
    (
        StatusCode::OK,
        Json(json!({
            "count": outcomes.len(),
            "duplicates": duplicates,
            "artifacts": outcomes,
        })),
    )
        .into_response()
}

/// 证据正文：给字符串就原样存，给 JSON 对象就序列化后存。
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum Content {
    Text(String),
    Json(serde_json::Value),
}

impl Content {
    fn into_text(self) -> String {
        match self {
            Self::Text(text) => text,
            Self::Json(value) => value.to_string(),
        }
    }
}

/// **三字段形状**：`cust_id` / `cmd_id` / `content`（`subject` 可选）。
#[derive(Debug, Deserialize)]
struct SimpleRecord {
    cust_id: String,
    cmd_id: String,
    /// 可选：覆盖主体标识（默认就是 `cust_id`）。
    #[serde(default)]
    subject: Option<String>,
    content: Content,
}

fn parse_records(body: &Bytes) -> Result<Vec<SimpleRecord>, WriteError> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|_| WriteError::BadRequest("evidence_body_invalid"))?;
    match value {
        serde_json::Value::Array(items) => items
            .into_iter()
            .map(|item| {
                serde_json::from_value(item)
                    .map_err(|_| WriteError::BadRequest("evidence_record_invalid"))
            })
            .collect(),
        serde_json::Value::Object(_) => serde_json::from_value::<SimpleRecord>(value)
            .map(|record| vec![record])
            .map_err(|_| WriteError::BadRequest("evidence_record_invalid")),
        _ => Err(WriteError::BadRequest("evidence_record_invalid")),
    }
}

trait WriteErrorResponse {
    fn into_response_for_write(self) -> Response;
}

impl WriteErrorResponse for WriteError {
    fn into_response_for_write(self) -> Response {
        let (status, code, message) = match self {
            Self::BadRequest(code) => (StatusCode::BAD_REQUEST, code, "bad request".to_string()),
            Self::KindNotAllowed(kind) => (
                StatusCode::FORBIDDEN,
                "evidence_kind_not_allowed",
                format!("kind {kind} is not enabled in this deployment"),
            ),
            Self::TooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "evidence_body_too_large",
                "artifact exceeds the configured limit".to_string(),
            ),
            // 5xx 是**故意**的：让 sink 重试并最终把失败落成可见的 rescue 文件。
            // 但**细节只进日志**：后端原文里有宿主绝对路径 / errno / 桶名，
            // 不能回给数据面，否则就是一个免费的目录与磁盘状态探测器。
            Self::Backend(detail) => {
                log::error!("evidence_write_failed: {detail}");
                (
                    StatusCode::BAD_GATEWAY,
                    "evidence_write_failed",
                    "internal error while storing the artifact".to_string(),
                )
            }
        };
        error_response(status, code, message)
    }
}

fn error_response(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message.into() } })),
    )
        .into_response()
}
