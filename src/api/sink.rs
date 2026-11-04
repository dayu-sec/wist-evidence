//! 写端点：**按 `http_sink` 的记录形状**收，不按"理想的 REST 资源"收。
//!
//! 三条来自 sink 的事实（设计稿 §6.1）：
//! - 只支持明文 HTTP + header 注入的身份 → token 校验放这里，不指望 mTLS；
//! - 非 2xx 会重试并最终落 rescue 文件 → 只有"真的没存下"才回 5xx，重投一律 200；
//! - `batch_size` 可能 > 1 → 同时接受单对象与数组。

use axum::Json;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use crate::service::{SinkCaller, SinkRecord, WriteError};

use super::SinkState;

pub async fn write_records(
    State(state): State<SinkState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let caller = match authenticate(&state, &headers) {
        Ok(caller) => caller,
        Err(error) => return error.into_response(),
    };

    let records = match parse_records(&body) {
        Ok(records) => records,
        Err(error) => return error.into_response_for_write(),
    };

    let mut outcomes = Vec::with_capacity(records.len());
    for record in records {
        match state.service.write(&caller, record).await {
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

fn authenticate(state: &SinkState, headers: &HeaderMap) -> Result<SinkCaller, AuthError> {
    let Some(raw) = headers.get(axum::http::header::AUTHORIZATION) else {
        return Err(AuthError::Missing);
    };
    let raw = raw.to_str().unwrap_or_default();
    let token = credential(raw).filter(|token| !token.is_empty());
    let Some(token) = token else {
        return Err(AuthError::Invalid);
    };
    state.tokens.get(&token).cloned().ok_or(AuthError::Invalid)
}

/// 两种凭据形状：
/// - `Bearer <token>`：网关 / 脚本直接用这个；
/// - `Basic base64(<token>:)`：给 **warp-parse 的 http 连接器**用 —— 它只有
///   `username` / `password` 两个字段，自定义 header 的 env 展开没验证过，
///   而 `username` 走环境变量是确定可用的。此时 `username` 就是站点 token。
fn credential(raw: &str) -> Option<String> {
    let raw = raw.trim();
    if let Some(token) = raw
        .strip_prefix("Bearer ")
        .or_else(|| raw.strip_prefix("bearer "))
    {
        return Some(token.trim().to_string());
    }
    if let Some(encoded) = raw
        .strip_prefix("Basic ")
        .or_else(|| raw.strip_prefix("basic "))
    {
        use base64::Engine as _;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .ok()?;
        let text = String::from_utf8(decoded).ok()?;
        let (user, _password) = text.split_once(':')?;
        return Some(user.to_string());
    }
    // 兼容直接贴 token 的裸写（不推荐，但不要让一个好心的调用方默默失败）。
    Some(raw.to_string())
}

/// 小枚举而不是 `Response`：后者会把 `Result` 撑到 128 字节（clippy::result_large_err）。
enum AuthError {
    Missing,
    Invalid,
}

impl AuthError {
    fn into_response(self) -> Response {
        match self {
            Self::Missing => error_response(
                StatusCode::UNAUTHORIZED,
                "evidence_token_missing",
                "missing Authorization header",
            ),
            Self::Invalid => error_response(
                StatusCode::UNAUTHORIZED,
                "evidence_token_invalid",
                "unknown sink token",
            ),
        }
    }
}

fn parse_records(body: &Bytes) -> Result<Vec<SinkRecord>, WriteError> {
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
        serde_json::Value::Object(_) => serde_json::from_value::<SinkRecord>(value)
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
            Self::Forbidden(code) => (StatusCode::FORBIDDEN, code, "identity mismatch".to_string()),
            Self::TooLarge => (
                StatusCode::PAYLOAD_TOO_LARGE,
                "evidence_body_too_large",
                "artifact exceeds the configured limit".to_string(),
            ),
            Self::IntegrityFailed => (
                StatusCode::UNPROCESSABLE_ENTITY,
                "evidence_integrity_failed",
                "sha256 mismatch".to_string(),
            ),
            // 5xx 是**故意**的：让 sink 重试并最终把失败落成可见的 rescue 文件。
            Self::Backend(message) => (StatusCode::BAD_GATEWAY, "evidence_write_failed", message),
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
