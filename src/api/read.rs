//! 读端点：mTLS（P0 由内网 TLS 终结层注入身份头）+ scope。
//!
//! 授权语义按设计稿 §7.2：一次读要同时满足身份、租户范围、敏感度、对象状态；
//! **任何一条不满足都回 404**，不泄露"存在但不能看"。

use axum::Json;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;

use crate::domain::Sensitivity;
use crate::service::{PresignOutcome, ReadError, ReadPrincipal};

use super::ReadState;

pub async fn metadata(
    State(state): State<ReadState>,
    headers: HeaderMap,
    Path(artifact_id): Path<String>,
) -> Response {
    let principal = match principal_from_headers(&headers) {
        Some(principal) => principal,
        None => return unauthorized(),
    };
    match state.service.metadata(&principal, &artifact_id).await {
        Ok(artifact) => Json(artifact).into_response(),
        Err(error) => read_error_response(error),
    }
}

#[derive(Debug, Default, Deserialize)]
pub struct PresignRequest {
    /// 期望有效期；服务端会夹到 60s（D8）。
    #[serde(default)]
    pub ttl_seconds: Option<u64>,
}

/// `POST /internal/evidence/{artifact_id}:presign`。
///
/// matchit 不允许参数段和通配段做兄弟，所以 `{artifact_id}` 会把 `art_x:presign`
/// 整段吃下来，由这里拆出动作 —— 对外仍保持设计稿 §12.2 的 `:presign` 形状。
pub async fn presign_at(
    State(state): State<ReadState>,
    headers: HeaderMap,
    Path(raw): Path<String>,
    body: Option<Json<PresignRequest>>,
) -> Response {
    let Some(artifact_id) = raw.strip_suffix(":presign") else {
        return read_error_response(ReadError::NotFound);
    };
    if artifact_id.is_empty() {
        return read_error_response(ReadError::NotFound);
    }
    let principal = match principal_from_headers(&headers) {
        Some(principal) => principal,
        None => return unauthorized(),
    };
    let ttl_seconds = body
        .and_then(|Json(request)| request.ttl_seconds)
        .unwrap_or(60);
    match state
        .service
        .presign(
            &principal,
            artifact_id,
            std::time::Duration::from_secs(ttl_seconds),
        )
        .await
    {
        Ok(outcome) => Json::<PresignOutcome>(outcome).into_response(),
        Err(error) => read_error_response(error),
    }
}

/// 从请求头取身份。P0：由内网 TLS 终结层注入；CA-I 落地后换成证书 claim。
fn principal_from_headers(headers: &HeaderMap) -> Option<ReadPrincipal> {
    let tenant_ids: Vec<String> = header_csv(headers, "x-evidence-tenant")?;
    if tenant_ids.is_empty() {
        return None;
    }
    let scopes = header_csv(headers, "x-evidence-scopes").unwrap_or_default();
    let max = header_value(headers, "x-evidence-max-sensitivity")
        .and_then(|raw| Sensitivity::parse(&raw))
        .unwrap_or(Sensitivity::Metrics);
    Some(ReadPrincipal {
        tenant_ids,
        scopes,
        max_sensitivity: max,
    })
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn header_csv(headers: &HeaderMap, name: &str) -> Option<Vec<String>> {
    header_value(headers, name).map(|raw| {
        raw.split(',')
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect()
    })
}

fn unauthorized() -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(serde_json::json!({
            "error": { "code": "evidence_principal_missing", "message": "missing read principal headers" }
        })),
    )
        .into_response()
}

fn read_error_response(error: ReadError) -> Response {
    match error {
        // 未知 / 跨租户 / 无权限 / 过期 —— 对外只有一种答案。
        ReadError::NotFound | ReadError::Forbidden => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "error": { "code": "evidence_not_found", "message": "artifact not found" }
            })),
        )
            .into_response(),
        ReadError::Backend(message) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "error": { "code": "evidence_read_failed", "message": message }
            })),
        )
            .into_response(),
    }
}
