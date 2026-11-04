//! 读端点：mTLS（P0 由内网 TLS 终结层注入身份头）+ scope。
//!
//! 授权语义按设计稿 §7.2：一次读要同时满足身份、租户范围、敏感度、对象状态；
//! **任何一条不满足都回 404**，不泄露"存在但不能看"。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::domain::{Kind, Sensitivity};
use crate::infra::ListFilter;
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
pub struct ListQuery {
    #[serde(default)]
    pub kind: Option<String>,
    #[serde(default)]
    pub subject_id: Option<String>,
    #[serde(default)]
    pub since: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
    /// 可选：显式指定租户；缺省用调用方身份里的第一个。必须落在身份集合内。
    #[serde(default)]
    pub tenant: Option<String>,
}

/// `GET /internal/evidence`：按时间倒序列出调用方可见的元数据。
pub async fn list_evidence(
    State(state): State<ReadState>,
    headers: HeaderMap,
    Query(query): Query<ListQuery>,
) -> Response {
    let principal = match principal_from_headers(&headers) {
        Some(principal) => principal,
        None => return unauthorized(),
    };
    let Some(fallback_tenant) = principal.tenant_ids.first().cloned() else {
        return unauthorized();
    };
    let kind = match query.kind.as_deref().filter(|raw| !raw.is_empty()) {
        Some(raw) => match Kind::parse(raw) {
            Some(kind) => Some(kind),
            None => return bad_request("evidence_kind_unknown", "unknown kind"),
        },
        None => None,
    };
    let since = query
        .since
        .as_deref()
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|value| value.with_timezone(&Utc));
    let filter = ListFilter {
        kind,
        subject_id: query.subject_id.filter(|value| !value.is_empty()),
        since,
    };
    let tenant = query.tenant.unwrap_or(fallback_tenant);
    match state
        .service
        .list(
            &principal,
            &tenant,
            &filter,
            query.limit.unwrap_or(50),
            query.cursor.as_deref(),
        )
        .await
    {
        Ok(page) => Json(page).into_response(),
        Err(error) => read_error_response(error),
    }
}

fn bad_request(code: &str, message: &str) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "error": { "code": code, "message": message }
        })),
    )
        .into_response()
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

/// `GET /internal/evidence/{artifact_id}/content`——代理读正文（§7.2）。
///
/// 可见性判断与元数据读一致；正文在对象层是 zstd 压缩的，这里解压后回原文。
/// 本地文件模式（`objects.kind = file`）没有签名 URL，就靠这条读正文。
pub async fn content(
    State(state): State<ReadState>,
    headers: HeaderMap,
    Path(artifact_id): Path<String>,
) -> Response {
    let principal = match principal_from_headers(&headers) {
        Some(principal) => principal,
        None => return unauthorized(),
    };
    match state.service.read_content(&principal, &artifact_id).await {
        Ok((_artifact, body)) => (
            [
                (header::CONTENT_TYPE, "application/json; charset=utf-8"),
                // 证据正文不进任何缓存（浏览器 / 中间代理）。
                (header::CACHE_CONTROL, "no-store"),
                // 正文是文件，不是页面：避免被当成 HTML 渲染。
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            body,
        )
            .into_response(),
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
        ReadError::Backend(detail) => {
            // 细节只进日志：后端原文带宿主路径 / errno，回给调用方等于泄露目录结构。
            log::error!("evidence_read_failed: {detail}");
            (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({
                    "error": {
                        "code": "evidence_read_failed",
                        "message": "internal error while reading the artifact"
                    }
                })),
            )
                .into_response()
        }
        // `objects.kind = file`：本地目录没有可对外下发的签名 URL。
        ReadError::Unsupported(message) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(serde_json::json!({
                "error": {
                    "code": "evidence_presign_unsupported",
                    "message": format!(
                        "{message}：本地文件模式不签发预签名 URL，正文在证据服务的 objects.dir 下（要签名 URL 请用 objects.kind = s3）"
                    )
                }
            })),
        )
            .into_response(),
    }
}
