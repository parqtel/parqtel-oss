use axum::{
    extract::State,
    http::{header, StatusCode, HeaderMap},
    response::{IntoResponse, Response},
    Json,
};
use crate::state::AppState;

/// Handler for GET /health.
pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Handler for GET /metrics (Prometheus text format).
pub async fn metrics(
    State(state): State<AppState>,
) -> (StatusCode, String) {
    let body = state.inner.metrics.render(&state.inner.index).await;
    (StatusCode::OK, body)
}

/// Handler for GET /ui (Embedded Dashboard).
///
/// Serves gzip-compressed HTML with ETag caching.
pub async fn ui(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Response {
    let etag = &state.inner.ui_etag;
    
    // Check if client has matching ETag
    if let Some(if_none_match) = headers.get(header::IF_NONE_MATCH) {
        if if_none_match == etag {
            return StatusCode::NOT_MODIFIED.into_response();
        }
    }

    (
        StatusCode::OK,
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CONTENT_ENCODING, "gzip"),
            (header::ETAG, etag),
            (header::CACHE_CONTROL, "public, max-age=3600"),
        ],
        state.inner.ui_content.clone(),
    ).into_response()
}

const OPENAPI_SPEC: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../openapi.yaml"));

/// Handler for GET /openapi.yaml
pub async fn openapi_spec() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/yaml; charset=utf-8")],
        OPENAPI_SPEC,
    ).into_response()
}
