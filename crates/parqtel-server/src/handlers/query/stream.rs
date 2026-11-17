//! Chunked NDJSON log streaming.
//!
//! Each time slice is executed through the normal log query path, so filters,
//! ordering, and limits stay identical; only the response is streamed instead
//! of materialised as one JSON array. A 10M-row query is still bounded by the
//! per-slice limit, not by the whole result set.

use super::logs::{query_logs, LogQuery};
use crate::state::AppState;
use async_stream::stream;
use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

pub async fn stream_logs(
    State(state): State<AppState>,
    params: Result<Query<LogQuery>, axum::extract::rejection::QueryRejection>,
) -> Response {
    let Query(params) = match params {
        Ok(params) => params,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"status": "error", "error": e.to_string()})),
            )
                .into_response()
        }
    };
    let start = params.start;
    let end = params.end;
    if !(start.is_finite() && end.is_finite()) || end <= start {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"status": "error", "error": "start/end must be finite and end > start"})),
        )
            .into_response();
    }
    // One slice per 5 minutes, rounded up; cap each slice's rows so the
    // stream's working set is bounded.
    let slice_secs = 300.0_f64.min((end - start).max(0.001));
    let per_slice = params.limit.unwrap_or(1000).clamp(1, 10_000);
    let stream = stream! {
        let mut cursor = start;
        while cursor < end {
            let slice_end = (cursor + slice_secs).min(end);
            let chunk = LogQuery {
                query: params.query.clone(),
                start: cursor,
                end: slice_end,
                limit: Some(per_slice),
                order: params.order.clone(),
                severity_min: params.severity_min,
                search: params.search.clone(),
            };
            let response = query_logs(State(state.clone()), Ok(Query(chunk))).await;
            let status = response.status();
            if !status.is_success() {
                let body = axum::body::to_bytes(response.into_body(), 1 << 20)
                    .await
                    .unwrap_or_default();
                yield Ok::<_, std::convert::Infallible>(body);
                return;
            }
            let body = match axum::body::to_bytes(response.into_body(), 16 << 20).await {
                Ok(body) => body,
                Err(e) => {
                    let line = format!("{{\"error\":\"{e}\"}}\n");
                    yield Ok(line.into_bytes().into());
                    return;
                }
            };
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
                if let Some(logs) = value["data"]["logs"].as_array() {
                    for log in logs {
                        if let Ok(line) = serde_json::to_string(log) {
                            yield Ok(format!("{line}\n").into_bytes().into());
                        }
                    }
                }
            }
            cursor = slice_end;
        }
    };
    let mut response = Body::from_stream(stream).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/x-ndjson"),
    );
    response
}
