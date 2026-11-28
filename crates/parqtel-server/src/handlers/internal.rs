//! Internal endpoints served by writers to the reader tier.
//!
//! These routes are not part of the public API: they carry a shared bearer
//! token and expose the writer's in-memory buffer so a reader can see the
//! seconds that have not been flushed to object storage yet.

use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use parqtel_query::remote::{RemoteBufferRequest, RemoteBufferRows, RemoteSignal, MAX_BUFFER_ROWS};

use crate::state::AppState;

/// `POST /internal/v1/buffer` — rows buffered on this writer, newer than the
/// reader's watermark.
///
/// Disabled (404) when no `query.internal_auth_token` is configured: an
/// unauthenticated buffer endpoint would expose live telemetry to anything
/// that can reach the pod.
pub async fn buffer_query(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(request): Json<RemoteBufferRequest>,
) -> Response {
    let Some(expected) = state.inner.config.query.internal_auth_token.as_deref() else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "status": "error",
                "error": "internal endpoints are disabled without query.internal_auth_token"
            })),
        )
            .into_response();
    };
    if !bearer_matches(&headers, expected) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"status": "error", "error": "unauthorized"})),
        )
            .into_response();
    }
    if request.start_ns > request.end_ns {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "status": "error",
                "error": "start_ns must not be after end_ns"
            })),
        )
            .into_response();
    }

    let limit = request.limit.clamp(1, MAX_BUFFER_ROWS);
    let (start_ns, end_ns, since_ns) = (request.start_ns, request.end_ns, request.since_ns);
    let buffer = state.inner.query_executor.memory_buffer();

    let rows = match request.signal {
        RemoteSignal::Metrics => {
            let Some(metric) = request.metric.as_deref() else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "status": "error",
                        "error": "signal=metrics requires a metric name"
                    })),
                )
                    .into_response();
            };
            let (points, _) = buffer
                .scan_metrics_filtered(metric, start_ns, end_ns, None)
                .await;
            let mut out: Vec<(String, parqtel_core::DataPoint)> = points
                .into_iter()
                .filter(|point| point.timestamp_ns > since_ns)
                .map(|point| (metric.to_string(), point))
                .collect();
            out.truncate(limit);
            let watermark = out.iter().map(|(_, point)| point.timestamp_ns).max();
            RemoteBufferRows {
                watermark_ns: watermark,
                points: out,
                ..Default::default()
            }
        }
        RemoteSignal::Logs => {
            let logs = buffer
                .scan_logs(start_ns, end_ns)
                .await
                .into_iter()
                .filter(|log| log.timestamp_ns > since_ns)
                .take(limit)
                .collect::<Vec<_>>();
            let watermark = logs.iter().map(|log| log.timestamp_ns).max();
            RemoteBufferRows {
                watermark_ns: watermark,
                logs,
                ..Default::default()
            }
        }
        RemoteSignal::Traces => {
            let spans = buffer
                .scan_spans(start_ns, end_ns)
                .await
                .into_iter()
                .filter(|span| span.start_time_ns > since_ns)
                .take(limit)
                .collect::<Vec<_>>();
            let watermark = spans.iter().map(|span| span.start_time_ns).max();
            RemoteBufferRows {
                watermark_ns: watermark,
                spans,
                ..Default::default()
            }
        }
    };

    Json(rows).into_response()
}

/// Compares the bearer token without an early exit on the first wrong byte.
fn bearer_matches(headers: &HeaderMap, expected: &str) -> bool {
    let Some(value) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    let Some(provided) = value.strip_prefix("Bearer ") else {
        return false;
    };
    if provided.len() != expected.len() {
        return false;
    }
    provided
        .bytes()
        .zip(expected.bytes())
        .fold(0u8, |diff, (a, b)| diff | (a ^ b))
        == 0
}
