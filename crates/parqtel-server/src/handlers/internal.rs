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
/// The reader sends one global watermark — the newest block it can see on any
/// node — so each writer clamps it down to the newest block it flushed itself.
/// A writer that flushed earlier would otherwise drop its own buffered rows in
/// the gap. The overlap this admits is deduped by the reader's fan-out merge.
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
    let (start_ns, end_ns) = (request.start_ns, request.end_ns);
    // The reader sends one global watermark — the newest block it can see on
    // any node. A writer that flushed earlier still holds newer rows in its
    // buffer, so clamp the watermark down to this writer's own newest block.
    let since_ns = request.since_ns.min(
        state
            .inner
            .query_executor
            .local_flushed_watermark_ns(request.signal)
            .await,
    );
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

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;
    use axum::body::to_bytes;
    use parqtel_core::models::storage::{BlockMetadata, SignalType};
    use parqtel_core::{Config, DataPoint, LabelSet, MetricValue};

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }

    fn point(timestamp_ns: i64, value: f64) -> DataPoint {
        DataPoint {
            timestamp_ns,
            value: MetricValue::Double(value),
            labels: LabelSet::try_from_iter([("host", "w1")]).unwrap(),
        }
    }

    async fn body_json(response: Response) -> serde_json::Value {
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    fn timestamps(body: &serde_json::Value) -> Vec<i64> {
        body["points"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| row[1]["timestamp_ns"].as_i64().unwrap())
            .collect()
    }

    /// Skewed writers: the reader's global watermark is the newest block it can
    /// see anywhere (1000, from writer-0), but this writer flushed only to 200.
    /// Without the clamp its buffered rows in `(200, 1000]` are lost.
    #[tokio::test]
    async fn buffer_query_clamps_the_global_watermark_to_the_local_flush() {
        let mut config = Config::default();
        config.query.internal_auth_token = Some("test-token".into());
        let state = AppState::default_for_tests_with_config(config).await;

        let dir = tempfile::tempdir().unwrap();
        let block_path = dir.path().join("skew-200.parquet");
        std::fs::write(&block_path, b"").unwrap();
        state.inner.index.write().await.add(BlockMetadata {
            path: block_path,
            start_timestamp_ns: 0,
            end_timestamp_ns: 200,
            row_count: 1,
            size_bytes: 1,
            metric_names: Default::default(),
            label_names: Default::default(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        });

        state
            .inner
            .query_executor
            .memory_buffer()
            .push_metrics(
                "skew_total",
                &[point(150, 1.5), point(300, 3.0), point(900, 9.0)],
            )
            .await;

        let request = RemoteBufferRequest {
            signal: RemoteSignal::Metrics,
            metric: Some("skew_total".into()),
            since_ns: 1000,
            start_ns: 0,
            end_ns: 2000,
            limit: 100,
        };
        let body = body_json(
            buffer_query(
                State(state.clone()),
                bearer("test-token"),
                Json(request.clone()),
            )
            .await,
        )
        .await;
        // Both unflushed rows survive — the reader merges and dedups across
        // writers. The row at 150 is already in this writer's block.
        assert_eq!(timestamps(&body), vec![300, 900]);

        // A reader watermark below this writer's flush is not raised: the
        // clamp only ever lowers it, so the same buffered rows come back.
        let body = body_json(
            buffer_query(
                State(state.clone()),
                bearer("test-token"),
                Json(RemoteBufferRequest {
                    since_ns: 250,
                    ..request
                }),
            )
            .await,
        )
        .await;
        assert_eq!(timestamps(&body), vec![300, 900]);

        // The auth guard still runs before the clamp: an unauthenticated
        // request never reaches the buffer.
        let response = buffer_query(
            State(state),
            HeaderMap::new(),
            Json(RemoteBufferRequest {
                signal: RemoteSignal::Metrics,
                metric: Some("skew_total".into()),
                since_ns: 1000,
                start_ns: 0,
                end_ns: 2000,
                limit: 100,
            }),
        )
        .await;
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
