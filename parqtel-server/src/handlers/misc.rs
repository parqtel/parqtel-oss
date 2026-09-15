use crate::state::AppState;
use axum::{
    extract::{Query, State},
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;

/// Handler for GET /health.
pub async fn health() -> impl IntoResponse {
    Json(serde_json::json!({"status": "ok"}))
}

/// Handler for GET /api/v1/stats — storage, block, buffer, and config
/// snapshot for the UI overview and settings panels. Reads block-index
/// metadata only (no Parquet decode), plus live buffer counts.
pub async fn stats(State(state): State<AppState>) -> Response {
    let cfg = &state.inner.config;
    let [metrics, logs, traces] = match state.inner.query_executor.storage_stats().await {
        Ok(s) => s,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({
                    "status": "error",
                    "error": e.to_string()
                })),
            )
                .into_response()
        }
    };
    let (buf_metrics, buf_logs, buf_spans) =
        state.inner.query_executor.memory_buffer().stats().await;

    Json(serde_json::json!({
        "status": "success",
        "data": {
            "storage": {
                "metrics": metrics,
                "logs": logs,
                "traces": traces,
            },
            "buffer": {
                "metrics_points": buf_metrics,
                "logs": buf_logs,
                "spans": buf_spans,
            },
            "config": {
                "storage": {
                    "data_dir": cfg.storage.data_dir,
                    "block_duration_secs": cfg.storage.block_duration_secs,
                    "max_rows_per_block": cfg.storage.max_rows_per_block,
                    "compression": cfg.storage.compression,
                    "retention_days": cfg.storage.retention_days,
                },
                "logs": {
                    "data_dir": cfg.logs.data_dir,
                    "block_duration_secs": cfg.logs.block_duration_secs,
                    "max_rows_per_block": cfg.logs.max_rows_per_block,
                    "compression": cfg.logs.compression,
                    "retention_days": cfg.logs.retention_days,
                },
                "ingest": {
                    "max_body_size": cfg.ingest.max_body_size,
                    "tail_sampling": cfg.ingest.tail_sampling,
                },
                "query": {
                    "max_series": cfg.query.max_series,
                    "max_samples_per_series": cfg.query.max_samples_per_series,
                    "timeout_secs": cfg.query.timeout_secs,
                },
            },
        }
    }))
    .into_response()
}

/// Handler for GET /api/v1/ingest_rates — live per-signal ingestion rates
/// for the UI overview. Returns current/60s/5m/15m rates, the seconds since
/// the last item (gap), and the last `history_secs` seconds of per-second
/// counts for sparklines. `history_secs` defaults to 180 and may be raised
/// up to the full 15-minute in-memory wheel (`RATE_WINDOW_BUCKETS` = 900).
/// Reads in-memory counters only — no locks, no Parquet access — so it
/// stays cheap at any ingest volume.
#[derive(Debug, Deserialize)]
pub struct RateHistoryParams {
    history_secs: Option<usize>,
}

pub async fn ingest_rates(
    State(state): State<AppState>,
    Query(params): Query<RateHistoryParams>,
) -> Response {
    const DEFAULT_HISTORY_SECS: usize = 180;
    let history_secs = params
        .history_secs
        .unwrap_or(DEFAULT_HISTORY_SECS)
        .clamp(1, crate::metrics::RATE_WINDOW_BUCKETS);
    let now = crate::metrics::now_secs();
    let [metrics, logs, spans] = state.inner.metrics.rates.snapshot(now);
    let [h_metrics, h_logs, h_spans] = state.inner.metrics.rates.history(history_secs, now);

    Json(serde_json::json!({
        "status": "success",
        "data": {
            "now": now,
            "history_secs": history_secs,
            "metrics": metrics,
            "logs": logs,
            "traces": spans,
            "history": {
                "metrics": h_metrics,
                "logs": h_logs,
                "traces": h_spans,
            },
        }
    }))
    .into_response()
}

/// Handler for GET /metrics (Prometheus text format).
pub async fn metrics(State(state): State<AppState>) -> (StatusCode, String) {
    let body = state.inner.metrics.render(&state.inner.index).await;
    (StatusCode::OK, body)
}

/// Handler for GET /ui (Embedded Dashboard).
///
/// Serves gzip-compressed HTML with ETag caching.
pub async fn ui(State(state): State<AppState>, headers: HeaderMap) -> Response {
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
    )
        .into_response()
}

const OPENAPI_SPEC: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../openapi.yaml"));

/// Handler for GET /openapi.yaml
pub async fn openapi_spec() -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/yaml; charset=utf-8")],
        OPENAPI_SPEC,
    )
        .into_response()
}
