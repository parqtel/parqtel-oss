use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use crate::state::AppState;
// TraceIngestionService is used via AppState

/// Handler for POST /v1/metrics (OTLP Protobuf).
pub async fn ingest_proto(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (StatusCode::BAD_REQUEST, Json(json!({ "status": "error", "error": "Empty body" }))).into_response();
    }
    state.inner.metrics.batches_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ingestion_service = state.inner.ingestion_service.lock().await;
    match ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state.inner.metrics.ingested_points.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/metrics/json (OTLP JSON).
pub async fn ingest_json(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (StatusCode::BAD_REQUEST, Json(json!({ "status": "error", "error": "Empty body" }))).into_response();
    }
    state.inner.metrics.batches_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ingestion_service = state.inner.ingestion_service.lock().await;
    match ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state.inner.metrics.ingested_points.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/logs (OTLP Protobuf).
pub async fn ingest_logs_proto(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (StatusCode::BAD_REQUEST, Json(json!({ "status": "error", "error": "Empty body" }))).into_response();
    }
    state.inner.metrics.batches_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ingestion_service = state.inner.log_ingestion_service.lock().await;
    match ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state.inner.metrics.ingested_points.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/logs/json (OTLP JSON).
pub async fn ingest_logs_json(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (StatusCode::BAD_REQUEST, Json(json!({ "status": "error", "error": "Empty body" }))).into_response();
    }
    state.inner.metrics.batches_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ingestion_service = state.inner.log_ingestion_service.lock().await;
    match ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state.inner.metrics.ingested_points.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/traces (OTLP Protobuf).
pub async fn ingest_traces_proto(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (StatusCode::BAD_REQUEST, Json(json!({ "status": "error", "error": "Empty body" }))).into_response();
    }
    state.inner.metrics.batches_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ingestion_service = state.inner.trace_ingestion_service.lock().await;
    match ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state.inner.metrics.ingested_points.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/traces/json (OTLP JSON).
pub async fn ingest_traces_json(
    State(state): State<AppState>,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (StatusCode::BAD_REQUEST, Json(json!({ "status": "error", "error": "Empty body" }))).into_response();
    }
    state.inner.metrics.batches_received.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ingestion_service = state.inner.trace_ingestion_service.lock().await;
    match ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state.inner.metrics.ingested_points.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state.inner.metrics.ingest_errors.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

fn map_error(e: parqtel_core::Error) -> Response {
    let status = match &e {
        parqtel_core::Error::Validation(_) => StatusCode::BAD_REQUEST,
        parqtel_core::Error::InvalidOtlp(_) => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (status, Json(json!({
        "status": "error",
        "error": e.to_string()
    }))).into_response()
}
