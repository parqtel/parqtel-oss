use crate::state::AppState;
use axum::{
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

/// Handler for POST /v1/metrics (OTLP Protobuf).
pub async fn ingest_proto(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        state
            .inner
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    state
        .inner
        .metrics
        .batches_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.inner.ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .ingest_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/metrics/json (OTLP JSON).
pub async fn ingest_json(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        state
            .inner
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    state
        .inner
        .metrics
        .batches_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.inner.ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .ingest_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/logs (OTLP Protobuf).
pub async fn ingest_logs_proto(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        state
            .inner
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    state
        .inner
        .metrics
        .batches_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.inner.log_ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .ingest_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/logs/json (OTLP JSON).
pub async fn ingest_logs_json(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        state
            .inner
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    state
        .inner
        .metrics
        .batches_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.inner.log_ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .ingest_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/traces (OTLP Protobuf).
pub async fn ingest_traces_proto(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        state
            .inner
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    state
        .inner
        .metrics
        .batches_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.inner.trace_ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .ingest_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

/// Handler for POST /v1/traces/json (OTLP JSON).
pub async fn ingest_traces_json(State(state): State<AppState>, body: Bytes) -> Response {
    if body.is_empty() {
        state
            .inner
            .metrics
            .ingest_errors
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    state
        .inner
        .metrics
        .batches_received
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    match state.inner.trace_ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            (StatusCode::OK, Json(json!({ "ingested": count }))).into_response()
        }
        Err(e) => {
            state
                .inner
                .metrics
                .ingest_errors
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            map_error(e)
        }
    }
}

fn map_error(e: parqtel_core::Error) -> Response {
    let status = match &e {
        parqtel_core::Error::Validation(_) | parqtel_core::Error::InvalidOtlp(_) => {
            StatusCode::BAD_REQUEST
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (
        status,
        Json(json!({"status": "error", "error": e.to_string()})),
    )
        .into_response()
}

/// OTLP content negotiation for `/v1/metrics`: protobuf when the request is
/// `application/x-protobuf`, otherwise JSON (OTLP spec allows both).
pub async fn ingest_otlp_metrics(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if is_protobuf(&headers) {
        ingest_proto(State(state), body).await
    } else {
        ingest_json(State(state), body).await
    }
}

/// OTLP content negotiation for `/v1/logs`.
pub async fn ingest_otlp_logs(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if is_protobuf(&headers) {
        ingest_logs_proto(State(state), body).await
    } else {
        ingest_logs_json(State(state), body).await
    }
}

/// OTLP content negotiation for `/v1/traces`.
pub async fn ingest_otlp_traces(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if is_protobuf(&headers) {
        ingest_traces_proto(State(state), body).await
    } else {
        ingest_traces_json(State(state), body).await
    }
}

fn is_protobuf(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|c| c.contains("protobuf"))
        .unwrap_or(false)
}
