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
    let wire_bytes = body.len() as u64;
    match state.inner.ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            state.inner.metrics.rates.record_metrics(count, wire_bytes);
            crate::otel_sli::record_ingest("metrics", count);
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
    let wire_bytes = body.len() as u64;
    match state.inner.ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            state.inner.metrics.rates.record_metrics(count, wire_bytes);
            crate::otel_sli::record_ingest("metrics", count);
            tracing::debug!(
                signal = "metrics",
                ingested = count,
                wire_bytes,
                "HTTP metrics ingest accepted"
            );
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
    let wire_bytes = body.len() as u64;
    match state.inner.log_ingestion_service.ingest_proto(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            state.inner.metrics.rates.record_logs(count, wire_bytes);
            crate::otel_sli::record_ingest("logs", count);
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
    let wire_bytes = body.len() as u64;
    match state.inner.log_ingestion_service.ingest_json(body).await {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            state.inner.metrics.rates.record_logs(count, wire_bytes);
            tracing::debug!(
                signal = "logs",
                ingested = count,
                wire_bytes,
                "HTTP logs ingest accepted"
            );
            crate::otel_sli::record_ingest("logs", count);
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
    ingest_traces(state, body, false).await
}

/// OTLP content negotiation for `/v1/traces`.
pub async fn ingest_otlp_traces(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    ingest_traces(state, body, is_protobuf(&headers)).await
}

async fn ingest_traces(state: AppState, body: Bytes, protobuf: bool) -> Response {
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
    let wire_bytes = body.len() as u64;
    let service = &state.inner.trace_ingestion_service;
    let result = if protobuf {
        service.ingest_proto(body).await
    } else {
        service.ingest_json(body).await
    };
    match result {
        Ok(count) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            state.inner.metrics.rates.record_spans(count, wire_bytes);
            crate::otel_sli::record_ingest("traces", count);
            tracing::debug!(
                signal = "traces",
                ingested = count,
                wire_bytes,
                "HTTP traces ingest accepted"
            );
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

fn is_protobuf(headers: &axum::http::HeaderMap) -> bool {
    headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|c| c.contains("protobuf"))
        .unwrap_or(false)
}

/// POST /api/v1/ingest — generic newline/JSON-array log ingest.
///
/// Maps a JSON object (or array) into the log model using
/// `ingest.json_{timestamp,body,severity}_field`; every other field becomes a
/// log attribute. The service name comes from `X-Parqtel-Service` (default
/// `"default"`).
pub async fn ingest_generic_json(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Response {
    if body.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "status": "error", "error": "Empty body" })),
        )
            .into_response();
    }
    let service = headers
        .get("x-parqtel-service")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("default");
    let logs = match parse_generic_json(&body, service, &state.inner.config.ingest) {
        Ok(logs) => logs,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "status": "error", "error": e })),
            )
                .into_response()
        }
    };
    let count = logs.len() as u64;
    match state
        .inner
        .log_ingestion_service
        .ingest_logs_live(logs)
        .await
    {
        Ok(_) => {
            state
                .inner
                .metrics
                .ingested_points
                .fetch_add(count, std::sync::atomic::Ordering::Relaxed);
            state
                .inner
                .metrics
                .rates
                .record_logs(count, body.len() as u64);
            crate::otel_sli::record_ingest("logs", count);
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

fn json_scalar_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn parse_json_timestamp(value: &serde_json::Value) -> i64 {
    if let Some(value) = value.as_i64() {
        // Accept seconds, milliseconds, microseconds and nanoseconds.
        return match value {
            value if value > 10_000_000_000_000_000 => value,
            value if value > 10_000_000_000_000 => value * 1_000,
            value if value > 10_000_000_000 => value * 1_000_000,
            value => value * 1_000_000_000,
        };
    }
    if let Some(text) = value.as_str() {
        if let Ok(value) = text.parse::<i64>() {
            return parse_json_timestamp(&serde_json::Value::Number(value.into()));
        }
        if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
            return parsed.timestamp_nanos_opt().unwrap_or(0);
        }
    }
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

fn json_severity(level: &str) -> (i32, String) {
    let number = match level.to_ascii_lowercase().as_str() {
        "trace" | "trace2" | "trace3" | "trace4" => 1,
        "debug" | "debug2" | "debug3" | "debug4" => 5,
        "info" | "information" | "notice" => 9,
        "warn" | "warning" => 13,
        "error" | "err" => 17,
        "fatal" | "critical" | "crit" | "alert" | "emergency" => 21,
        _ => 9,
    };
    (number, level.to_string())
}

/// Decodes one generic JSON body into log records. Used by the HTTP handler
/// and the Kafka consumer so both accept exactly the same shape.
pub(crate) fn parse_generic_json(
    body: &[u8],
    service: &str,
    config: &parqtel_core::config::IngestConfig,
) -> Result<Vec<parqtel_core::LogRecord>, String> {
    let value: serde_json::Value =
        serde_json::from_slice(body).map_err(|e| format!("invalid JSON: {e}"))?;
    let objects = match value {
        serde_json::Value::Array(values) => values,
        object @ serde_json::Value::Object(_) => vec![object],
        _ => return Err("body must be a JSON object or array".into()),
    };
    let mut logs = Vec::with_capacity(objects.len());
    for object in objects {
        let Some(fields) = object.as_object() else {
            return Err("array entries must be objects".into());
        };
        let timestamp_ns = fields
            .get(&config.json_timestamp_field)
            .map(parse_json_timestamp)
            .unwrap_or_else(|| chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0));
        let body_text = fields
            .get(&config.json_body_field)
            .map(json_scalar_string)
            .unwrap_or_else(|| object.to_string());
        let level = fields
            .get(&config.json_severity_field)
            .map(json_scalar_string)
            .unwrap_or_else(|| "INFO".to_string());
        let (severity_number, severity_text) = json_severity(&level);
        let attributes: Vec<(String, String)> = fields
            .iter()
            .filter(|(key, _)| {
                key.as_str() != config.json_timestamp_field
                    && key.as_str() != config.json_body_field
                    && key.as_str() != config.json_severity_field
            })
            .map(|(key, value)| (key.clone(), json_scalar_string(value)))
            .collect();
        let attributes = parqtel_core::LabelSet::try_from_iter(attributes).unwrap_or_default();
        let resource_attributes =
            parqtel_core::LabelSet::try_from_iter(vec![("service.name", service)])
                .unwrap_or_default();
        logs.push(parqtel_core::LogRecord::new(
            timestamp_ns,
            chrono::Utc::now()
                .timestamp_nanos_opt()
                .unwrap_or(timestamp_ns),
            severity_number,
            severity_text,
            body_text,
            attributes,
            resource_attributes,
            [0; 16],
            [0; 8],
            0,
            "generic-json".into(),
            "1.0".into(),
        ));
    }
    Ok(logs)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn timestamp_units_are_detected() {
        let seconds = parse_json_timestamp(&serde_json::json!(1_700_000_000));
        assert_eq!(seconds, 1_700_000_000 * 1_000_000_000);
        let millis = parse_json_timestamp(&serde_json::json!(1_700_000_000_000i64));
        assert_eq!(millis, 1_700_000_000_000_000_000);
        let rfc = parse_json_timestamp(&serde_json::json!("2024-01-01T00:00:00Z"));
        assert!(rfc > 1_700_000_000_000_000_000);
    }

    #[test]
    fn severity_maps_to_otel_numbers() {
        assert_eq!(json_severity("error").0, 17);
        assert_eq!(json_severity("WARN").0, 13);
        assert_eq!(json_severity("info").0, 9);
        assert_eq!(json_severity("unknown").0, 9);
    }
}
