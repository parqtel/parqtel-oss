use crate::handlers;
use crate::state::AppState;
use axum::{
    extract::State,
    http::{header, HeaderValue, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Redirect, Response},
    routing::{delete, get, post, put},
    Json, Router,
};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tower_http::{
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    trace::TraceLayer,
};

/// Paths that execute scans and therefore honor `query.timeout_secs`. Ingest
/// is deliberately excluded: a slow ingest is backpressure, not a query error.
pub(crate) fn is_query_path(path: &str) -> bool {
    path.starts_with("/api/v1/query")
        || path.starts_with("/api/v1/label")
        || path == "/api/v1/labels"
        || path == "/api/v1/logs"
        || path.starts_with("/api/v1/logs/tail")
        || path == "/v1/logs/count"
        || path == "/v1/logs/fields"
        || path == "/v1/logs/field_values"
        || path == "/v1/traces/search"
        || path == "/v1/correlate"
        || path == "/v1/search"
        // Grafana SimpleJSON queries scan the same blocks.
        || path == "/query"
        || path == "/search"
}

/// OTLP/HTTP ingest paths guarded by the concurrency limit.
pub(crate) fn is_ingest_path(path: &str) -> bool {
    matches!(
        path,
        "/v1/metrics"
            | "/v1/metrics/json"
            | "/v1/logs"
            | "/v1/logs/json"
            | "/v1/traces"
            | "/v1/traces/json"
            | "/api/v1/ingest"
            | "/api/v1/write"
    )
}

async fn query_timeout(
    State(seconds): State<u64>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if !is_query_path(req.uri().path()) {
        return next.run(req).await;
    }
    match tokio::time::timeout(Duration::from_secs(seconds.max(1)), next.run(req)).await {
        Ok(response) => response,
        Err(_) => (StatusCode::REQUEST_TIMEOUT, "query timed out").into_response(),
    }
}

#[derive(Clone)]
struct IngestGuard {
    permits: Arc<Semaphore>,
    state: AppState,
    retry_after_secs: u64,
}

async fn shed_ingest(
    State(guard): State<IngestGuard>,
    req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    if !is_ingest_path(req.uri().path()) {
        return next.run(req).await;
    }
    let permit = match guard.permits.clone().try_acquire_owned() {
        Ok(permit) => permit,
        Err(_) => {
            guard.state.inner.metrics.record_ingest_rejected();
            let mut response = (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({
                    "status": "error",
                    "error": "ingest concurrency limit reached; retry shortly"
                })),
            )
                .into_response();
            if let Ok(value) = HeaderValue::from_str(&guard.retry_after_secs.to_string()) {
                response.headers_mut().insert(header::RETRY_AFTER, value);
            }
            return response;
        }
    };
    let response = next.run(req).await;
    drop(permit);
    response
}

/// Builds the axum [Router] with all routes and middleware.
pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let query_config = state.inner.config.query.clone();
    let ingest_config = state.inner.config.ingest.clone();
    let auth = state.inner.auth.clone();
    let ingest_guard = IngestGuard {
        permits: Arc::new(Semaphore::new(ingest_config.max_concurrent_requests.max(1))),
        state: state.clone(),
        retry_after_secs: ingest_config.load_shed_retry_after_secs.max(1),
    };

    // Every role keeps liveness, readiness, metrics and the debugging probes;
    // an operator scrapes the same endpoints regardless of what the pod does.
    let mut routes = Router::new()
        .route("/health", get(handlers::misc::health))
        .route("/auth/login", get(crate::auth::login))
        .route("/auth/callback", get(crate::auth::callback))
        // Readiness is separate from liveness: /health is a static liveness
        // probe, /ready checks the storage path is actually usable.
        .route("/ready", get(handlers::misc::ready))
        .route("/metrics", get(handlers::misc::metrics))
        .route("/api/v1/stats", get(handlers::misc::stats))
        .route("/api/v1/ingest_rates", get(handlers::misc::ingest_rates))
        // Self-profiling (CPU bottleneck analysis; 404s when disabled).
        // Flamegraph views: `go tool pprof -http=:0` on the protobuf profile.
        .route("/debug/pprof/profile", get(crate::otel_sli::pprof_profile))
        .route("/debug/pprof/summary", get(crate::otel_sli::pprof_summary))
        .route("/debug/pprof/memory", get(crate::otel_sli::pprof_memory));

    routes
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        // Only scan endpoints get the query timeout. Ingest overload must
        // surface as 429/503, not as a misleading query timeout.
        .layer(axum::middleware::from_fn_with_state(
            query_config.timeout_secs,
            query_timeout,
        ))
        // axum's own `DefaultBodyLimit` is 2 MB and applies to every route that
        // does not override it. It was silently the *effective* ingest limit:
        // a 2.15 MB OTLP batch was rejected with 413 even though
        // `ingest.max_body_size` defaults to 10 MB, so raising the setting
        // appeared to do nothing. Both layers are applied so the configured
        // value is authoritative.
        .layer(axum::extract::DefaultBodyLimit::max(
            ingest_config.max_body_size,
        ))
        .layer(RequestBodyLimitLayer::new(ingest_config.max_body_size))
        // OTLP/HTTP exporters gzip their bodies (the Collector's `otlphttp`
        // exporter does by default). Placed outside the limits above so they
        // bound the *decompressed* size: a small gzip bomb is refused with 413
        // rather than inflated into memory. Other encodings get 415.
        .layer(tower_http::decompression::RequestDecompressionLayer::new().gzip(true))
        // Bounded ingest concurrency: when every permit is held, reject the
        // request instead of letting the per-request work queue grow.
        .layer(axum::middleware::from_fn_with_state(
            ingest_guard,
            shed_ingest,
        ))
        // Outermost layer: records the golden signals (count + latency
        // histogram) for the whole request, including any inner middleware.
        .layer(axum::middleware::from_fn(
            crate::otel_sli::http_sli_middleware,
        ))
        // Outermost: reject unauthenticated requests before any scan or body
        // work. Disabled by default for the single-user deployment.
        .layer(axum::middleware::from_fn_with_state(
            auth,
            crate::auth::require_auth,
        ))
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_paths_are_timed() {
        for path in [
            "/api/v1/query",
            "/api/v1/query_range",
            "/api/v1/labels",
            "/api/v1/label/service.name/values",
            "/api/v1/logs",
            "/v1/logs/count",
            "/v1/traces/search",
            "/v1/correlate",
            "/v1/search",
        ] {
            assert!(is_query_path(path), "{path} should be timed");
        }
        for path in ["/health", "/metrics", "/api/v1/stats", "/v1/metrics"] {
            assert!(!is_query_path(path), "{path} should not be timed");
        }
    }

    #[test]
    fn ingest_paths_are_load_shed() {
        for path in [
            "/v1/metrics",
            "/v1/metrics/json",
            "/v1/logs",
            "/v1/logs/json",
            "/v1/traces",
            "/v1/traces/json",
        ] {
            assert!(is_ingest_path(path), "{path} should be shed");
        }
        for path in ["/api/v1/query", "/health", "/v1/logs/count"] {
            assert!(!is_ingest_path(path), "{path} should not be shed");
        }
    }
}
