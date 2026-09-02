use crate::handlers;
use crate::state::AppState;
use axum::{
    response::Redirect,
    routing::{delete, get, post, put},
    Router,
};
use std::time::Duration;
use tower_http::{
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

/// Builds the axum [Router] with all routes and middleware.
pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let query_config = state.inner.config.query.clone();
    let ingest_config = state.inner.config.ingest.clone();

    let routes = Router::new()
        // Health & UI
        .route("/health", get(handlers::misc::health))
        .route("/metrics", get(handlers::misc::metrics))
        .route("/", get(|| async { Redirect::to("/ui") }))
        .route("/ui", get(handlers::misc::ui))
        .route("/oas", get(handlers::misc::openapi_spec))
        // OTLP Ingestion
        .route("/v1/metrics", post(handlers::ingest::ingest_otlp_metrics))
        .route("/v1/metrics/json", post(handlers::ingest::ingest_json))
        .route("/v1/logs", post(handlers::ingest::ingest_otlp_logs))
        .route("/v1/logs/json", post(handlers::ingest::ingest_logs_json))
        .route("/v1/traces", post(handlers::ingest::ingest_traces_json))
        .route(
            "/v1/traces/json",
            post(handlers::ingest::ingest_traces_json),
        )
        // Prometheus API v1
        .route("/api/v1/query", get(handlers::prometheus::query_instant))
        .route(
            "/api/v1/query_range",
            get(handlers::prometheus::query_range),
        )
        .route("/api/v1/logs", get(handlers::prometheus::query_logs))
        .route(
            "/api/v1/label/__name__/values",
            get(handlers::prometheus::list_metric_names),
        )
        .route(
            "/api/v1/label/:name/values",
            get(handlers::prometheus::list_label_values),
        )
        .route(
            "/api/v1/labels",
            get(handlers::prometheus::list_label_names),
        )
        // Log API
        .route(
            "/v1/logs/count",
            get(handlers::prometheus::query_logs_count),
        )
        .route(
            "/v1/logs/fields",
            get(handlers::prometheus::list_log_fields),
        )
        .route(
            "/v1/logs/field_values",
            get(handlers::prometheus::list_log_field_values),
        )
        // Correlation & Traces
        .route("/v1/correlate", get(handlers::prometheus::correlate))
        .route(
            "/v1/traces/search",
            get(handlers::prometheus::search_traces),
        )
        // Grafana SimpleJSON
        .route("/search", post(handlers::simplejson::search))
        .route("/query", post(handlers::simplejson::query))
        .route("/annotations", post(handlers::simplejson::annotations))
        .route("/tag-keys", post(handlers::simplejson::tag_keys))
        .route("/tag-values", post(handlers::simplejson::tag_values))
        // Alert Engine API (basic)
        .route("/api/v1/alerts", get(handlers::alerts::list_alerts))
        .route("/api/v1/alerts/:id", get(handlers::alerts::get_alert))
        .route(
            "/api/v1/alerts/:id/acknowledge",
            post(handlers::alerts::acknowledge_alert),
        )
        .route(
            "/api/v1/alerts/routes",
            get(handlers::alerts::list_routes).post(handlers::alerts::set_routes),
        )
        .route(
            "/api/v1/alerts/silences",
            get(handlers::alerts::list_silences).post(handlers::alerts::create_silence),
        )
        .route(
            "/api/v1/alerts/silences/:name",
            delete(handlers::alerts::delete_silence),
        )
        .route(
            "/api/v1/alerts/:id/resolve",
            post(handlers::alerts::resolve_alert),
        )
        .route("/api/v1/rules", get(handlers::alerts::list_rules))
        .route("/api/v1/rules", post(handlers::alerts::create_rule))
        .route("/api/v1/rules/:id", put(handlers::alerts::update_rule))
        .route("/api/v1/rules/:id", delete(handlers::alerts::delete_rule))
        // Pipeline & Recording Rules API
        .route(
            "/api/v1/recording_rules",
            get(handlers::pipeline::list_recording_rules),
        )
        .route(
            "/api/v1/recording_rules",
            post(handlers::pipeline::create_recording_rule),
        )
        .route(
            "/api/v1/recording_rules/:name",
            delete(handlers::pipeline::delete_recording_rule),
        )
        .route("/api/v1/pipelines", get(handlers::pipeline::list_pipelines))
        .route(
            "/api/v1/pipelines",
            post(handlers::pipeline::create_pipeline),
        )
        .route(
            "/api/v1/pipelines/:name",
            delete(handlers::pipeline::delete_pipeline),
        );

    routes
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .layer(TimeoutLayer::new(Duration::from_secs(
            query_config.timeout_secs,
        )))
        .layer(RequestBodyLimitLayer::new(ingest_config.max_body_size))
        .with_state(state)
}
