#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use crate::router::build_router;
use crate::state::AppState;
#[cfg(test)]
use axum::{
    body::{to_bytes, Body},
    http::{header, Request, StatusCode},
};
use flate2::write::GzEncoder;
use flate2::Compression;
use parqtel_core::{BlockIndex, Config, StorageEngine};
use parqtel_ingest::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use parqtel_ingest::{IngestionService, LogIngestionService, TraceIngestionService};
use parqtel_query::QueryExecutor;
use prost::Message;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;
use tower::util::ServiceExt;

#[cfg(test)]
async fn setup_test_app() -> axum::Router {
    let dir = tempfile::tempdir().unwrap();
    let config = Config::default();
    let (tx, _) = mpsc::unbounded_channel();
    let (ltx, _) = mpsc::unbounded_channel();
    let (ttx, _) = mpsc::unbounded_channel();
    let index = Arc::new(tokio::sync::RwLock::new(BlockIndex::new(dir.path())));
    let log_index = Arc::new(tokio::sync::RwLock::new(BlockIndex::new(
        &dir.path().join("logs"),
    )));

    let storage_engine: Arc<dyn StorageEngine> = Arc::new(
        parqtel_core::engine::parquet::ParquetStorageEngine::new(config.storage.clone()),
    );

    let ui_html = "<html><body>Test</body></html>";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(ui_html.as_bytes()).unwrap();
    let ui_content = encoder.finish().unwrap();

    let mut hasher = Sha256::new();
    hasher.update(ui_html.as_bytes());
    let ui_etag = format!("\"{}\"", hex::encode(hasher.finalize()));

    let memory_buffer = parqtel_core::MemoryBuffer::new();
    let state = AppState::new(
        storage_engine,
        IngestionService::new(config.storage.clone(), tx),
        LogIngestionService::new(config.logs.clone(), ltx),
        TraceIngestionService::new(config.storage.clone(), ttx),
        QueryExecutor::new(index.clone(), log_index),
        index,
        memory_buffer,
        config,
        ui_content,
        ui_etag,
    )
    .await;

    build_router(state)
}

#[tokio::test]
async fn test_ui_redirect() {
    let app = setup_test_app().await;
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers().get("location").unwrap(), "/ui");
}

#[tokio::test]
async fn test_ui_headers() {
    let app = setup_test_app().await;
    let response = app
        .oneshot(Request::builder().uri("/ui").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(response
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("text/html"));
    assert_eq!(response.headers().get("content-encoding").unwrap(), "gzip");
    assert!(response.headers().contains_key("etag"));
}

#[tokio::test]
async fn test_ui_caching() {
    let app = setup_test_app().await;
    let response1 = app
        .clone()
        .oneshot(Request::builder().uri("/ui").body(Body::empty()).unwrap())
        .await
        .unwrap();

    let etag = response1.headers().get("etag").unwrap();

    let response2 = app
        .oneshot(
            Request::builder()
                .uri("/ui")
                .header("if-none-match", etag)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response2.status(), StatusCode::NOT_MODIFIED);
}

#[tokio::test]
async fn test_metrics_endpoint() {
    let app = setup_test_app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = to_bytes(response.into_body(), 10240).await.unwrap();
    let body_str = String::from_utf8(body.to_vec()).unwrap();
    assert!(body_str.contains("parqtel_storage_blocks"));
}

#[tokio::test]
async fn test_prometheus_query_range_missing_params() {
    let app = setup_test_app().await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = to_bytes(response.into_body(), 1024).await.unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "error");
}

#[tokio::test]
async fn test_config_validation() {
    let mut config = Config::default();
    config.server.bind_address = "".into();
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn test_ingest_empty_body() {
    let app = setup_test_app().await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_ingest_metrics_json() {
    let app = setup_test_app().await;
    let payload = serde_json::json!({
        "resourceMetrics": [{"scopeMetrics": [{"metrics": [{"name": "test", "gauge": {"dataPoints": [{"timeUnixNano": 1000, "asDouble": 1.0}]}}]}]}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/metrics/json")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ingest_logs_json() {
    let app = setup_test_app().await;
    let payload = serde_json::json!({
        "resourceLogs": [{"scopeLogs": [{"logRecords": [{"timeUnixNano": 1000, "body": "test"}]}]}]
    });
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs/json")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(payload.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_prometheus_list_handlers() {
    let app = setup_test_app().await;

    let req = Request::builder()
        .uri("/api/v1/labels")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .uri("/api/v1/label/__name__/values")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .uri("/api/v1/label/host/values")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .uri("/v1/logs/fields")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_correlate_handler() {
    let app = setup_test_app().await;
    let labels = serde_json::json!({"service_name": "test"}).to_string();
    let uri = format!("/v1/correlate?anchor_signal=metric&anchor_timestamp_ns=1000&anchor_labels={}&target_signal=log", 
                      urlencoding::encode(&labels));

    let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_query_instant() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/api/v1/query?query=up")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_query_logs() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/api/v1/logs?query=m&start=0&end=100")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_search_traces() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/v1/traces/search?trace_id=123")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_inspect_logic() {
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(tokio::sync::RwLock::new(BlockIndex::new(dir.path())));
    let log_index = Arc::new(tokio::sync::RwLock::new(BlockIndex::new(
        &dir.path().join("logs"),
    )));

    super::run_inspect(index, log_index).await.unwrap();
}

#[test]
fn test_v_to_f64() {
    use parqtel_core::MetricValue;
    assert_eq!(super::v_to_f64(&MetricValue::Double(1.0)), 1.0);
    assert_eq!(super::v_to_f64(&MetricValue::Int(42)), 42.0);
}

#[tokio::test]
async fn test_prometheus_query_range() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/api/v1/query_range?query=cpu&start=0&end=100&step=10s")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_prometheus_invalid_step() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/api/v1/query_range?query=cpu&start=0&end=100&step=invalid")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_query_instant_missing_query() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/api/v1/query")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_list_log_field_values() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/v1/logs/field_values?field_name=host")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_query_logs_count() {
    let app = setup_test_app().await;
    let req = Request::builder()
        .uri("/v1/logs/count?query=m&start=0&end=100")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_ingest_metrics_proto() {
    let app = setup_test_app().await;
    use parqtel_ingest::otel::metrics::v1::metric::Data;
    use parqtel_ingest::otel::metrics::v1::number_data_point::Value;
    use parqtel_ingest::otel::metrics::v1::{
        Gauge, Metric as ProtoMetric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
    };

    let req_proto = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![ProtoMetric {
                    name: "test".into(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: 1000,
                            value: Some(Value::AsDouble(1.0)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut body = Vec::new();
    req_proto.encode(&mut body).unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/metrics")
                .header(header::CONTENT_TYPE, "application/x-protobuf")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_export_logic() {
    let dir = tempfile::tempdir().unwrap();
    let index = Arc::new(tokio::sync::RwLock::new(BlockIndex::new(dir.path())));
    let output = dir.path().join("export.csv");

    super::run_export(
        Config::default(),
        index,
        "cpu".into(),
        "2023-01-01T00:00:00Z".into(),
        "2023-01-01T01:00:00Z".into(),
        output.clone(),
    )
    .await
    .unwrap();

    assert!(output.exists());
}

#[tokio::test]
async fn test_simplejson_handlers() {
    let app = setup_test_app().await;

    let req = Request::builder()
        .method("POST")
        .uri("/search")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let payload = serde_json::json!({
        "range": {"from": "2023-01-01T00:00:00Z", "to": "2023-01-01T01:00:00Z"},
        "interval_ms": 1000,
        "targets": [{"target": "cpu"}],
        "max_data_points": 100
    });
    let req = Request::builder()
        .method("POST")
        .uri("/query")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(payload.to_string()))
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .method("POST")
        .uri("/tag-keys")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .method("POST")
        .uri("/tag-values")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);

    let req = Request::builder()
        .method("POST")
        .uri("/annotations")
        .body(Body::empty())
        .unwrap();
    let res = app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::OK);
}
