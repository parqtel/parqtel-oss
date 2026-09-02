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
use parqtel_core::{BlockIndex, Config};
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

    let ui_html = "<html><body>Test</body></html>";
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(ui_html.as_bytes()).unwrap();
    let ui_content = encoder.finish().unwrap();

    let mut hasher = Sha256::new();
    hasher.update(ui_html.as_bytes());
    let ui_etag = format!("\"{}\"", hex::encode(hasher.finalize()));

    let state = AppState::new(
        IngestionService::new(config.storage.clone(), tx),
        LogIngestionService::new(config.logs.clone(), ltx),
        TraceIngestionService::new(config.storage.clone(), ttx),
        QueryExecutor::new(
            index.clone(),
            log_index,
            config.storage.data_dir.join("traces"),
        ),
        index,
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
    let mut config = Config::default();
    config.storage.data_dir = dir.path().to_path_buf();
    let index = Arc::new(tokio::sync::RwLock::new(BlockIndex::new(dir.path())));
    let output = dir.path().join("export.csv");

    super::run_export(
        config,
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

#[tokio::test]
async fn test_grpc_trace_export_and_immediate_query() {
    use parqtel_ingest::otel::collector::trace::v1::trace_service_client::TraceServiceClient;
    use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
    use parqtel_ingest::otel::trace::v1::Span as OtelSpan;

    let state = AppState::default_for_tests().await;

    // Bind gRPC on an ephemeral port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let grpc_state = state.clone();
    tokio::spawn(async move {
        let _ = crate::grpc::OtlpGrpcService::serve(grpc_state, addr).await;
    });
    // Give the server a moment to bind
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    let mut client = TraceServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("gRPC connect");

    let span = OtelSpan {
        trace_id: vec![0x42; 16],
        span_id: vec![0x24; 8],
        name: "grpc-test-op".into(),
        kind: 2,
        start_time_unix_nano: 5_000,
        end_time_unix_nano: 6_000,
        ..Default::default()
    };
    let request = ExportTraceServiceRequest {
        resource_spans: vec![parqtel_ingest::otel::trace::v1::ResourceSpans {
            scope_spans: vec![parqtel_ingest::otel::trace::v1::ScopeSpans {
                spans: vec![span],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    client.export(request).await.expect("gRPC export ok");

    // Spans must be queryable IMMEDIATELY via the memory buffer (F2),
    // long before any Parquet block flush.
    let spans = state
        .inner
        .query_executor
        .query_traces(0, i64::MAX, None, 10)
        .await
        .expect("query traces");
    assert_eq!(spans.len(), 1, "span should be queryable from buffer");
    assert_eq!(spans[0].name, "grpc-test-op");
}

#[tokio::test]
async fn test_grpc_metrics_and_logs_export() {
    use parqtel_ingest::otel::collector::logs::v1::logs_service_client::LogsServiceClient;
    use parqtel_ingest::otel::collector::logs::v1::ExportLogsServiceRequest;
    use parqtel_ingest::otel::collector::metrics::v1::metrics_service_client::MetricsServiceClient;
    use parqtel_ingest::otel::collector::metrics::v1::ExportMetricsServiceRequest;
    use parqtel_ingest::otel::logs::v1::LogRecord;
    use parqtel_ingest::otel::metrics::v1::{Gauge, Metric, NumberDataPoint};
    use parqtel_ingest::otel::trace::v1::ScopeSpans;

    let state = AppState::default_for_tests().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let grpc_state = state.clone();
    tokio::spawn(async move {
        let _ = crate::grpc::OtlpGrpcService::serve(grpc_state, addr).await;
    });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Metrics
    let mut mclient = MetricsServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("metrics connect");
    let metric = Metric {
        name: "grpc_requests".into(),
        data: Some(parqtel_ingest::otel::metrics::v1::metric::Data::Gauge(
            Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 42,
                    value: Some(
                        parqtel_ingest::otel::metrics::v1::number_data_point::Value::AsDouble(7.0),
                    ),
                    ..Default::default()
                }],
            },
        )),
        ..Default::default()
    };
    mclient
        .export(ExportMetricsServiceRequest {
            resource_metrics: vec![parqtel_ingest::otel::metrics::v1::ResourceMetrics {
                scope_metrics: vec![parqtel_ingest::otel::metrics::v1::ScopeMetrics {
                    metrics: vec![metric],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
        .await
        .expect("metrics export");

    // Logs — reusing ScopeSpans import path shape
    let mut lclient = LogsServiceClient::connect(format!("http://{addr}"))
        .await
        .expect("logs connect");
    let record = LogRecord {
        time_unix_nano: 42,
        body: Some(parqtel_ingest::otel::common::v1::AnyValue {
            value: Some(
                parqtel_ingest::otel::common::v1::any_value::Value::StringValue("grpc log".into()),
            ),
        }),
        ..Default::default()
    };
    let _ = ScopeSpans::default(); // import sanity
    lclient
        .export(ExportLogsServiceRequest {
            resource_logs: vec![parqtel_ingest::otel::logs::v1::ResourceLogs {
                scope_logs: vec![parqtel_ingest::otel::logs::v1::ScopeLogs {
                    log_records: vec![record],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        })
        .await
        .expect("logs export");

    // Buffered metrics should be queryable immediately
    let points = state
        .inner
        .query_executor
        .memory_buffer()
        .scan_metrics("grpc_requests", 0, i64::MAX)
        .await;
    assert_eq!(points.len(), 1);
}

#[tokio::test]
async fn test_span_metrics_red_bridge_end_to_end() {
    use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
    use parqtel_ingest::otel::trace::v1::Span as OtelSpan;
    use tokio::sync::mpsc;

    let state = AppState::default_for_tests().await;

    // The test-state trace service has no span-metrics sink wired (it's built
    // by main.rs in production), so drive the bridge directly through the
    // public channel surface the same way main.rs does.
    let (span_metrics_tx, mut rx) = mpsc::unbounded_channel();
    // Rebuild a trace service with the sink to exercise process_traces.
    let (ttx, _trx) = mpsc::unbounded_channel();
    let config = parqtel_core::Config::default();
    let buffer = state.inner.query_executor.memory_buffer();
    let trace_service = TraceIngestionService::new(config.storage.clone(), ttx)
        .with_memory_buffer(buffer)
        .with_span_metrics(span_metrics_tx);

    // One successful + one failed server span across two services.
    let mk_span = |name: &str, status: i32| OtelSpan {
        trace_id: vec![0x99; 16],
        span_id: vec![0x11; 8],
        name: name.into(),
        kind: 2, // SERVER
        start_time_unix_nano: 10_000,
        end_time_unix_nano: 20_000,
        status: Some(parqtel_ingest::otel::trace::v1::Status {
            code: status,
            message: String::new(),
        }),
        ..Default::default()
    };
    let request = ExportTraceServiceRequest {
        resource_spans: vec![parqtel_ingest::otel::trace::v1::ResourceSpans {
            resource: Some(parqtel_ingest::otel::resource::v1::Resource {
                attributes: vec![parqtel_ingest::otel::common::v1::KeyValue {
                    key: "service.name".into(),
                    value: Some(parqtel_ingest::otel::common::v1::AnyValue {
                        value: Some(
                            parqtel_ingest::otel::common::v1::any_value::Value::StringValue(
                                "api".into(),
                            ),
                        ),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![parqtel_ingest::otel::trace::v1::ScopeSpans {
                spans: vec![mk_span("GET /ok", 0), mk_span("GET /fail", 2)],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };

    // Encode + decode through the proto path (mirrors gRPC handling).
    let body = prost::Message::encode_to_vec(&request);
    trace_service
        .ingest_proto(bytes::Bytes::from(body))
        .await
        .expect("trace ingest");

    // The RED bridge must have emitted metrics on the channel.
    let derived = rx.recv().await.expect("span metrics emitted");
    let names: Vec<&str> = derived.iter().map(|m| m.name.as_str()).collect();
    assert!(names.contains(&"traces_service_requests_total"));
    assert!(names.contains(&"traces_service_errors_total"));
    assert!(names.contains(&"traces_service_duration_ms"));

    // Ingest them and verify queryability via the buffer.
    state
        .inner
        .ingestion_service
        .ingest_metrics(derived)
        .await
        .expect("metric ingest");
    let pts = state
        .inner
        .query_executor
        .memory_buffer()
        .scan_metrics("traces_service_errors_total", 0, i64::MAX)
        .await;
    assert_eq!(pts.len(), 2);
    let svc_label = pts[0]
        .labels
        .get("service.name")
        .expect("service.name label");
    assert_eq!(svc_label, "api");
}

#[tokio::test]
async fn test_tail_sampling_keeps_red_full_fidelity() {
    use parqtel_core::config::TailSamplingConfig;
    use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
    use parqtel_ingest::otel::trace::v1::{ResourceSpans, ScopeSpans, Span as OtelSpan, Status};
    use tokio::sync::mpsc;

    // Drop ALL traces (ratio 0, keep_errors off) — the RED bridge must
    // still emit metrics from the full span set.
    let policy = TailSamplingConfig {
        keep_errors: false,
        slow_trace_ms: None,
        sampling_ratio: 0.0,
        per_service: std::collections::HashMap::new(),
    };
    let (span_metrics_tx, mut rx) = mpsc::unbounded_channel();
    let (ttx, _trx) = mpsc::unbounded_channel();
    let config = parqtel_core::Config::default();
    let trace_service = TraceIngestionService::new(config.storage.clone(), ttx)
        .with_span_metrics(span_metrics_tx)
        .with_tail_sampling(policy);

    let mk = |id: u8, status: i32| OtelSpan {
        trace_id: vec![id; 16],
        span_id: vec![id; 8],
        name: format!("GET /{id}"),
        kind: 2,
        start_time_unix_nano: 1_000,
        end_time_unix_nano: 2_000,
        status: Some(Status {
            code: status,
            message: String::new(),
        }),
        ..Default::default()
    };
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(parqtel_ingest::otel::resource::v1::Resource {
                attributes: vec![parqtel_ingest::otel::common::v1::KeyValue {
                    key: "service.name".into(),
                    value: Some(parqtel_ingest::otel::common::v1::AnyValue {
                        value: Some(
                            parqtel_ingest::otel::common::v1::any_value::Value::StringValue(
                                "sampled-svc".into(),
                            ),
                        ),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![mk(1, 0), mk(2, 2)],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body = prost::Message::encode_to_vec(&request);
    trace_service
        .ingest_proto(bytes::Bytes::from(body))
        .await
        .expect("ingest");

    // RED metrics MUST be emitted despite 100% trace dropping.
    let derived = rx.recv().await.expect("RED metrics from full span set");
    let reqs = derived
        .iter()
        .find(|m| m.name == "traces_service_requests_total")
        .expect("requests metric");
    assert_eq!(reqs.data_points.len(), 2, "RED sees ALL spans, unsampled");
}

#[tokio::test]
async fn test_tail_sampling_drops_from_storage() {
    use parqtel_core::config::TailSamplingConfig;
    use parqtel_core::MemoryBuffer;
    use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
    use parqtel_ingest::otel::trace::v1::{ResourceSpans, ScopeSpans, Span as OtelSpan, Status};

    // 100% drop policy.
    let policy = TailSamplingConfig {
        keep_errors: false,
        slow_trace_ms: None,
        sampling_ratio: 0.0,
        per_service: std::collections::HashMap::new(),
    };
    let (ttx, _trx) = mpsc::unbounded_channel();
    let config = parqtel_core::Config::default();
    let buffer = MemoryBuffer::new();
    let svc = TraceIngestionService::new(config.storage.clone(), ttx)
        .with_memory_buffer(buffer.clone())
        .with_tail_sampling(policy);

    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(parqtel_ingest::otel::resource::v1::Resource {
                attributes: vec![parqtel_ingest::otel::common::v1::KeyValue {
                    key: "service.name".into(),
                    value: Some(parqtel_ingest::otel::common::v1::AnyValue {
                        value: Some(
                            parqtel_ingest::otel::common::v1::any_value::Value::StringValue(
                                "dropped-svc".into(),
                            ),
                        ),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![OtelSpan {
                    trace_id: vec![5; 16],
                    span_id: vec![5; 8],
                    name: "GET /x".into(),
                    kind: 2,
                    start_time_unix_nano: 1_000,
                    end_time_unix_nano: 2_000,
                    status: Some(Status {
                        code: 0,
                        message: String::new(),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body = prost::Message::encode_to_vec(&request);
    let count = svc
        .ingest_proto(bytes::Bytes::from(body))
        .await
        .expect("ingest");
    assert_eq!(count, 1, "ingest count reflects received spans");

    // Buffer must be EMPTY — all spans dropped by sampling.
    let spans = buffer.scan_spans(0, i64::MAX).await;
    assert!(
        spans.is_empty(),
        "sampled-out spans must not be stored/buffered"
    );

    // Error trace survives even under ratio 0 when keep_errors is on.
    let (ttx2, _trx2) = mpsc::unbounded_channel();
    let keep_errors_policy = TailSamplingConfig {
        keep_errors: true,
        ..Default::default()
    };
    let buffer2 = MemoryBuffer::new();
    let svc2 = TraceIngestionService::new(config.storage.clone(), ttx2)
        .with_memory_buffer(buffer2.clone())
        .with_tail_sampling(keep_errors_policy);
    let err_request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(parqtel_ingest::otel::resource::v1::Resource {
                attributes: vec![parqtel_ingest::otel::common::v1::KeyValue {
                    key: "service.name".into(),
                    value: Some(parqtel_ingest::otel::common::v1::AnyValue {
                        value: Some(
                            parqtel_ingest::otel::common::v1::any_value::Value::StringValue(
                                "err-svc".into(),
                            ),
                        ),
                    }),
                }],
                ..Default::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans: vec![OtelSpan {
                    trace_id: vec![6; 16],
                    span_id: vec![6; 8],
                    name: "GET /err".into(),
                    kind: 2,
                    start_time_unix_nano: 1_000,
                    end_time_unix_nano: 2_000,
                    status: Some(Status {
                        code: 2,
                        message: "boom".into(),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let body2 = prost::Message::encode_to_vec(&err_request);
    svc2.ingest_proto(bytes::Bytes::from(body2))
        .await
        .expect("ingest");
    let kept = buffer2.scan_spans(0, i64::MAX).await;
    assert_eq!(kept.len(), 1, "error traces survive sampling");
}

#[tokio::test]
async fn test_ast_composed_queries_end_to_end() {
    let state = AppState::default_for_tests().await;

    // Seed: two services, counters rising 1/sec, 10s interval, 100s.
    let now_ns = 1_788_300_000_000_000_000i64;
    let mut metrics = Vec::new();
    for (svc, mult) in [("a", 1.0), ("b", 3.0)] {
        for i in 0..11 {
            metrics.push(parqtel_core::Metric {
                name: "ast_requests".into(),
                description: String::new(),
                unit: String::new(),
                kind: parqtel_core::MetricKind::Sum,
                resource_attributes: parqtel_core::LabelSet::default(),
                data_points: vec![parqtel_core::DataPoint {
                    timestamp_ns: now_ns + i * 10_000_000_000,
                    value: parqtel_core::MetricValue::Double(i as f64 * mult * 10.0),
                    labels: parqtel_core::LabelSet::try_from_iter(vec![(
                        "service".to_string(),
                        svc.to_string(),
                    )])
                    .unwrap(),
                }],
            });
        }
    }
    state
        .inner
        .ingestion_service
        .ingest_metrics(metrics)
        .await
        .unwrap();

    let t_end = now_ns + 100_000_000_000;

    // 1. THE canonical RED pattern — was impossible before Phase 1A.
    let expr = parqtel_query::parser::parse_expr("sum(rate(ast_requests[1m]))").unwrap();
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, t_end - 30_000_000_000, t_end, Some(15_000_000_000))
        .await
        .unwrap();
    assert_eq!(r.series.len(), 1, "aggregated to one series");
    let last = r.series[0].samples.last().unwrap();
    // combined rate = 1 + 3 = 4/sec (± extrapolation slack)
    assert!(
        (last.value - 4.0).abs() < 0.8,
        "sum(rate) ≈ 4.0, got {}",
        last.value
    );

    // 2. Grouped aggregation with a nested range fn.
    let expr =
        parqtel_query::parser::parse_expr("sum by (service) (rate(ast_requests[1m]))").unwrap();
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, t_end - 30_000_000_000, t_end, Some(15_000_000_000))
        .await
        .unwrap();
    assert_eq!(r.series.len(), 2, "one series per service");

    // 3. Binary ratio.
    let expr = parqtel_query::parser::parse_expr(
        r#"sum(ast_requests{service="a"}) / sum(ast_requests{service="b"})"#,
    )
    .unwrap();
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, t_end, t_end + 1, None)
        .await
        .unwrap();
    assert_eq!(r.series.len(), 1);
    let v = r.series[0].samples[0].value;
    assert!((v - 100.0 / 300.0).abs() < 1e-6, "a/b = 1/3, got {v}");

    // 4. avg_over_time family.
    let expr = parqtel_query::parser::parse_expr("avg_over_time(ast_requests[1m])").unwrap();
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, t_end - 1, t_end, None)
        .await
        .unwrap();
    assert_eq!(r.series.len(), 2);

    // 5. Comparison with bool.
    let expr = parqtel_query::parser::parse_expr("ast_requests > bool 15").unwrap();
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, t_end, t_end + 1, None)
        .await
        .unwrap();
    assert!(r
        .series
        .iter()
        .all(|s| s.samples[0].value == 0.0 || s.samples[0].value == 1.0));

    // 6. Vector matching with on().
    let expr = parqtel_query::parser::parse_expr(
        r#"sum(ast_requests{service="a"}) * on() sum(ast_requests{service="b"})"#,
    )
    .unwrap();
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, t_end, t_end + 1, None)
        .await
        .unwrap();
    assert_eq!(r.series.len(), 1);
    let v = r.series[0].samples[0].value;
    assert!((v - 100.0 * 300.0).abs() < 1e-6, "product 30000, got {v}");
}

#[tokio::test]
async fn test_ast_avg_over_time_range_bug() {
    let state = AppState::default_for_tests().await;
    let base = 1_788_400_000_000_000_000i64;
    let mut metrics = Vec::new();
    for i in 0..10 {
        metrics.push(parqtel_core::Metric {
            name: "gauge_x".into(),
            description: String::new(),
            unit: String::new(),
            kind: parqtel_core::MetricKind::Gauge,
            resource_attributes: parqtel_core::LabelSet::default(),
            data_points: vec![parqtel_core::DataPoint {
                timestamp_ns: base + i * 10_000_000_000,
                value: parqtel_core::MetricValue::Double(i as f64),
                labels: parqtel_core::LabelSet::try_from_iter(vec![("service".to_string(), "s1".to_string())]).unwrap(),
            }],
        });
    }
    state.inner.ingestion_service.ingest_metrics(metrics).await.unwrap();

    let expr = parqtel_query::parser::parse_expr("avg_over_time(gauge_x[1m])").unwrap();
    let end = base + 100_000_000_000;
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, end - 60_000_000_000, end, Some(30_000_000_000))
        .await
        .unwrap();
    assert!(
        !r.series.is_empty(),
        "avg_over_time must return series; got {} (samples {:?})",
        r.series.len(),
        r.series.first().map(|s| &s.samples)
    );
}

#[tokio::test]
async fn test_ast_avg_over_time_block_backed() {
    let state = AppState::default_for_tests().await;
    let base = 1_788_400_000_000_000_000i64;
    let mut metrics = Vec::new();
    for i in 0..10 {
        metrics.push(parqtel_core::Metric {
            name: "gauge_y".into(),
            description: String::new(),
            unit: String::new(),
            kind: parqtel_core::MetricKind::Gauge,
            resource_attributes: parqtel_core::LabelSet::default(),
            data_points: vec![parqtel_core::DataPoint {
                timestamp_ns: base + i * 10_000_000_000,
                value: parqtel_core::MetricValue::Double(i as f64),
                labels: parqtel_core::LabelSet::try_from_iter(vec![("service".to_string(), "s1".to_string())]).unwrap(),
            }],
        });
    }
    state.inner.ingestion_service.ingest_metrics(metrics).await.unwrap();
    // Force a block flush so the data lives on disk, not the buffer.
    state.inner.ingestion_service.shutdown().await.unwrap();
    // Give the async index-update task a beat to register the new block.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let expr = parqtel_query::parser::parse_expr("avg_over_time(gauge_y[1m])").unwrap();
    let end = base + 100_000_000_000;
    let r = state
        .inner
        .query_executor
        .execute_ast(&expr, end - 60_000_000_000, end, Some(30_000_000_000))
        .await
        .unwrap();
    assert!(
        !r.series.is_empty(),
        "block-backed avg_over_time must return series; got {}",
        r.series.len()
    );
}
