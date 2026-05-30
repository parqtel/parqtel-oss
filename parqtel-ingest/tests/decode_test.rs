use parqtel_ingest::decode::OtlpDecoder;
use parqtel_core::{MetricKind, MetricValue, Span, SpanEvent, SpanLink, SpanStatus};
use serde_json::json;

#[test]
fn test_decode_metrics_json_gauge() {
    let payload = json!({
        "resourceMetrics": [{
            "resource": {
                "attributes": [{"key": "service.name", "value": {"stringValue": "test-svc"}}]
            },
            "scopeMetrics": [{
                "metrics": [{
                    "name": "http_requests",
                    "gauge": {
                        "dataPoints": [{
                            "timeUnixNano": "1600000000000000000",
                            "asDouble": 42.5,
                            "attributes": [{"key": "method", "value": {"stringValue": "GET"}}]
                        }]
                    }
                }]
            }]
        }]
    });

    let metrics = OtlpDecoder::decode_metrics_json(payload).unwrap();
    assert_eq!(metrics.len(), 1);
    let m = &metrics[0];
    assert_eq!(m.name, "http_requests");
    assert_eq!(m.kind, MetricKind::Gauge);
    assert_eq!(m.resource_attributes.get("service.name"), Some("test-svc"));
    assert_eq!(m.data_points.len(), 1);
    let dp = &m.data_points[0];
    assert_eq!(dp.timestamp_ns, 1600000000000000000);
    assert_eq!(dp.labels.get("method"), Some("GET"));
    if let MetricValue::Double(v) = dp.value {
        assert_eq!(v, 42.5);
    } else {
        panic!("Expected double value");
    }
}

#[test]
fn test_decode_metrics_json_sum() {
    let payload = json!({
        "resourceMetrics": [{
            "scopeMetrics": [{
                "metrics": [{
                    "name": "error_count",
                    "sum": {
                        "dataPoints": [{
                            "timeUnixNano": 1600000000000000000i64,
                            "asInt": "100"
                        }]
                    }
                }]
            }]
        }]
    });

    let metrics = OtlpDecoder::decode_metrics_json(payload).unwrap();
    assert_eq!(metrics.len(), 1);
    let m = &metrics[0];
    assert_eq!(m.kind, MetricKind::Sum);
    let dp = &m.data_points[0];
    if let MetricValue::Int(v) = dp.value {
        assert_eq!(v, 100);
    } else {
        panic!("Expected int value");
    }
}

#[test]
fn test_decode_logs_json() {
    let payload = json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [{"key": "host", "value": {"stringValue": "localhost"}}]
            },
            "scopeLogs": [{
                "scope": {"name": "test-scope", "version": "1.0"},
                "logRecords": [{
                    "timeUnixNano": "1600000000000000000",
                    "severityNumber": 9,
                    "severityText": "INFO",
                    "body": {"stringValue": "log message"},
                    "attributes": [{"key": "app", "value": {"stringValue": "web"}}]
                }]
            }]
        }]
    });

    let logs = OtlpDecoder::decode_logs_json(payload).unwrap();
    assert_eq!(logs.len(), 1);
    let l = &logs[0];
    assert_eq!(l.body, "log message");
    assert_eq!(l.severity_text, "INFO");
    assert_eq!(l.scope_name, "test-scope");
    assert_eq!(l.resource_attributes.get("host"), Some("localhost"));
    assert_eq!(l.attributes.get("app"), Some("web"));
}

#[test]
fn test_decode_metrics_json_histogram() {
    let payload = json!({
        "resourceMetrics": [{
            "scopeMetrics": [{
                "metrics": [{
                    "name": "latency",
                    "histogram": {
                        "dataPoints": [{
                            "timeUnixNano": "1600000000000000000",
                            "count": "10",
                            "sum": 50.0,
                            "bucketCounts": ["1", "2", "3", "4"],
                            "explicitBounds": [1.0, 2.0, 5.0]
                        }]
                    }
                }]
            }]
        }]
    });
    
    // This currently just tests the JSON skeleton; 
    // real histograms in OTLP JSON usually have values inside.
    let _metrics = OtlpDecoder::decode_metrics_json(payload).unwrap();
}

#[test]
fn test_decode_metrics_proto() {
    use parqtel_ingest::otel::collector::metrics::v1::ExportMetricsServiceRequest;
    use parqtel_ingest::otel::metrics::v1::{ResourceMetrics, ScopeMetrics, Metric as ProtoMetric, Gauge, NumberDataPoint};
    use parqtel_ingest::otel::metrics::v1::metric::Data;
    use parqtel_ingest::otel::metrics::v1::number_data_point::Value;

    let req = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                scope: None,
                metrics: vec![ProtoMetric {
                    name: "test_gauge".into(),
                    description: "".into(),
                    unit: "".into(),
                    data: Some(Data::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            attributes: vec![],
                            time_unix_nano: 1000,
                            start_time_unix_nano: 0,
                            value: Some(Value::AsDouble(123.45)),
                            exemplars: vec![],
                            flags: 0,
                        }],
                    })),
                }],
                schema_url: "".into(),
            }],
            schema_url: "".into(),
        }],
    };

    let metrics = OtlpDecoder::decode_metrics(req).unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "test_gauge");
    if let MetricValue::Double(v) = metrics[0].data_points[0].value {
        assert_eq!(v, 123.45);
    }
}


#[test]
fn test_decode_invalid_json() {
    let payload = serde_json::json!({"resourceMetrics": [{"scopeMetrics": [{"metrics": [{"name": "bad", "gauge": {"dataPoints": [{"timeUnixNano": "not-a-number"}]}}]}]}]});
    let res = OtlpDecoder::decode_metrics_json(payload);
    assert!(res.is_err());
}

#[test]
fn test_decode_missing_fields() {
    let payload = serde_json::json!({"resourceMetrics": [{}]});
    let res = OtlpDecoder::decode_metrics_json(payload);
    assert!(res.is_ok()); // Should handle missing fields gracefully
    assert!(res.unwrap().is_empty());
}

#[test]
fn test_decode_traces_proto() {
    use parqtel_ingest::otel::collector::trace::v1::ExportTraceServiceRequest;
    use parqtel_ingest::otel::trace::v1::{ResourceSpans, ScopeSpans, Span as ProtoSpan};
    use parqtel_ingest::otel::common::v1::KeyValue;

    let req = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: None,
            scope_spans: vec![ScopeSpans {
                scope: None,
                spans: vec![ProtoSpan {
                    trace_id: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
                    span_id: vec![1, 2, 3, 4, 5, 6, 7, 8],
                    parent_span_id: vec![],
                    name: "test-span".into(),
                    kind: 1,
                    start_time_unix_nano: 1000,
                    end_time_unix_nano: 2000,
                    attributes: vec![KeyValue { key: "env".into(), value: None }],
                    events: vec![],
                    links: vec![],
                    status: None,
                    trace_state: "".into(),
                    flags: 0,
                    dropped_attributes_count: 0,
                    dropped_events_count: 0,
                    dropped_links_count: 0,
                }],
                schema_url: "".into(),
            }],
            schema_url: "".into(),
        }],
    };

    let spans = OtlpDecoder::decode_traces(req).unwrap();
    assert_eq!(spans.len(), 1);
    let s = &spans[0];
    assert_eq!(s.name, "test-span");
    assert_eq!(s.kind, 1);
    assert_eq!(s.duration_ns(), 1000);
    assert_eq!(s.status.code, 0);
}

#[test]
fn test_decode_traces_json() {
    let payload = json!({
        "resource_spans": [{
            "resource": {
                "attributes": [{"key": "service.name", "value": {"stringValue": "test-svc"}}]
            },
            "scope_spans": [{
                "spans": [{
                    "trace_id": "0102030405060708090a0b0c0d0e0f10",
                    "span_id": "0102030405060708",
                    "name": "test-span",
                    "kind": 2,
                    "start_time_unix_nano": "1600000000000000000",
                    "end_time_unix_nano": "1600000000000001000",
                    "attributes": [{"key": "method", "value": {"stringValue": "GET"}}],
                    "status": {"code": 1, "message": "OK"}
                }]
            }]
        }]
    });

    let spans = OtlpDecoder::decode_traces_json(payload).unwrap();
    assert_eq!(spans.len(), 1);
    let s = &spans[0];
    assert_eq!(s.name, "test-span");
    assert_eq!(s.kind, 2);
    assert_eq!(s.duration_ns(), 1000);
    assert_eq!(s.status.code, 1);
    assert_eq!(s.status.message, "OK");
    assert_eq!(s.attributes.get("method"), Some("GET"));
}

#[test]
fn test_decode_traces_with_events_and_links() {
    let payload = json!({
        "resource_spans": [{
            "scope_spans": [{
                "spans": [{
                    "trace_id": "0102030405060708090a0b0c0d0e0f10",
                    "span_id": "0102030405060708",
                    "name": "span-with-events",
                    "kind": 1,
                    "start_time_unix_nano": "1600000000000000000",
                    "end_time_unix_nano": "1600000000000002000",
                    "events": [{
                        "time_unix_nano": "1600000000000000500",
                        "name": "event1",
                        "attributes": [{"key": "key1", "value": {"stringValue": "val1"}}]
                    }],
                    "links": [{
                        "trace_id": "1002030405060708090a0b0c0d0e0f10",
                        "span_id": "1002030405060708",
                        "attributes": [{"key": "link_key", "value": {"stringValue": "link_val"}}]
                    }]
                }]
            }]
        }]
    });

    let spans = OtlpDecoder::decode_traces_json(payload).unwrap();
    assert_eq!(spans.len(), 1);
    let s = &spans[0];
    assert_eq!(s.events.len(), 1);
    assert_eq!(s.events[0].name, "event1");
    assert_eq!(s.events[0].attributes.get("key1"), Some("val1"));
    assert_eq!(s.links.len(), 1);
    assert_eq!(s.links[0].attributes.get("link_key"), Some("link_val"));
}

#[test]
fn test_decode_traces_invalid_json() {
    let payload = json!({"resource_spans": [{"scope_spans": [{"spans": [{"trace_id": "invalid"}]}]}]});
    let res = OtlpDecoder::decode_traces_json(payload);
    assert!(res.is_err());
}
