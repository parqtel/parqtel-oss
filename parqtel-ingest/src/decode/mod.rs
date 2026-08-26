mod common;
mod logs;
mod metrics;
mod traces;

use crate::otel::collector::logs::v1::ExportLogsServiceRequest;
use crate::otel::collector::metrics::v1::ExportMetricsServiceRequest;
use crate::otel::collector::trace::v1::ExportTraceServiceRequest;
use parqtel_core::{LogRecord, Metric, Result, Span};

/// Decodes OTLP metric, log, and trace payloads into internal types.
pub struct OtlpDecoder;

impl OtlpDecoder {
    pub fn decode_metrics(request: ExportMetricsServiceRequest) -> Result<Vec<Metric>> {
        metrics::decode_metrics(request)
    }
    pub fn decode_metrics_json(json: serde_json::Value) -> Result<Vec<Metric>> {
        metrics::decode_metrics_json(json)
    }
    pub fn decode_logs(request: ExportLogsServiceRequest) -> Result<Vec<LogRecord>> {
        logs::decode_logs(request)
    }
    pub fn decode_logs_json(json: serde_json::Value) -> Result<Vec<LogRecord>> {
        logs::decode_logs_json(json)
    }
    pub fn decode_traces(request: ExportTraceServiceRequest) -> Result<Vec<Span>> {
        traces::decode_traces(request)
    }
    pub fn decode_traces_json(json: serde_json::Value) -> Result<Vec<Span>> {
        traces::decode_traces_json(json)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_decode_metrics_proto_gauge() {
        use crate::otel::metrics::v1::{
            number_data_point, Gauge, Metric as PM, NumberDataPoint, ResourceMetrics, ScopeMetrics,
        };
        let req = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![PM {
                        name: "cpu".into(),
                        description: "".into(),
                        unit: "".into(),
                        data: Some(crate::otel::metrics::v1::metric::Data::Gauge(Gauge {
                            data_points: vec![NumberDataPoint {
                                time_unix_nano: 1000,
                                value: Some(number_data_point::Value::AsDouble(42.5)),
                                attributes: vec![],
                                start_time_unix_nano: 0,
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
        assert_eq!(metrics[0].name, "cpu");
        assert_eq!(metrics[0].kind, parqtel_core::MetricKind::Gauge);
    }

    #[test]
    fn test_decode_logs_proto() {
        use crate::otel::logs::v1::{LogRecord as PLR, ResourceLogs, ScopeLogs};
        let req = ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource: None,
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records: vec![PLR {
                        time_unix_nano: 5000,
                        observed_time_unix_nano: 5001,
                        severity_number: 9,
                        severity_text: "INFO".into(),
                        body: Some(crate::otel::common::v1::AnyValue {
                            value: Some(crate::otel::common::v1::any_value::Value::StringValue(
                                "hello".into(),
                            )),
                        }),
                        attributes: vec![],
                        flags: 0,
                        trace_id: vec![0; 16],
                        span_id: vec![0; 8],
                        dropped_attributes_count: 0,
                    }],
                    schema_url: "".into(),
                }],
                schema_url: "".into(),
            }],
        };
        let logs = OtlpDecoder::decode_logs(req).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].body, "hello");
    }

    #[test]
    fn test_decode_traces_proto() {
        use crate::otel::trace::v1::{ResourceSpans, ScopeSpans, Span as PS};
        let req = ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![PS {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        parent_span_id: vec![],
                        name: "test-span".into(),
                        kind: 2,
                        start_time_unix_nano: 1000,
                        end_time_unix_nano: 2000,
                        attributes: vec![],
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
        assert_eq!(spans[0].name, "test-span");
    }

    #[test]
    fn test_decode_metrics_json_gauge() {
        let json = serde_json::json!({
            "resourceMetrics": [{
                "resource": {"attributes": []},
                "scopeMetrics": [{"metrics": [{
                    "name": "cpu",
                    "data": {"gauge": {"dataPoints": [{"timeUnixNano": "1000", "asDouble": 42.5}]}}
                }]}]
            }]
        });
        let metrics = OtlpDecoder::decode_metrics_json(json).unwrap();
        assert_eq!(metrics.len(), 1);
        assert_eq!(metrics[0].name, "cpu");
    }

    #[test]
    fn test_decode_logs_json() {
        let json = serde_json::json!({
            "resourceLogs": [{
                "resource": {"attributes": []},
                "scopeLogs": [{"scope": {"name": "test"}, "logRecords": [{
                    "timeUnixNano": "5000", "severityNumber": 9, "severityText": "INFO",
                    "body": {"stringValue": "hello"}
                }]}]
            }]
        });
        let logs = OtlpDecoder::decode_logs_json(json).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].body, "hello");
    }
}
