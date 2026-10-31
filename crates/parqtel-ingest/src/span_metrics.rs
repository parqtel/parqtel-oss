//! Span-metrics RED bridge: derives Rate/Errors/Duration metrics from trace
//! spans and feeds them back through the normal metrics ingestion path so
//! services get out-of-the-box RED dashboards without hand-written PromQL.
//!
//! Derived metrics (labels: `service`, `operation`):
//! - `traces_service_requests_total`  — one point per span (rate via `rate()`)
//! - `traces_service_errors_total`    — 1 when the span status is ERROR
//! - `traces_service_duration_ms`     — span duration in milliseconds (gauge)

use parqtel_core::models::metrics::{DataPoint, Metric, MetricKind, MetricValue};
use parqtel_core::models::traces::Span;
use parqtel_core::LabelSet;
use std::collections::BTreeMap;

/// Metric names emitted by the bridge.
pub const REQUESTS_METRIC: &str = "traces_service_requests_total";
pub const ERRORS_METRIC: &str = "traces_service_errors_total";
pub const DURATION_METRIC: &str = "traces_service_duration_ms";

/// Only server-kind spans represent user-facing requests.
const SERVER_KIND: i32 = 2;

/// Derives RED metrics from a batch of spans.
///
/// Only `SPAN_KIND_SERVER` spans are counted — they map to inbound requests.
/// Client/internal spans would inflate request rates for downstream calls.
pub fn derive_span_metrics(spans: &[Span]) -> Vec<Metric> {
    // (service, operation) → (requests, errors, duration_ms_sum, count, last_ts)
    struct Acc {
        requests: Vec<DataPoint>,
        errors: Vec<DataPoint>,
        durations: Vec<DataPoint>,
    }

    let mut by_series: BTreeMap<(String, String), Acc> = BTreeMap::new();

    for span in spans {
        if span.kind != SERVER_KIND {
            continue;
        }
        let service = span
            .attributes
            .get("service.name")
            .map(|v| v.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let operation = span.name.clone();
        let ts = span.start_time_ns;
        let is_error = span.status.code == 2;
        let duration_ms = span.duration_ns() as f64 / 1_000_000.0;

        let entry = by_series
            .entry((service.clone(), operation.clone()))
            .or_insert_with(|| Acc {
                requests: Vec::new(),
                errors: Vec::new(),
                durations: Vec::new(),
            });

        // `service.name` rides on the point labels (semantic-convention
        // label) so buffered queries match on it regardless of which
        // service a point belongs to; `service`/`operation` are the
        // RED grouping labels.
        let labels = match LabelSet::try_from_iter(vec![
            ("service", service.clone()),
            ("operation", operation.clone()),
            ("service.name", service),
        ]) {
            Ok(l) => l,
            Err(_) => continue,
        };

        entry.requests.push(DataPoint {
            timestamp_ns: ts,
            value: MetricValue::Double(1.0),
            labels: labels.clone(),
        });
        entry.errors.push(DataPoint {
            timestamp_ns: ts,
            value: MetricValue::Double(if is_error { 1.0 } else { 0.0 }),
            labels: labels.clone(),
        });
        entry.durations.push(DataPoint {
            timestamp_ns: ts,
            value: MetricValue::Double(duration_ms),
            labels,
        });
    }

    // One metric per kind (not per series): all series' points merged into a
    // single metric with service/operation labels per point.
    let mut requests = Vec::new();
    let mut errors = Vec::new();
    let mut durations = Vec::new();
    for (_key, acc) in by_series {
        requests.extend(acc.requests);
        errors.extend(acc.errors);
        durations.extend(acc.durations);
    }

    let mut metrics = Vec::new();
    for (name, points, unit) in [
        (REQUESTS_METRIC, requests, "1"),
        (ERRORS_METRIC, errors, "1"),
        (DURATION_METRIC, durations, "ms"),
    ] {
        if points.is_empty() {
            continue;
        }
        metrics.push(Metric {
            name: name.to_string(),
            description: "Derived from trace spans by the span-metrics RED bridge".into(),
            unit: unit.into(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::default(),
            data_points: points,
        });
    }
    metrics
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use parqtel_core::models::traces::SpanStatus;

    fn server_span(name: &str, service: &str, status: i32, dur_ns: i64) -> Span {
        let attrs = LabelSet::try_from_iter(vec![("service.name", service.to_string())])
            .unwrap_or_default();
        Span {
            trace_id: [1; 16],
            span_id: [1; 8],
            trace_state: String::new(),
            parent_span_id: [0; 8],
            name: name.into(),
            kind: SERVER_KIND,
            start_time_ns: 1_000,
            end_time_ns: 1_000 + dur_ns,
            attributes: attrs,
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: status,
                message: String::new(),
            },
            flags: 0,
        }
    }

    #[test]
    fn derives_red_for_server_spans_only() {
        let mut internal = server_span("db-call", "api", 0, 1_000_000);
        internal.kind = 1; // internal — must be excluded
        let spans = vec![
            server_span("GET /a", "api", 0, 2_000_000),
            server_span("GET /a", "api", 2, 5_000_000),
            internal,
        ];
        let metrics = derive_span_metrics(&spans);
        assert_eq!(metrics.len(), 3); // requests, errors, duration

        let reqs = metrics.iter().find(|m| m.name == REQUESTS_METRIC).unwrap();
        assert_eq!(reqs.data_points.len(), 2);
        let errs = metrics.iter().find(|m| m.name == ERRORS_METRIC).unwrap();
        let vals: Vec<f64> = errs
            .data_points
            .iter()
            .map(|dp| match dp.value {
                MetricValue::Double(v) => v,
                _ => 0.0,
            })
            .collect();
        assert_eq!(vals, vec![0.0, 1.0]);
        let dur = metrics.iter().find(|m| m.name == DURATION_METRIC).unwrap();
        let dvals: Vec<f64> = dur
            .data_points
            .iter()
            .map(|dp| match dp.value {
                MetricValue::Double(v) => v,
                _ => 0.0,
            })
            .collect();
        assert_eq!(dvals, vec![2.0, 5.0]);
    }

    #[test]
    fn groups_by_service_and_operation() {
        let spans = vec![
            server_span("GET /a", "api", 0, 1_000_000),
            server_span("GET /b", "api", 0, 1_000_000),
            server_span("GET /x", "web", 0, 1_000_000),
        ];
        let metrics = derive_span_metrics(&spans);
        let reqs = metrics.iter().find(|m| m.name == REQUESTS_METRIC).unwrap();
        assert_eq!(reqs.data_points.len(), 3);
        let series: Vec<String> = reqs
            .data_points
            .iter()
            .map(|dp| {
                format!(
                    "{}/{}",
                    dp.labels.get("service").unwrap_or_default(),
                    dp.labels.get("operation").unwrap_or_default()
                )
            })
            .collect();
        assert!(series.contains(&"api/GET /a".to_string()));
        assert!(series.contains(&"api/GET /b".to_string()));
        assert!(series.contains(&"web/GET /x".to_string()));
    }
}
