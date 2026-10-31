use super::correlation::extract_correlation_labels;
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric, MetricKind, MetricValue};
use crate::models::storage::{logs_schema, metrics_schema, traces_schema};
use crate::models::traces::Span;
use crate::storage::log_index::{LogIndexBuilder, LogIndexTable};
use crate::storage::series_index::{SeriesTable, SeriesTableBuilder};
use arrow::record_batch::RecordBatch;
use arrow_array::{
    builder::{
        FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        StringDictionaryBuilder, TimestampNanosecondBuilder, UInt32Builder,
    },
    types::Int32Type,
};
use std::sync::Arc;

/// Resolved correlation columns and resource JSON for one resource set.
struct ResolvedResource {
    json: String,
    correlation: super::correlation::CorrelationLabels,
}

const RESOURCE_CACHE_MAX: usize = 4096;

/// Memoises [`extract_correlation_labels`] per distinct resource set.
///
/// A block's rows come from a handful of resources, and each resource's
/// attributes are shared (one `LabelSet` handle) across all of its rows, so the
/// split into correlation columns and the resource JSON are computed once per
/// resource instead of once per row.
#[derive(Default)]
struct ResourceCache {
    by_addr: std::collections::HashMap<usize, usize>,
    resolved: Vec<(LabelSet, ResolvedResource)>,
}

impl ResourceCache {
    fn resolve(&mut self, labels: &LabelSet) -> &ResolvedResource {
        let idx = match self.by_addr.get(&labels.addr()) {
            Some(i) => *i,
            None => {
                // Bounded: rows that each carry a distinct set (unique ids in
                // their attributes) would otherwise keep a copy of every one.
                if self.resolved.len() >= RESOURCE_CACHE_MAX {
                    self.resolved.clear();
                    self.by_addr.clear();
                }
                let (rest, correlation) = extract_correlation_labels(labels);
                self.resolved.push((
                    // Keeps the handle alive so its address cannot be reused.
                    labels.clone(),
                    ResolvedResource {
                        json: rest.json_str().to_string(),
                        correlation,
                    },
                ));
                self.by_addr.insert(labels.addr(), self.resolved.len() - 1);
                self.resolved.len() - 1
            }
        };
        &self.resolved[idx].1
    }
}

/// Rows that share one metric identity: name, kind and resource attributes.
///
/// Everything that is constant per run - the metric name, the kind, the
/// correlation columns and the resource JSON - is resolved once per run and
/// appended as a block, rather than re-derived for every row.
pub struct MetricRun<'a> {
    pub name: &'a str,
    pub kind: MetricKind,
    pub resource: &'a LabelSet,
    pub points: Vec<&'a DataPoint>,
}

fn kind_name(kind: MetricKind) -> &'static str {
    match kind {
        MetricKind::Gauge => "Gauge",
        MetricKind::Sum => "Sum",
        MetricKind::Histogram => "Histogram",
        MetricKind::Summary => "Summary",
    }
}

/// Converts a list of [Metric]s into a single Arrow [RecordBatch].
pub fn metrics_to_chunk(metrics: &[Metric]) -> Result<RecordBatch> {
    let runs: Vec<MetricRun<'_>> = metrics
        .iter()
        .map(|m| MetricRun {
            name: &m.name,
            kind: m.kind,
            resource: &m.resource_attributes,
            points: m.data_points.iter().collect(),
        })
        .collect();
    metric_runs_to_chunk(&runs)
}

/// Converts runs of rows into a single Arrow [RecordBatch], in run order.
pub fn metric_runs_to_chunk(runs: &[MetricRun<'_>]) -> Result<RecordBatch> {
    build_metric_batch(runs, None)
}

/// As [`metric_runs_to_chunk`], and also describes the layout: a
/// [`SeriesTable`] listing every series of every run with the rows it occupies.
///
/// Within a run, consecutive points with equal labels form one series, so a
/// caller that orders each run by series (then time) gets exactly one entry per
/// series; an unordered run is still described correctly, only with more entries.
pub fn metric_runs_to_block(runs: &[MetricRun<'_>]) -> Result<(RecordBatch, SeriesTable)> {
    let mut table = SeriesTableBuilder::new();
    let batch = build_metric_batch(runs, Some(&mut table))?;
    Ok((batch, table.finish()))
}

fn build_metric_batch(
    runs: &[MetricRun<'_>],
    mut table: Option<&mut SeriesTableBuilder>,
) -> Result<RecordBatch> {
    let total: usize = runs.iter().map(|r| r.points.len()).sum();
    let mut timestamp_ns = TimestampNanosecondBuilder::with_capacity(total);

    // Dictionary columns - use DictionaryBuilder
    let mut metric_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut metric_kind = StringBuilder::with_capacity(total, total * 5);
    let mut service_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut service_version = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_namespace = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_uid = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_container_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_node_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut resource_attributes = StringDictionaryBuilder::<Int32Type>::new();

    // Non-dictionary columns
    let mut labels = StringBuilder::with_capacity(total, total * 48);
    let mut value_float = Float64Builder::with_capacity(total);
    let mut value_int = Int64Builder::with_capacity(total);
    let mut value_complex = StringBuilder::new();

    let mut next_row = 0u64;
    for run in runs {
        let n = run.points.len();
        if n == 0 {
            continue;
        }
        let (resource_labels, correlation) = extract_correlation_labels(run.resource);
        let first_row = next_row;
        next_row += n as u64;

        metric_name.append_values(run.name, n);
        metric_kind.append_value_n(kind_name(run.kind), n);
        service_name.append_options(correlation.service_name.as_deref(), n);
        service_version.append_options(correlation.service_version.as_deref(), n);
        k8s_namespace.append_options(correlation.k8s_namespace.as_deref(), n);
        k8s_pod_name.append_options(correlation.k8s_pod_name.as_deref(), n);
        k8s_pod_uid.append_options(correlation.k8s_pod_uid.as_deref(), n);
        k8s_container_name.append_options(correlation.k8s_container_name.as_deref(), n);
        k8s_node_name.append_options(correlation.k8s_node_name.as_deref(), n);
        resource_attributes.append_values(resource_labels.json_str(), n);

        if let Some(t) = table.as_deref_mut() {
            t.begin_metric(run.name);
            describe_series(
                t,
                &run.points,
                correlation.service_name.as_deref(),
                first_row,
            );
        }

        for dp in &run.points {
            timestamp_ns.append_value(dp.timestamp_ns);
            labels.append_value(dp.labels.json_str());

            match &dp.value {
                MetricValue::Double(v) => {
                    value_float.append_value(*v);
                    value_int.append_null();
                    value_complex.append_null();
                }
                MetricValue::Int(v) => {
                    value_float.append_null();
                    value_int.append_value(*v);
                    value_complex.append_null();
                }
                complex => {
                    value_float.append_null();
                    value_int.append_null();
                    value_complex
                        .append_value(&serde_json::to_string(complex).map_err(Error::Serde)?);
                }
            }
        }
    }

    let schema = metrics_schema();
    let record_batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(timestamp_ns.finish()),
            Arc::new(metric_name.finish()),
            Arc::new(metric_kind.finish()),
            Arc::new(service_name.finish()),
            Arc::new(service_version.finish()),
            Arc::new(k8s_namespace.finish()),
            Arc::new(k8s_pod_name.finish()),
            Arc::new(k8s_pod_uid.finish()),
            Arc::new(k8s_container_name.finish()),
            Arc::new(k8s_node_name.finish()),
            Arc::new(resource_attributes.finish()),
            Arc::new(labels.finish()),
            Arc::new(value_float.finish()),
            Arc::new(value_int.finish()),
            Arc::new(value_complex.finish()),
        ],
    )
    .map_err(|e| Error::Arrow(e.to_string()))?;

    Ok(record_batch)
}

/// Adds one table entry per series of `points`, which occupy the rows starting
/// at `first_row`. A series ends where the next point's labels differ.
fn describe_series(
    table: &mut SeriesTableBuilder,
    points: &[&DataPoint],
    service: Option<&str>,
    first_row: u64,
) {
    let mut i = 0;
    while i < points.len() {
        let labels = &points[i].labels;
        let (mut min_ts, mut max_ts) = (points[i].timestamp_ns, points[i].timestamp_ns);
        let mut j = i + 1;
        while j < points.len() && (points[j].labels.ptr_eq(labels) || points[j].labels == *labels) {
            min_ts = min_ts.min(points[j].timestamp_ns);
            max_ts = max_ts.max(points[j].timestamp_ns);
            j += 1;
        }
        // As a scan reports it: the resource's service name merged in.
        let merged = match service {
            Some(svc) if !svc.is_empty() => labels.with("service.name", svc),
            _ => labels.clone(),
        };
        table.push_series(
            &merged,
            first_row + i as u64,
            (j - i) as u32,
            min_ts,
            max_ts,
        );
        i = j;
    }
}

/// Converts a list of [LogRecord]s into a single Arrow [RecordBatch].
pub fn logs_to_chunk(logs: &[LogRecord]) -> Result<RecordBatch> {
    let mut timestamp_ns = TimestampNanosecondBuilder::new();
    let mut observed_timestamp_ns = TimestampNanosecondBuilder::new();
    let mut severity_number = Int32Builder::new();

    // Dictionary columns
    let mut severity_text = StringDictionaryBuilder::<Int32Type>::new();
    let mut body = StringBuilder::new();
    let mut service_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut service_version = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_namespace = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_uid = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_container_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_node_name = StringDictionaryBuilder::<Int32Type>::new();

    // FixedSizeBinary columns
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(logs.len(), 16);
    let mut span_id = FixedSizeBinaryBuilder::with_capacity(logs.len(), 8);
    let mut flags = UInt32Builder::new();
    let mut scope_name = StringBuilder::new();
    let mut scope_version = StringBuilder::new();
    let mut attributes = StringBuilder::new();
    let mut resource_attributes = StringBuilder::new();

    let mut resources = ResourceCache::default();
    for log in logs {
        let res = resources.resolve(&log.resource_attributes);
        let correlation = &res.correlation;
        timestamp_ns.append_value(log.timestamp_ns);
        observed_timestamp_ns.append_value(log.observed_timestamp_ns);
        severity_number.append_value(log.severity_number);
        severity_text.append_value(&log.severity_text);
        body.append_value(&log.body);
        service_name.append_option(correlation.service_name.as_deref());
        service_version.append_option(correlation.service_version.as_deref());
        k8s_namespace.append_option(correlation.k8s_namespace.as_deref());
        k8s_pod_name.append_option(correlation.k8s_pod_name.as_deref());
        k8s_pod_uid.append_option(correlation.k8s_pod_uid.as_deref());
        k8s_container_name.append_option(correlation.k8s_container_name.as_deref());
        k8s_node_name.append_option(correlation.k8s_node_name.as_deref());

        // FixedSizeBinary: trace_id is 16 bytes, span_id is 8 bytes
        let _ = trace_id.append_value(log.trace_id);
        let _ = span_id.append_value(log.span_id);

        flags.append_value(log.flags);
        scope_name.append_value(&log.scope_name);
        scope_version.append_value(&log.scope_version);
        attributes.append_value(log.attributes.json_str());
        resource_attributes.append_value(&res.json);
    }

    let schema = logs_schema();
    let record_batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(timestamp_ns.finish()),
            Arc::new(observed_timestamp_ns.finish()),
            Arc::new(severity_number.finish()),
            Arc::new(severity_text.finish()),
            Arc::new(body.finish()),
            Arc::new(service_name.finish()),
            Arc::new(service_version.finish()),
            Arc::new(k8s_namespace.finish()),
            Arc::new(k8s_pod_name.finish()),
            Arc::new(k8s_pod_uid.finish()),
            Arc::new(k8s_container_name.finish()),
            Arc::new(k8s_node_name.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(span_id.finish()),
            Arc::new(flags.finish()),
            Arc::new(scope_name.finish()),
            Arc::new(scope_version.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource_attributes.finish()),
        ],
    )
    .map_err(|e| Error::Arrow(e.to_string()))?;

    Ok(record_batch)
}

/// As [`logs_to_chunk`], and also builds the block's skip index for the rows in
/// the order they are written.
pub fn logs_to_block(logs: &[LogRecord]) -> Result<(RecordBatch, LogIndexTable)> {
    let batch = logs_to_chunk(logs)?;
    let mut index = LogIndexBuilder::new();
    for log in logs {
        index.push_row(
            log.timestamp_ns,
            log.severity_number,
            &log.body,
            &log.attributes,
            &log.resource_attributes,
        );
    }
    Ok((batch, index.finish()))
}

/// Converts a list of [Span]s into a single Arrow [RecordBatch].
pub fn traces_to_chunk(spans: &[Span]) -> Result<RecordBatch> {
    let mut timestamp_ns = TimestampNanosecondBuilder::new();

    // FixedSizeBinary columns
    let mut span_id_col = FixedSizeBinaryBuilder::with_capacity(spans.len(), 8);
    let mut trace_id = FixedSizeBinaryBuilder::with_capacity(spans.len(), 16);
    let mut parent_span_id = FixedSizeBinaryBuilder::with_capacity(spans.len(), 8);

    // Dictionary columns
    let mut span_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut span_kind = StringBuilder::new();
    let mut status_code = StringBuilder::new();
    let mut status_message = StringBuilder::new();
    let mut service_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut service_version = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_namespace = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_uid = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_container_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_node_name = StringDictionaryBuilder::<Int32Type>::new();

    // Timestamp columns
    let mut start_time_ns = TimestampNanosecondBuilder::new();
    let mut end_time_ns = TimestampNanosecondBuilder::new();
    let mut duration_ns = Int64Builder::new();
    let mut trace_state = StringBuilder::new();
    let mut flags = UInt32Builder::new();

    // Regular string columns
    let mut attributes = StringBuilder::new();
    let mut resource_attributes = StringBuilder::new();
    let mut events = StringBuilder::new();
    let mut links = StringBuilder::new();

    let mut resources = ResourceCache::default();
    for span in spans {
        let res = resources.resolve(&span.attributes);
        let correlation = &res.correlation;
        let kind_str = match span.kind {
            1 => "SPAN_KIND_INTERNAL",
            2 => "SPAN_KIND_SERVER",
            3 => "SPAN_KIND_CLIENT",
            4 => "SPAN_KIND_PRODUCER",
            5 => "SPAN_KIND_CONSUMER",
            _ => "SPAN_KIND_UNSPECIFIED",
        };
        let status_str = match span.status.code {
            1 => "STATUS_CODE_OK",
            2 => "STATUS_CODE_ERROR",
            _ => "STATUS_CODE_UNSET",
        };

        timestamp_ns.append_value(span.start_time_ns);
        let _ = span_id_col.append_value(span.span_id);
        span_name.append_value(&span.name);
        span_kind.append_value(kind_str);
        start_time_ns.append_value(span.start_time_ns);
        end_time_ns.append_value(span.end_time_ns);
        duration_ns.append_value(span.duration_ns());
        status_code.append_value(status_str);
        status_message.append_value(&span.status.message);
        service_name.append_option(correlation.service_name.as_deref());
        service_version.append_option(correlation.service_version.as_deref());
        k8s_namespace.append_option(correlation.k8s_namespace.as_deref());
        k8s_pod_name.append_option(correlation.k8s_pod_name.as_deref());
        k8s_pod_uid.append_option(correlation.k8s_pod_uid.as_deref());
        k8s_container_name.append_option(correlation.k8s_container_name.as_deref());
        k8s_node_name.append_option(correlation.k8s_node_name.as_deref());
        let _ = trace_id.append_value(span.trace_id);
        let _ = parent_span_id.append_value(span.parent_span_id);
        flags.append_value(span.flags);
        trace_state.append_value(&span.trace_state);
        attributes.append_value(span.attributes.json_str());
        resource_attributes.append_value(&res.json);
        events.append_value(&serde_json::to_string(&span.events).map_err(Error::Serde)?);
        links.append_value(&serde_json::to_string(&span.links).map_err(Error::Serde)?);
    }

    let schema = traces_schema();
    let record_batch = RecordBatch::try_new(
        schema.into(),
        vec![
            Arc::new(timestamp_ns.finish()),
            Arc::new(span_id_col.finish()),
            Arc::new(span_name.finish()),
            Arc::new(span_kind.finish()),
            Arc::new(start_time_ns.finish()),
            Arc::new(end_time_ns.finish()),
            Arc::new(duration_ns.finish()),
            Arc::new(status_code.finish()),
            Arc::new(status_message.finish()),
            Arc::new(service_name.finish()),
            Arc::new(service_version.finish()),
            Arc::new(k8s_namespace.finish()),
            Arc::new(k8s_pod_name.finish()),
            Arc::new(k8s_pod_uid.finish()),
            Arc::new(k8s_container_name.finish()),
            Arc::new(k8s_node_name.finish()),
            Arc::new(trace_id.finish()),
            Arc::new(parent_span_id.finish()),
            Arc::new(flags.finish()),
            Arc::new(trace_state.finish()),
            Arc::new(attributes.finish()),
            Arc::new(resource_attributes.finish()),
            Arc::new(events.finish()),
            Arc::new(links.finish()),
        ],
    )
    .map_err(|e| Error::Arrow(e.to_string()))?;

    Ok(record_batch)
}

#[cfg(test)]
mod conversion_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use arrow_array::{Array, DictionaryArray, StringArray};

    fn dict_value(batch: &RecordBatch, col: &str, row: usize) -> Option<String> {
        let c = batch.column(batch.schema().index_of(col).unwrap());
        let d = c
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        if d.is_null(row) {
            return None;
        }
        let v = d.values().as_any().downcast_ref::<StringArray>().unwrap();
        Some(v.value(d.keys().value(row) as usize).to_string())
    }

    fn str_value(batch: &RecordBatch, col: &str, row: usize) -> String {
        let c = batch.column(batch.schema().index_of(col).unwrap());
        c.as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string()
    }

    /// Rows from different resources (and one with none) must each get their own
    /// correlation columns and resource JSON, however the rows interleave.
    #[test]
    fn logs_conversion_resolves_each_resource_per_row() {
        let a = LabelSet::try_from_iter(vec![
            ("service.name", "api"),
            ("k8s.pod.name", "api-0"),
            ("region", "eu"),
        ])
        .unwrap();
        let b = LabelSet::try_from_iter(vec![("service.name", "db"), ("zone", "z1")]).unwrap();
        let none = LabelSet::default();
        let mk = |ts: i64, res: &LabelSet| {
            LogRecord::new(
                ts,
                ts,
                9,
                "INFO".into(),
                format!("m{ts}"),
                LabelSet::try_from_iter(vec![("k", format!("v{ts}"))]).unwrap(),
                res.clone(),
                [0; 16],
                [0; 8],
                0,
                "".into(),
                "".into(),
            )
        };
        let logs = vec![mk(1, &a), mk(2, &b), mk(3, &a), mk(4, &none), mk(5, &b)];
        let batch = logs_to_chunk(&logs).unwrap();
        let want_svc = [Some("api"), Some("db"), Some("api"), None, Some("db")];
        let want_pod = [Some("api-0"), None, Some("api-0"), None, None];
        let want_res = [
            r#"{"region":"eu"}"#,
            r#"{"zone":"z1"}"#,
            r#"{"region":"eu"}"#,
            "{}",
            r#"{"zone":"z1"}"#,
        ];
        for i in 0..5 {
            assert_eq!(
                dict_value(&batch, "service_name", i).as_deref(),
                want_svc[i]
            );
            assert_eq!(
                dict_value(&batch, "k8s_pod_name", i).as_deref(),
                want_pod[i]
            );
            assert_eq!(str_value(&batch, "resource_attributes", i), want_res[i]);
            assert_eq!(
                str_value(&batch, "attributes", i),
                format!(r#"{{"k":"v{}"}}"#, i + 1)
            );
        }
    }
}
