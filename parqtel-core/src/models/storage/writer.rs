use super::correlation::extract_correlation_labels;
use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::{Metric, MetricValue};
use crate::models::storage::{logs_schema, metrics_schema, traces_schema};
use crate::models::traces::Span;
use arrow::record_batch::RecordBatch;
use arrow_array::{
    builder::{
        FixedSizeBinaryBuilder, Float64Builder, Int32Builder, Int64Builder, StringBuilder,
        StringDictionaryBuilder, TimestampNanosecondBuilder, UInt32Builder,
    },
    types::Int32Type,
};
use std::sync::Arc;

/// Converts a list of [Metric]s into a single Arrow [RecordBatch].
pub fn metrics_to_chunk(metrics: &[Metric]) -> Result<RecordBatch> {
    let mut timestamp_ns = TimestampNanosecondBuilder::new();

    // Dictionary columns - use DictionaryBuilder
    let mut metric_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut metric_kind = StringBuilder::new();
    let mut service_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut service_version = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_namespace = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_pod_uid = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_container_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut k8s_node_name = StringDictionaryBuilder::<Int32Type>::new();
    let mut resource_attributes = StringDictionaryBuilder::<Int32Type>::new();

    // Non-dictionary columns
    let mut labels = StringBuilder::new();
    let mut value_float = Float64Builder::new();
    let mut value_int = Int64Builder::new();
    let mut value_complex = StringBuilder::new();

    for metric in metrics {
        let (resource_labels, correlation) =
            extract_correlation_labels(&metric.resource_attributes);
        let resource_attr_json = resource_labels.to_json()?;
        let kind_str = format!("{:?}", metric.kind);

        for dp in &metric.data_points {
            timestamp_ns.append_value(dp.timestamp_ns);
            metric_name.append_value(&metric.name);
            metric_kind.append_value(&kind_str);
            service_name.append_option(correlation.service_name.as_deref());
            service_version.append_option(correlation.service_version.as_deref());
            k8s_namespace.append_option(correlation.k8s_namespace.as_deref());
            k8s_pod_name.append_option(correlation.k8s_pod_name.as_deref());
            k8s_pod_uid.append_option(correlation.k8s_pod_uid.as_deref());
            k8s_container_name.append_option(correlation.k8s_container_name.as_deref());
            k8s_node_name.append_option(correlation.k8s_node_name.as_deref());
            resource_attributes.append_value(&resource_attr_json);
            labels.append_value(&dp.labels.to_json()?);

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

    for log in logs {
        let (res_labels, correlation) = extract_correlation_labels(&log.resource_attributes);
        timestamp_ns.append_value(log.timestamp_ns);
        observed_timestamp_ns.append_value(log.observed_timestamp_ns);
        severity_number.append_value(log.severity_number);
        severity_text.append_value(&log.severity_text);
        body.append_value(&log.body);
        service_name.append_option(correlation.service_name.as_deref().map(|s| s.to_string()));
        service_version.append_option(
            correlation
                .service_version
                .as_deref()
                .map(|s| s.to_string()),
        );
        k8s_namespace.append_option(correlation.k8s_namespace.as_deref().map(|s| s.to_string()));
        k8s_pod_name.append_option(correlation.k8s_pod_name.as_deref().map(|s| s.to_string()));
        k8s_pod_uid.append_option(correlation.k8s_pod_uid.as_deref().map(|s| s.to_string()));
        k8s_container_name.append_option(
            correlation
                .k8s_container_name
                .as_deref()
                .map(|s| s.to_string()),
        );
        k8s_node_name.append_option(correlation.k8s_node_name.as_deref().map(|s| s.to_string()));

        // FixedSizeBinary: trace_id is 16 bytes, span_id is 8 bytes
        let _ = trace_id.append_value(log.trace_id);
        let _ = span_id.append_value(log.span_id);

        flags.append_value(log.flags);
        scope_name.append_value(&log.scope_name);
        scope_version.append_value(&log.scope_version);
        attributes.append_value(&log.attributes.to_json()?);
        resource_attributes.append_value(&res_labels.to_json()?);
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

    for span in spans {
        let (resource_labels, correlation) = extract_correlation_labels(&span.attributes);
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
        service_name.append_option(correlation.service_name.as_deref().map(|s| s.to_string()));
        service_version.append_option(
            correlation
                .service_version
                .as_deref()
                .map(|s| s.to_string()),
        );
        k8s_namespace.append_option(correlation.k8s_namespace.as_deref().map(|s| s.to_string()));
        k8s_pod_name.append_option(correlation.k8s_pod_name.as_deref().map(|s| s.to_string()));
        k8s_pod_uid.append_option(correlation.k8s_pod_uid.as_deref().map(|s| s.to_string()));
        k8s_container_name.append_option(
            correlation
                .k8s_container_name
                .as_deref()
                .map(|s| s.to_string()),
        );
        k8s_node_name.append_option(correlation.k8s_node_name.as_deref().map(|s| s.to_string()));
        let _ = trace_id.append_value(span.trace_id);
        let _ = parent_span_id.append_value(span.parent_span_id);
        flags.append_value(span.flags);
        trace_state.append_value(&span.trace_state);
        attributes.append_value(&span.attributes.to_json()?);
        resource_attributes.append_value(&resource_labels.to_json()?);
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
