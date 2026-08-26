use super::correlation::extract_correlation_labels;
use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::{Metric, MetricValue};
use crate::models::traces::Span;
use arrow2::array::{
    Array, BinaryArray, DictionaryArray, MutableBinaryArray, MutableDictionaryArray,
    MutablePrimitiveArray, MutableUtf8Array, PrimitiveArray, TryPush, Utf8Array,
};
use arrow2::chunk::Chunk;
use std::sync::Arc;

/// Converts a list of [Metric]s into a single Arrow [Chunk].
pub fn metrics_to_chunk(metrics: &[Metric]) -> Result<Chunk<Arc<dyn Array>>> {
    let mut timestamp_ns = MutablePrimitiveArray::<i64>::new();
    let mut metric_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut metric_kind = MutableUtf8Array::<i32>::new();
    let mut service_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut service_version = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_namespace = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_pod_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_pod_uid = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_container_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_node_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut resource_attributes = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut labels = MutableUtf8Array::<i32>::new();
    let mut value_float = MutablePrimitiveArray::<f64>::new();
    let mut value_int = MutablePrimitiveArray::<i64>::new();
    let mut value_complex = MutableUtf8Array::<i32>::new();

    for metric in metrics {
        let (resource_labels, correlation) =
            extract_correlation_labels(&metric.resource_attributes);
        let resource_attr_json = resource_labels.to_json()?;
        let kind_str = format!("{:?}", metric.kind);

        for dp in &metric.data_points {
            timestamp_ns.push(Some(dp.timestamp_ns));
            metric_name
                .try_push(Some(&metric.name))
                .map_err(|e| Error::Arrow(e.to_string()))?;
            metric_kind.push(Some(&kind_str));
            service_name
                .try_push(correlation.service_name.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            service_version
                .try_push(correlation.service_version.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            k8s_namespace
                .try_push(correlation.k8s_namespace.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            k8s_pod_name
                .try_push(correlation.k8s_pod_name.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            k8s_pod_uid
                .try_push(correlation.k8s_pod_uid.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            k8s_container_name
                .try_push(correlation.k8s_container_name.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            k8s_node_name
                .try_push(correlation.k8s_node_name.as_deref())
                .map_err(|e| Error::Arrow(e.to_string()))?;
            resource_attributes
                .try_push(Some(&resource_attr_json))
                .map_err(|e| Error::Arrow(e.to_string()))?;
            labels.push(Some(&dp.labels.to_json()?));

            match &dp.value {
                MetricValue::Double(v) => {
                    value_float.push(Some(*v));
                    value_int.push(None);
                    value_complex.push(None::<&str>);
                }
                MetricValue::Int(v) => {
                    value_float.push(None);
                    value_int.push(Some(*v));
                    value_complex.push(None::<&str>);
                }
                complex => {
                    value_float.push(None);
                    value_int.push(None);
                    value_complex
                        .push(Some(&serde_json::to_string(complex).map_err(Error::Serde)?));
                }
            }
        }
    }

    Ok(Chunk::new(vec![
        Arc::new(Into::<PrimitiveArray<i64>>::into(timestamp_ns)),
        Arc::new(Into::<DictionaryArray<i32>>::into(metric_name)),
        Arc::new(Into::<Utf8Array<i32>>::into(metric_kind)),
        Arc::new(Into::<DictionaryArray<i32>>::into(service_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(service_version)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_namespace)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_pod_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_pod_uid)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_container_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_node_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(resource_attributes)),
        Arc::new(Into::<Utf8Array<i32>>::into(labels)),
        Arc::new(Into::<PrimitiveArray<f64>>::into(value_float)),
        Arc::new(Into::<PrimitiveArray<i64>>::into(value_int)),
        Arc::new(Into::<Utf8Array<i32>>::into(value_complex)),
    ]))
}

/// Converts a list of [LogRecord]s into a single Arrow [Chunk].
pub fn logs_to_chunk(logs: &[LogRecord]) -> Result<Chunk<Arc<dyn Array>>> {
    let mut timestamp_ns = MutablePrimitiveArray::<i64>::new();
    let mut observed_timestamp_ns = MutablePrimitiveArray::<i64>::new();
    let mut severity_number = MutablePrimitiveArray::<i32>::new();
    let mut severity_text = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut body = MutableUtf8Array::<i32>::new();
    let mut service_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut service_version = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_namespace = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_pod_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_pod_uid = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_container_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_node_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut trace_id = MutableBinaryArray::<i32>::new();
    let mut span_id = MutableBinaryArray::<i32>::new();
    let mut flags = MutablePrimitiveArray::<u32>::new();
    let mut scope_name = MutableUtf8Array::<i32>::new();
    let mut scope_version = MutableUtf8Array::<i32>::new();
    let mut attributes = MutableUtf8Array::<i32>::new();
    let mut resource_attributes = MutableUtf8Array::<i32>::new();

    for log in logs {
        let (res_labels, correlation) = extract_correlation_labels(&log.resource_attributes);
        timestamp_ns.push(Some(log.timestamp_ns));
        observed_timestamp_ns.push(Some(log.observed_timestamp_ns));
        severity_number.push(Some(log.severity_number));
        severity_text
            .try_push(Some(&log.severity_text))
            .map_err(|e| Error::Arrow(e.to_string()))?;
        body.push(Some(&log.body));
        service_name
            .try_push(correlation.service_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        service_version
            .try_push(correlation.service_version.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_namespace
            .try_push(correlation.k8s_namespace.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_pod_name
            .try_push(correlation.k8s_pod_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_pod_uid
            .try_push(correlation.k8s_pod_uid.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_container_name
            .try_push(correlation.k8s_container_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_node_name
            .try_push(correlation.k8s_node_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        trace_id.push(Some(&log.trace_id));
        span_id.push(Some(&log.span_id));
        flags.push(Some(log.flags));
        scope_name.push(Some(&log.scope_name));
        scope_version.push(Some(&log.scope_version));
        attributes.push(Some(&log.attributes.to_json()?));
        resource_attributes.push(Some(&res_labels.to_json()?));
    }

    Ok(Chunk::new(vec![
        Arc::new(Into::<PrimitiveArray<i64>>::into(timestamp_ns)),
        Arc::new(Into::<PrimitiveArray<i64>>::into(observed_timestamp_ns)),
        Arc::new(Into::<PrimitiveArray<i32>>::into(severity_number)),
        Arc::new(Into::<DictionaryArray<i32>>::into(severity_text)),
        Arc::new(Into::<Utf8Array<i32>>::into(body)),
        Arc::new(Into::<DictionaryArray<i32>>::into(service_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(service_version)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_namespace)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_pod_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_pod_uid)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_container_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_node_name)),
        Arc::new(Into::<BinaryArray<i32>>::into(trace_id)),
        Arc::new(Into::<BinaryArray<i32>>::into(span_id)),
        Arc::new(Into::<PrimitiveArray<u32>>::into(flags)),
        Arc::new(Into::<Utf8Array<i32>>::into(scope_name)),
        Arc::new(Into::<Utf8Array<i32>>::into(scope_version)),
        Arc::new(Into::<Utf8Array<i32>>::into(attributes)),
        Arc::new(Into::<Utf8Array<i32>>::into(resource_attributes)),
    ]))
}

/// Converts a list of [Span]s into a single Arrow [Chunk].
pub fn traces_to_chunk(spans: &[Span]) -> Result<Chunk<Arc<dyn Array>>> {
    let mut timestamp_ns = MutablePrimitiveArray::<i64>::new();
    let mut span_id_col = MutableBinaryArray::<i32>::new();
    let mut span_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut span_kind = MutableUtf8Array::<i32>::new();
    let mut start_time_ns = MutablePrimitiveArray::<i64>::new();
    let mut end_time_ns = MutablePrimitiveArray::<i64>::new();
    let mut duration_ns = MutablePrimitiveArray::<i64>::new();
    let mut status_code = MutableUtf8Array::<i32>::new();
    let mut status_message = MutableUtf8Array::<i32>::new();
    let mut service_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut service_version = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_namespace = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_pod_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_pod_uid = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_container_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut k8s_node_name = MutableDictionaryArray::<i32, MutableUtf8Array<i32>>::new();
    let mut trace_id = MutableBinaryArray::<i32>::new();
    let mut parent_span_id = MutableBinaryArray::<i32>::new();
    let mut flags = MutablePrimitiveArray::<u32>::new();
    let mut trace_state = MutableUtf8Array::<i32>::new();
    let mut attributes = MutableUtf8Array::<i32>::new();
    let mut resource_attributes = MutableUtf8Array::<i32>::new();
    let mut events = MutableUtf8Array::<i32>::new();
    let mut links = MutableUtf8Array::<i32>::new();

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

        timestamp_ns.push(Some(span.start_time_ns));
        span_id_col.push(Some(&span.span_id));
        span_name
            .try_push(Some(&span.name))
            .map_err(|e| Error::Arrow(e.to_string()))?;
        span_kind.push(Some(kind_str));
        start_time_ns.push(Some(span.start_time_ns));
        end_time_ns.push(Some(span.end_time_ns));
        duration_ns.push(Some(span.duration_ns()));
        status_code.push(Some(status_str));
        status_message.push(Some(&span.status.message));
        service_name
            .try_push(correlation.service_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        service_version
            .try_push(correlation.service_version.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_namespace
            .try_push(correlation.k8s_namespace.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_pod_name
            .try_push(correlation.k8s_pod_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_pod_uid
            .try_push(correlation.k8s_pod_uid.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_container_name
            .try_push(correlation.k8s_container_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        k8s_node_name
            .try_push(correlation.k8s_node_name.as_deref())
            .map_err(|e| Error::Arrow(e.to_string()))?;
        trace_id.push(Some(&span.trace_id));
        parent_span_id.push(Some(&span.parent_span_id));
        flags.push(Some(span.flags));
        trace_state.push(Some(&span.trace_state));
        attributes.push(Some(&span.attributes.to_json()?));
        resource_attributes.push(Some(&resource_labels.to_json()?));
        events.push(Some(
            &serde_json::to_string(&span.events).map_err(Error::Serde)?,
        ));
        links.push(Some(
            &serde_json::to_string(&span.links).map_err(Error::Serde)?,
        ));
    }

    Ok(Chunk::new(vec![
        Arc::new(Into::<PrimitiveArray<i64>>::into(timestamp_ns)),
        Arc::new(Into::<BinaryArray<i32>>::into(span_id_col)),
        Arc::new(Into::<DictionaryArray<i32>>::into(span_name)),
        Arc::new(Into::<Utf8Array<i32>>::into(span_kind)),
        Arc::new(Into::<PrimitiveArray<i64>>::into(start_time_ns)),
        Arc::new(Into::<PrimitiveArray<i64>>::into(end_time_ns)),
        Arc::new(Into::<PrimitiveArray<i64>>::into(duration_ns)),
        Arc::new(Into::<Utf8Array<i32>>::into(status_code)),
        Arc::new(Into::<Utf8Array<i32>>::into(status_message)),
        Arc::new(Into::<DictionaryArray<i32>>::into(service_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(service_version)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_namespace)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_pod_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_pod_uid)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_container_name)),
        Arc::new(Into::<DictionaryArray<i32>>::into(k8s_node_name)),
        Arc::new(Into::<BinaryArray<i32>>::into(trace_id)),
        Arc::new(Into::<BinaryArray<i32>>::into(parent_span_id)),
        Arc::new(Into::<PrimitiveArray<u32>>::into(flags)),
        Arc::new(Into::<Utf8Array<i32>>::into(trace_state)),
        Arc::new(Into::<Utf8Array<i32>>::into(attributes)),
        Arc::new(Into::<Utf8Array<i32>>::into(resource_attributes)),
        Arc::new(Into::<Utf8Array<i32>>::into(events)),
        Arc::new(Into::<Utf8Array<i32>>::into(links)),
    ]))
}
