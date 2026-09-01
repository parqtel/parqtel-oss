use super::correlation::{inject_correlation, row_to_correlation};
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, MetricKind, MetricValue};
use crate::models::traces::{Span, SpanEvent, SpanLink, SpanStatus};
use arrow::record_batch::RecordBatch;
use arrow_array::types::Int32Type;
use arrow_array::{
    Array, BinaryArray, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int32Array,
    Int64Array, StringArray, TimestampNanosecondArray, UInt32Array,
};
use std::collections::HashMap;

/// Reads a single row from an Arrow [RecordBatch] back into a [DataPoint] and its metric metadata.
pub fn row_to_point(
    batch: &RecordBatch,
    row: usize,
) -> Result<(String, MetricKind, LabelSet, DataPoint)> {
    let timestamp_ns = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?
        .value(row);

    let metric_name_arr = batch
        .column(1)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| Error::Arrow("Invalid metric_name column".into()))?;
    let metric_name = metric_name_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid metric_name values".into()))?
        .value(metric_name_arr.keys().value(row) as usize)
        .to_string();

    let kind_str = batch
        .column(2)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid metric_kind column".into()))?
        .value(row);
    let kind = match kind_str {
        "Gauge" => MetricKind::Gauge,
        "Sum" => MetricKind::Sum,
        "Histogram" => MetricKind::Histogram,
        "Summary" => MetricKind::Summary,
        _ => MetricKind::Gauge,
    };

    let resource_attr_arr = batch
        .column(10)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| Error::Arrow("Invalid resource_attributes column".into()))?;
    let resource_attr_json = resource_attr_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid resource_attributes values".into()))?
        .value(resource_attr_arr.keys().value(row) as usize);
    let resource_attributes = LabelSet::from_json(resource_attr_json)?;
    let correlation = row_to_correlation(batch, row, 3);
    let resource_attributes = inject_correlation(resource_attributes, correlation);

    let labels_json = batch
        .column(11)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid labels column".into()))?
        .value(row);
    let labels = LabelSet::from_json(labels_json)?;

    let value_float = batch
        .column(12)
        .as_any()
        .downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::Arrow("Invalid value_float column".into()))?;
    let value_int = batch
        .column(13)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Arrow("Invalid value_int column".into()))?;
    let value_complex = batch
        .column(14)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid value_complex column".into()))?;

    let value = if !value_float.is_null(row) {
        MetricValue::Double(value_float.value(row))
    } else if !value_int.is_null(row) {
        MetricValue::Int(value_int.value(row))
    } else {
        let json = value_complex.value(row);
        serde_json::from_str(json).map_err(Error::Serde)?
    };

    Ok((
        metric_name,
        kind,
        resource_attributes,
        DataPoint::new(timestamp_ns, value, labels)?,
    ))
}

/// Reads a single row from an Arrow [RecordBatch] back into a [LogRecord].
///
/// `attr_cache` / `res_cache` map raw attribute-JSON text to parsed [LabelSet]s.
/// Attribute sets repeat once per series across many rows, so caching removes
/// a serde_json round-trip per repeated row. Cache keys borrow from `batch`
/// arrays — callers must clear or recreate the caches per batch.
pub fn row_to_log<'a>(
    batch: &'a RecordBatch,
    row: usize,
    attr_cache: &mut HashMap<&'a str, LabelSet>,
    res_cache: &mut HashMap<&'a str, LabelSet>,
) -> Result<LogRecord> {
    let timestamp_ns = batch
        .column(0)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?
        .value(row);
    let observed_timestamp_ns = batch
        .column(1)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| Error::Arrow("Invalid observed_timestamp column".into()))?
        .value(row);
    let severity_number = batch
        .column(2)
        .as_any()
        .downcast_ref::<Int32Array>()
        .ok_or_else(|| Error::Arrow("Invalid severity_number column".into()))?
        .value(row);

    let severity_text_arr = batch
        .column(3)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| Error::Arrow("Invalid severity_text column".into()))?;
    let severity_text = severity_text_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid severity_text values".into()))?
        .value(severity_text_arr.keys().value(row) as usize)
        .to_string();

    let body = batch
        .column(4)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid body column".into()))?
        .value(row)
        .to_string();

    let res_json = batch
        .column(18)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid resource_attributes column".into()))?
        .value(row);
    let resource_attributes = match res_cache.get(res_json) {
        Some(l) => l.clone(),
        None => {
            let l = LabelSet::from_json(res_json)?;
            res_cache.insert(res_json, l.clone());
            l
        }
    };
    let correlation = row_to_correlation(batch, row, 5);
    let resource_attributes = inject_correlation(resource_attributes, correlation);

    let attr_json = batch
        .column(17)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid attributes column".into()))?
        .value(row);
    let attributes = match attr_cache.get(attr_json) {
        Some(l) => l.clone(),
        None => {
            let l = LabelSet::from_json(attr_json)?;
            attr_cache.insert(attr_json, l.clone());
            l
        }
    };

    let trace_id_arr = batch
        .column(12)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| Error::Arrow("Invalid trace_id column".into()))?;
    let mut trace_id = [0u8; 16];
    if !trace_id_arr.is_null(row) {
        let val = trace_id_arr.value(row);
        let len = val.len().min(16);
        trace_id[..len].copy_from_slice(&val[..len]);
    }

    let span_id_arr = batch
        .column(13)
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| Error::Arrow("Invalid span_id column".into()))?;
    let mut span_id = [0u8; 8];
    if !span_id_arr.is_null(row) {
        let val = span_id_arr.value(row);
        let len = val.len().min(8);
        span_id[..len].copy_from_slice(&val[..len]);
    }

    let flags = batch
        .column(14)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| Error::Arrow("Invalid flags column".into()))?
        .value(row);
    let scope_name = batch
        .column(15)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid scope_name column".into()))?
        .value(row)
        .to_string();
    let scope_version = batch
        .column(16)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid scope_version column".into()))?
        .value(row)
        .to_string();

    Ok(LogRecord::new(
        timestamp_ns,
        observed_timestamp_ns,
        severity_number,
        severity_text,
        body,
        attributes,
        resource_attributes,
        trace_id,
        span_id,
        flags,
        scope_name,
        scope_version,
    ))
}

/// Reads a single row from an Arrow [RecordBatch] back into a [Span].
pub fn row_to_span(batch: &RecordBatch, row: usize) -> Result<Span> {
    // Column 1: span_id (FixedSizeBinary(8) or Binary)
    let mut span_id = [0u8; 8];
    read_binary_field(batch.column(1).as_ref(), row, &mut span_id)?;

    // Column 2: span_name (Dictionary)
    let span_name_arr = batch
        .column(2)
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .ok_or_else(|| Error::Arrow("Invalid span_name column".into()))?;
    let name = span_name_arr
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid span_name values".into()))?
        .value(span_name_arr.keys().value(row) as usize)
        .to_string();

    // Column 3: span_kind (Utf8)
    let kind_str = batch
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid span_kind column".into()))?
        .value(row);
    let kind = match kind_str {
        "SPAN_KIND_INTERNAL" => 1,
        "SPAN_KIND_SERVER" => 2,
        "SPAN_KIND_CLIENT" => 3,
        "SPAN_KIND_PRODUCER" => 4,
        "SPAN_KIND_CONSUMER" => 5,
        _ => 0,
    };

    // Columns 4,5: start_time_ns, end_time_ns
    let start_time_ns = batch
        .column(4)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| Error::Arrow("Invalid start_time_ns column".into()))?
        .value(row);
    let end_time_ns = batch
        .column(5)
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .ok_or_else(|| Error::Arrow("Invalid end_time_ns column".into()))?
        .value(row);

    // Column 7: status_code (Utf8)
    let status_code_str = batch
        .column(7)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid status_code column".into()))?
        .value(row);
    let status_code = match status_code_str {
        "STATUS_CODE_OK" => 1,
        "STATUS_CODE_ERROR" => 2,
        _ => 0,
    };

    // Column 8: status_message (Utf8, nullable)
    let status_message = batch
        .column(8)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid status_message column".into()))?;
    let status_msg = if status_message.is_null(row) {
        String::new()
    } else {
        status_message.value(row).to_string()
    };

    // Column 16: trace_id (FixedSizeBinary(16) or Binary)
    let mut trace_id = [0u8; 16];
    read_binary_field(batch.column(16).as_ref(), row, &mut trace_id)?;

    // Column 17: parent_span_id (FixedSizeBinary(8) or Binary, nullable)
    let mut parent_span_id = [0u8; 8];
    if !batch.column(17).is_null(row) {
        read_binary_field(batch.column(17).as_ref(), row, &mut parent_span_id)?;
    }

    // Column 18: flags (UInt32, nullable)
    let flags_arr = batch
        .column(18)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| Error::Arrow("Invalid flags column".into()))?;
    let flags = if flags_arr.is_null(row) {
        0
    } else {
        flags_arr.value(row)
    };

    // Column 19: trace_state (Utf8, nullable)
    let trace_state_arr = batch
        .column(19)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid trace_state column".into()))?;
    let trace_state = if trace_state_arr.is_null(row) {
        String::new()
    } else {
        trace_state_arr.value(row).to_string()
    };

    // Column 20: attributes (Utf8 JSON)
    let attributes = LabelSet::from_json(
        batch
            .column(20)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Arrow("Invalid attributes column".into()))?
            .value(row),
    )?;

    // Column 22: events (Utf8 JSON)
    let events_json = batch
        .column(22)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid events column".into()))?
        .value(row);
    let events: Vec<SpanEvent> = serde_json::from_str(events_json).unwrap_or_default();

    // Column 23: links (Utf8 JSON)
    let links_json = batch
        .column(23)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| Error::Arrow("Invalid links column".into()))?
        .value(row);
    let links: Vec<SpanLink> = serde_json::from_str(links_json).unwrap_or_default();

    // Correlation columns 9-15 → inject back into attributes
    let correlation = row_to_correlation(batch, row, 9);
    let attributes = inject_correlation(attributes, correlation);

    Ok(Span::new(
        trace_id,
        span_id,
        trace_state,
        name,
        kind,
        start_time_ns,
        end_time_ns,
        attributes,
        events,
        links,
        SpanStatus {
            code: status_code,
            message: status_msg,
        },
        parent_span_id,
        flags,
    ))
}

/// Reads a binary field that may be either FixedSizeBinaryArray or BinaryArray<i32>.
fn read_binary_field(arr: &dyn Array, row: usize, out: &mut [u8]) -> Result<()> {
    if let Some(fixed) = arr.as_any().downcast_ref::<FixedSizeBinaryArray>() {
        let val = fixed.value(row);
        let len = val.len().min(out.len());
        out[..len].copy_from_slice(&val[..len]);
    } else if let Some(var) = arr.as_any().downcast_ref::<BinaryArray>() {
        let val = var.value(row);
        let len = val.len().min(out.len());
        out[..len].copy_from_slice(&val[..len]);
    } else {
        return Err(Error::Arrow("Invalid binary column type".into()));
    }
    Ok(())
}
