use arrow2::array::{Array, DictionaryArray, Int64Array, Float64Array, Int32Array, Utf8Array, PrimitiveArray, FixedSizeBinaryArray};
use arrow2::chunk::Chunk;
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::metrics::{DataPoint, MetricKind, MetricValue};
use crate::models::logs::LogRecord;
use super::correlation::{row_to_correlation, inject_correlation};

/// Reads a single row from an Arrow [Chunk] back into a [DataPoint] and its metric metadata.
pub fn row_to_point<A: AsRef<dyn Array>>(chunk: &Chunk<A>, row: usize) -> Result<(String, MetricKind, LabelSet, DataPoint)> {
    let timestamp_ns = chunk.arrays()[0].as_ref().as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?
        .value(row);

    let metric_name_arr = chunk.arrays()[1].as_ref().as_any().downcast_ref::<DictionaryArray<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid metric_name column".into()))?;
    let metric_name = metric_name_arr.values().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid metric_name values".into()))?
        .value(metric_name_arr.keys().value(row) as usize)
        .to_string();

    let kind_str = chunk.arrays()[2].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid metric_kind column".into()))?
        .value(row);
    let kind = match kind_str {
        "Gauge" => MetricKind::Gauge,
        "Sum" => MetricKind::Sum,
        "Histogram" => MetricKind::Histogram,
        "Summary" => MetricKind::Summary,
        _ => MetricKind::Gauge,
    };

    let resource_attr_arr = chunk.arrays()[10].as_ref().as_any().downcast_ref::<DictionaryArray<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid resource_attributes column".into()))?;
    let resource_attr_json = resource_attr_arr.values().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid resource_attributes values".into()))?
        .value(resource_attr_arr.keys().value(row) as usize);
    let resource_attributes = LabelSet::from_json(resource_attr_json)?;
    let correlation = row_to_correlation(chunk, row, 3);
    let resource_attributes = inject_correlation(resource_attributes, correlation);

    let labels_json = chunk.arrays()[11].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid labels column".into()))?
        .value(row);
    let labels = LabelSet::from_json(labels_json)?;

    let value_float = chunk.arrays()[12].as_ref().as_any().downcast_ref::<Float64Array>()
        .ok_or_else(|| Error::Arrow("Invalid value_float column".into()))?;
    let value_int = chunk.arrays()[13].as_ref().as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Arrow("Invalid value_int column".into()))?;
    let value_complex = chunk.arrays()[14].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid value_complex column".into()))?;

    let value = if !value_float.is_null(row) {
        MetricValue::Double(value_float.value(row))
    } else if !value_int.is_null(row) {
        MetricValue::Int(value_int.value(row))
    } else {
        let json = value_complex.value(row);
        serde_json::from_str(json).map_err(Error::Serde)?
    };

    Ok((metric_name, kind, resource_attributes, DataPoint::new(timestamp_ns, value, labels)?))
}

/// Reads a single row from an Arrow [Chunk] back into a [LogRecord].
pub fn row_to_log<A: AsRef<dyn Array>>(chunk: &Chunk<A>, row: usize) -> Result<LogRecord> {
    let timestamp_ns = chunk.arrays()[0].as_ref().as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Arrow("Invalid timestamp column".into()))?.value(row);
    let observed_timestamp_ns = chunk.arrays()[1].as_ref().as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| Error::Arrow("Invalid observed_timestamp column".into()))?.value(row);
    let severity_number = chunk.arrays()[2].as_ref().as_any().downcast_ref::<Int32Array>()
        .ok_or_else(|| Error::Arrow("Invalid severity_number column".into()))?.value(row);

    let severity_text_arr = chunk.arrays()[3].as_ref().as_any().downcast_ref::<DictionaryArray<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid severity_text column".into()))?;
    let severity_text = severity_text_arr.values().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid severity_text values".into()))?
        .value(severity_text_arr.keys().value(row) as usize).to_string();

    let body = chunk.arrays()[4].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid body column".into()))?.value(row).to_string();

    let resource_attributes = LabelSet::from_json(
        chunk.arrays()[18].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| Error::Arrow("Invalid resource_attributes column".into()))?.value(row)
    )?;
    let correlation = row_to_correlation(chunk, row, 5);
    let resource_attributes = inject_correlation(resource_attributes, correlation);

    let attributes = LabelSet::from_json(
        chunk.arrays()[17].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
            .ok_or_else(|| Error::Arrow("Invalid attributes column".into()))?.value(row)
    )?;

    let trace_id_arr = chunk.arrays()[12].as_ref().as_any().downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| Error::Arrow("Invalid trace_id column".into()))?;
    let mut trace_id = [0u8; 16];
    if !trace_id_arr.is_null(row) {
        let val = trace_id_arr.value(row);
        let len = val.len().min(16);
        trace_id[..len].copy_from_slice(&val[..len]);
    }

    let span_id_arr = chunk.arrays()[13].as_ref().as_any().downcast_ref::<FixedSizeBinaryArray>()
        .ok_or_else(|| Error::Arrow("Invalid span_id column".into()))?;
    let mut span_id = [0u8; 8];
    if !span_id_arr.is_null(row) {
        let val = span_id_arr.value(row);
        let len = val.len().min(8);
        span_id[..len].copy_from_slice(&val[..len]);
    }

    let flags = chunk.arrays()[14].as_ref().as_any().downcast_ref::<PrimitiveArray<u32>>()
        .ok_or_else(|| Error::Arrow("Invalid flags column".into()))?.value(row);
    let scope_name = chunk.arrays()[15].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid scope_name column".into()))?.value(row).to_string();
    let scope_version = chunk.arrays()[16].as_ref().as_any().downcast_ref::<Utf8Array<i32>>()
        .ok_or_else(|| Error::Arrow("Invalid scope_version column".into()))?.value(row).to_string();

    Ok(LogRecord::new(
        timestamp_ns, observed_timestamp_ns, severity_number, severity_text, body,
        attributes, resource_attributes, trace_id, span_id, flags, scope_name, scope_version
    ))
}
