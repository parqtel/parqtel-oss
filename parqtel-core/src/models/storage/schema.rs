use arrow_schema::{DataType, Field, Schema, TimeUnit};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::PathBuf;

/// Metadata about a written Parquet block.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BlockMetadata {
    pub path: PathBuf,
    pub start_timestamp_ns: i64,
    pub end_timestamp_ns: i64,
    pub row_count: usize,
    pub size_bytes: u64,
    pub metric_names: HashSet<String>,
    #[serde(default)]
    pub label_names: HashSet<String>,
    /// Per-field distinct values collected at flush time (G5 label-value
    /// index). Capped per field at flush to bound memory; merged on read.
    #[serde(default)]
    pub label_values: BTreeMap<String, BTreeSet<String>>,
    #[serde(default)]
    pub signal_type: SignalType,
}

/// Supported signal types in parqtel.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SignalType {
    #[default]
    Metrics,
    Logs,
    Traces,
}

/// Returns the canonical Arrow [Schema] for metric storage.
pub fn metrics_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "metric_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("metric_kind", DataType::Utf8, false),
        Field::new(
            "service_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "service_version",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_namespace",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_pod_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_pod_uid",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_container_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_node_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "resource_attributes",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("labels", DataType::Utf8, false),
        Field::new("value_float", DataType::Float64, true),
        Field::new("value_int", DataType::Int64, true),
        Field::new("value_complex", DataType::Utf8, true),
    ])
}

/// Returns the canonical Arrow [Schema] for log storage.
pub fn logs_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "observed_timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("severity_number", DataType::Int32, false),
        Field::new(
            "severity_text",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("body", DataType::Utf8, false),
        Field::new(
            "service_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "service_version",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_namespace",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_pod_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_pod_uid",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_container_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_node_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("trace_id", DataType::FixedSizeBinary(16), true),
        Field::new("span_id", DataType::FixedSizeBinary(8), true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
    ])
}

/// Returns the canonical Arrow [Schema] for trace storage.
pub fn traces_schema() -> Schema {
    Schema::new(vec![
        Field::new(
            "timestamp_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("span_id", DataType::FixedSizeBinary(8), false),
        Field::new(
            "span_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("span_kind", DataType::Utf8, false),
        Field::new(
            "start_time_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "end_time_ns",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("duration_ns", DataType::Int64, false),
        Field::new("status_code", DataType::Utf8, false),
        Field::new("status_message", DataType::Utf8, true),
        Field::new(
            "service_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "service_version",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_namespace",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_pod_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_pod_uid",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_container_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new(
            "k8s_node_name",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            true,
        ),
        Field::new("trace_id", DataType::FixedSizeBinary(16), false),
        Field::new("parent_span_id", DataType::FixedSizeBinary(8), true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("trace_state", DataType::Utf8, true),
        Field::new("attributes", DataType::Utf8, false),
        Field::new("resource_attributes", DataType::Utf8, false),
        Field::new("events", DataType::Utf8, false),
        Field::new("links", DataType::Utf8, false),
    ])
}
