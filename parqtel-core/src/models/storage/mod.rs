pub(crate) mod correlation;
mod reader;
mod schema;
mod writer;

pub use schema::{logs_schema, metrics_schema, traces_schema, BlockMetadata, SignalType};

use crate::error::Result;
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric, MetricKind};
use crate::models::traces::Span;
use arrow2::array::Array;
use arrow2::chunk::Chunk;
use arrow2::datatypes::Schema;
use std::sync::Arc;

/// Facade that delegates to schema/writer/reader submodules.
pub struct StorageModel;

impl StorageModel {
    pub fn metrics_schema() -> Schema {
        schema::metrics_schema()
    }
    pub fn logs_schema() -> Schema {
        schema::logs_schema()
    }
    pub fn traces_schema() -> Schema {
        schema::traces_schema()
    }
    pub fn schema() -> Schema {
        schema::metrics_schema()
    }

    pub fn metrics_to_chunk(metrics: &[Metric]) -> Result<Chunk<Arc<dyn Array>>> {
        writer::metrics_to_chunk(metrics)
    }
    pub fn logs_to_chunk(logs: &[LogRecord]) -> Result<Chunk<Arc<dyn Array>>> {
        writer::logs_to_chunk(logs)
    }
    pub fn traces_to_chunk(spans: &[Span]) -> Result<Chunk<Arc<dyn Array>>> {
        writer::traces_to_chunk(spans)
    }

    pub fn row_to_point<A: AsRef<dyn Array>>(
        chunk: &Chunk<A>,
        row: usize,
    ) -> Result<(String, MetricKind, LabelSet, DataPoint)> {
        reader::row_to_point(chunk, row)
    }
    pub fn row_to_log<'a, A: AsRef<dyn Array>>(
        chunk: &'a Chunk<A>,
        row: usize,
        attr_cache: &mut std::collections::HashMap<&'a str, crate::LabelSet>,
        res_cache: &mut std::collections::HashMap<&'a str, crate::LabelSet>,
    ) -> Result<LogRecord> {
        reader::row_to_log(chunk, row, attr_cache, res_cache)
    }
    pub fn row_to_span<A: AsRef<dyn Array>>(chunk: &Chunk<A>, row: usize) -> Result<Span> {
        reader::row_to_span(chunk, row)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::models::metrics::{MetricKind, MetricValue};
    use crate::models::traces::SpanStatus;

    #[test]
    fn test_metrics_schema_definition() {
        assert_eq!(StorageModel::metrics_schema().fields.len(), 15);
    }

    #[test]
    fn test_logs_schema_definition() {
        assert_eq!(StorageModel::logs_schema().fields.len(), 19);
    }

    #[test]
    fn test_traces_schema_definition() {
        assert_eq!(StorageModel::traces_schema().fields.len(), 24);
    }

    #[test]
    fn test_metrics_roundtrip() {
        let metric = Metric {
            name: "test_metric".into(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::try_from_iter(vec![
                ("host", "localhost"),
                ("service.name", "test-svc"),
            ])
            .unwrap_or_default(),
            data_points: vec![DataPoint::new(
                100,
                MetricValue::Double(42.0),
                LabelSet::try_from_iter(vec![("env", "prod")]).unwrap_or_default(),
            )
            .unwrap()],
            ..Default::default()
        };
        let chunk = StorageModel::metrics_to_chunk(&[metric]).unwrap();
        let (name, kind, res, dp) = StorageModel::row_to_point(&chunk, 0).unwrap();
        assert_eq!(name, "test_metric");
        assert_eq!(kind, MetricKind::Gauge);
        assert_eq!(res.get("service.name"), Some("test-svc"));
        assert_eq!(dp.timestamp_ns, 100);
    }

    #[test]
    fn test_traces_to_chunk() {
        let span = Span::new(
            [1; 16],
            [1; 8],
            "".into(),
            "test-span".into(),
            2,
            1000,
            2000,
            LabelSet::default(),
            vec![],
            vec![],
            SpanStatus {
                code: 1,
                message: "OK".into(),
            },
            [0; 8],
            0,
        );
        let chunk = StorageModel::traces_to_chunk(&[span]).unwrap();
        assert_eq!(chunk.len(), 1);
    }

    #[test]
    fn test_traces_roundtrip() {
        let span = Span::new(
            [1; 16],
            [2; 8],
            "state".into(),
            "my-span".into(),
            2,
            1000,
            2000,
            LabelSet::try_from_iter(vec![("key", "val")]).unwrap_or_default(),
            vec![],
            vec![],
            SpanStatus {
                code: 1,
                message: "OK".into(),
            },
            [3; 8],
            5,
        );
        let chunk = StorageModel::traces_to_chunk(&[span.clone()]).unwrap();
        let decoded = StorageModel::row_to_span(&chunk, 0).unwrap();
        assert_eq!(decoded.trace_id, [1; 16]);
        assert_eq!(decoded.span_id, [2; 8]);
        assert_eq!(decoded.parent_span_id, [3; 8]);
        assert_eq!(decoded.name, "my-span");
        assert_eq!(decoded.kind, 2);
        assert_eq!(decoded.start_time_ns, 1000);
        assert_eq!(decoded.end_time_ns, 2000);
        assert_eq!(decoded.status.code, 1);
        assert_eq!(decoded.status.message, "OK");
        assert_eq!(decoded.flags, 5);
        assert_eq!(decoded.trace_state, "state");
        assert_eq!(decoded.attributes.get("key"), Some("val"));
    }

    #[test]
    fn test_logs_to_chunk() {
        let log = LogRecord::new(
            5000,
            5001,
            9,
            "INFO".into(),
            "hello".into(),
            LabelSet::default(),
            LabelSet::try_from_iter(vec![("service.name", "web")]).unwrap_or_default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "scope".into(),
            "1.0".into(),
        );
        let chunk = StorageModel::logs_to_chunk(&[log]).unwrap();
        assert_eq!(chunk.len(), 1);
        assert_eq!(
            chunk.arrays().len(),
            StorageModel::logs_schema().fields.len()
        );
    }

    #[test]
    fn test_block_metadata_serialization() {
        let meta = BlockMetadata {
            path: std::path::PathBuf::from("/data/block1.parquet"),
            start_timestamp_ns: 1000,
            end_timestamp_ns: 2000,
            row_count: 100,
            size_bytes: 4096,
            metric_names: std::collections::HashSet::from(["cpu".into()]),
            label_names: std::collections::HashSet::from(["host".into()]),
            signal_type: SignalType::Metrics,
        };
        let json = serde_json::to_string(&meta).unwrap();
        let decoded: BlockMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.row_count, 100);
    }
}
