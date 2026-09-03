pub mod compactor;
pub mod index;
pub mod retention;
pub mod scanner;

pub use compactor::Compactor;
pub use index::BlockIndex;
pub use retention::RetentionPolicy;
pub use scanner::Scanner;

use crate::config::BlockConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Starts background maintenance tasks.
pub fn start_maintenance(index: Arc<RwLock<BlockIndex>>, config: BlockConfig) {
    tokio::spawn(Compactor::run_loop(index.clone(), config.clone()));
    tokio::spawn(RetentionPolicy::run_loop(index, config));
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::models::storage::{BlockMetadata, SignalType, StorageModel};
    use crate::models::{DataPoint, LabelSet, LogRecord, Metric, MetricKind, MetricValue};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use std::collections::HashSet;
    use std::fs;
    use tempfile::tempdir;

    /// Helper: write a real parquet metrics file using new parquet/arrow APIs.
    fn write_metrics_parquet(path: &std::path::Path, metrics: &[Metric]) {
        let chunk = StorageModel::metrics_to_chunk(metrics).unwrap();
        let file = fs::File::create(path).unwrap();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();

        let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
        writer.write(&chunk).unwrap();
        writer.close().unwrap();
    }

    /// Helper: write a real parquet logs file.
    fn write_logs_parquet(path: &std::path::Path, logs: &[LogRecord]) {
        let chunk = StorageModel::logs_to_chunk(logs).unwrap();
        let file = fs::File::create(path).unwrap();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();

        let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
        writer.write(&chunk).unwrap();
        writer.close().unwrap();
    }

    #[tokio::test]
    async fn test_scanner_missing_file() {
        let dir = tempdir().unwrap();
        let b1 = BlockMetadata {
            path: dir.path().join("missing.parquet"),
            start_timestamp_ns: 100,
            end_timestamp_ns: 200,
            row_count: 10,
            size_bytes: 100,
            metric_names: HashSet::from(["m1".into()]),
            label_names: HashSet::from(["l1".into()]),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        };
        let results = Scanner::scan(vec![b1], "m1".into(), 0, 300).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_scanner_logs_missing_file() {
        let dir = tempdir().unwrap();
        let b1 = BlockMetadata {
            path: dir.path().join("missing_logs.parquet"),
            start_timestamp_ns: 100,
            end_timestamp_ns: 200,
            row_count: 10,
            size_bytes: 100,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Logs,
        };
        let results = Scanner::scan_logs(vec![b1], 0, 300).await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_index_persistence() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        index
            .add(BlockMetadata {
                path: dir.path().join("b1.parquet"),
                start_timestamp_ns: 100,
                end_timestamp_ns: 200,
                row_count: 10,
                size_bytes: 100,
                metric_names: HashSet::from(["m1".into()]),
                label_names: HashSet::from(["l1".into()]),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();

        let mut index2 = BlockIndex::new(dir.path());
        index2.load().unwrap();
        assert_eq!(index2.total_blocks(), 1);
    }

    #[tokio::test]
    async fn test_index_query_time_range() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        index
            .add(BlockMetadata {
                path: dir.path().join("b1.parquet"),
                start_timestamp_ns: 100,
                end_timestamp_ns: 200,
                row_count: 10,
                size_bytes: 100,
                metric_names: HashSet::from(["m1".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();
        index
            .add(BlockMetadata {
                path: dir.path().join("b2.parquet"),
                start_timestamp_ns: 300,
                end_timestamp_ns: 400,
                row_count: 10,
                size_bytes: 100,
                metric_names: HashSet::from(["m2".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();

        assert_eq!(index.query(150, 250, None).len(), 1);
        assert_eq!(index.query(0, 500, None).len(), 2);
        assert_eq!(index.query(0, 500, Some("m1")).len(), 1);
    }

    #[tokio::test]
    async fn test_index_remove() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        let path = dir.path().join("b1.parquet");
        index
            .add(BlockMetadata {
                path: path.clone(),
                start_timestamp_ns: 100,
                end_timestamp_ns: 200,
                row_count: 10,
                size_bytes: 100,
                metric_names: HashSet::from(["m1".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();
        assert_eq!(index.total_blocks(), 1);
        index.remove(&path).unwrap();
        assert_eq!(index.total_blocks(), 0);
    }

    #[tokio::test]
    async fn test_index_stats() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        index
            .add(BlockMetadata {
                path: dir.path().join("b1.parquet"),
                start_timestamp_ns: 100,
                end_timestamp_ns: 200,
                row_count: 10,
                size_bytes: 500,
                metric_names: HashSet::from(["m1".into(), "m2".into()]),
                label_names: HashSet::from(["env".into()]),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();
        assert_eq!(index.total_rows(), 10);
        assert_eq!(index.total_bytes(), 500);
        assert_eq!(index.all_metrics().len(), 2);
        assert_eq!(index.all_labels().len(), 1);
    }

    #[tokio::test]
    async fn test_scanner_reads_real_metrics() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.parquet");
        let m = Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(
                    1000,
                    MetricValue::Double(10.0),
                    LabelSet::try_from_iter(vec![("host", "h1")]).unwrap(),
                )
                .unwrap(),
                DataPoint::new(
                    2000,
                    MetricValue::Double(20.0),
                    LabelSet::try_from_iter(vec![("host", "h2")]).unwrap(),
                )
                .unwrap(),
            ],
            ..Default::default()
        };
        write_metrics_parquet(&path, &[m]);

        let meta = BlockMetadata {
            path,
            start_timestamp_ns: 1000,
            end_timestamp_ns: 2000,
            row_count: 2,
            size_bytes: 100,
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::from(["host".into()]),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        };
        let points = Scanner::scan(vec![meta], "cpu".into(), 0, 3000)
            .await
            .unwrap();
        assert_eq!(points.len(), 2);
    }

    #[tokio::test]
    async fn test_scanner_reads_real_logs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("logs.parquet");
        let log = LogRecord::new(
            1000,
            1000,
            9,
            "INFO".into(),
            "hello".into(),
            LabelSet::try_from_iter(vec![("k", "v")]).unwrap(),
            LabelSet::default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        );
        write_logs_parquet(&path, &[log]);

        let meta = BlockMetadata {
            path,
            start_timestamp_ns: 1000,
            end_timestamp_ns: 1000,
            row_count: 1,
            size_bytes: 100,
            metric_names: HashSet::new(),
            label_names: HashSet::from(["k".into()]),
            label_values: Default::default(),
            signal_type: SignalType::Logs,
        };
        let logs = Scanner::scan_logs(vec![meta], 0, 2000).await.unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].body, "hello");
    }

    #[tokio::test]
    async fn test_compactor_merges_small_blocks() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            ..Default::default()
        };

        let p1 = dir.path().join("b1.parquet");
        let p2 = dir.path().join("b2.parquet");
        let m1 = Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(1000, MetricValue::Double(10.0), LabelSet::default()).unwrap(),
            ],
            ..Default::default()
        };
        let m2 = Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(2000, MetricValue::Double(20.0), LabelSet::default()).unwrap(),
            ],
            ..Default::default()
        };
        write_metrics_parquet(&p1, &[m1]);
        write_metrics_parquet(&p2, &[m2]);

        let mut index = BlockIndex::new(dir.path());
        index
            .add(BlockMetadata {
                path: p1.clone(),
                start_timestamp_ns: 1000,
                end_timestamp_ns: 1000,
                row_count: 1,
                size_bytes: fs::metadata(&p1).unwrap().len(),
                metric_names: HashSet::from(["cpu".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();
        index
            .add(BlockMetadata {
                path: p2.clone(),
                start_timestamp_ns: 2000,
                end_timestamp_ns: 2000,
                row_count: 1,
                size_bytes: fs::metadata(&p2).unwrap().len(),
                metric_names: HashSet::from(["cpu".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();

        let index = Arc::new(RwLock::new(index));
        Compactor::compact_once(&index, &config).await.unwrap();

        let idx = index.read().await;
        assert_eq!(idx.total_blocks(), 1);
        assert_eq!(idx.blocks[0].row_count, 2);
        assert!(!p1.exists());
        assert!(!p2.exists());
    }

    #[tokio::test]
    async fn test_retention_policy_deletes_expired_blocks() {
        let dir = tempdir().unwrap();
        let p1 = dir.path().join("old.parquet");
        let p2 = dir.path().join("new.parquet");
        fs::write(&p1, b"old").unwrap();
        fs::write(&p2, b"new").unwrap();

        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let old_ns = now_ns - (10 * 24 * 3600 * 1_000_000_000);

        let mut index = BlockIndex::new(dir.path());
        index
            .add(BlockMetadata {
                path: p1.clone(),
                start_timestamp_ns: old_ns - 1000,
                end_timestamp_ns: old_ns,
                row_count: 5,
                size_bytes: 100,
                metric_names: HashSet::from(["cpu".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();
        index
            .add(BlockMetadata {
                path: p2.clone(),
                start_timestamp_ns: now_ns - 1000,
                end_timestamp_ns: now_ns,
                row_count: 5,
                size_bytes: 100,
                metric_names: HashSet::from(["cpu".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            })
            .unwrap();

        let index = Arc::new(RwLock::new(index));
        RetentionPolicy::enforce(&index, 7).await.unwrap();

        let idx = index.read().await;
        assert_eq!(idx.total_blocks(), 1);
        assert!(!p1.exists());
        assert!(p2.exists());
    }
}
