pub mod compactor;
pub mod index;
pub mod retention;
pub mod scanner;

pub use compactor::Compactor;
pub use index::BlockIndex;
pub use retention::RetentionPolicy;
pub use scanner::{LogRowFilter, LogScanStats, Scanner};

use crate::config::BlockConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Starts background maintenance tasks.
///
/// `retention_interval_secs` controls the sweep cadence for time-based
/// expiry. It is a parameter rather than a literal because the sweep holds the
/// block-index write lock while deleting files, so its cost is exactly the kind
/// of thing an operator on a busy cluster needs to tune.
pub fn start_maintenance(
    index: Arc<RwLock<BlockIndex>>,
    config: BlockConfig,
    retention_interval_secs: u64,
) {
    tokio::spawn(Compactor::run_loop(index.clone(), config.clone()));
    tokio::spawn(RetentionPolicy::run_loop(
        index,
        config,
        retention_interval_secs,
    ));
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

    /// Helper: write a real parquet metrics file with an explicit row-group
    /// size, so tests can create blocks that span several row groups.
    fn write_metrics_parquet_rg(path: &std::path::Path, metrics: &[Metric], rg_rows: usize) {
        let chunk = StorageModel::metrics_to_chunk(metrics).unwrap();
        let file = fs::File::create(path).unwrap();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_row_count(Some(rg_rows))
            .build();

        let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
        writer.write(&chunk).unwrap();
        writer.close().unwrap();
    }

    /// Builds a metrics block of `rows` points, 1 ns apart, in 10 series.
    fn prune_test_metrics(rows: usize) -> Vec<Metric> {
        vec![Metric {
            name: "prune.cpu".into(),
            kind: MetricKind::Gauge,
            data_points: (0..rows)
                .map(|i| {
                    DataPoint::new(
                        i as i64 + 1,
                        MetricValue::Double(i as f64),
                        LabelSet::try_from_iter(vec![("host", format!("h{}", i % 10))]).unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        }]
    }

    /// The pruning helper must select exactly the row groups that can hold a
    /// row in range, and must fail open (never prune) when it cannot tell.
    #[test]
    fn test_row_groups_in_range_selects_only_overlapping_groups() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prune.parquet");
        // 1000 rows over 10 row groups of 100, timestamps 1..=1000.
        write_metrics_parquet_rg(&path, &prune_test_metrics(1000), 100);

        let file = fs::File::open(&path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let md = builder.metadata().clone();

        assert_eq!(md.num_row_groups(), 10, "test file should have 10 groups");

        // Rows 301..=400 live entirely in the 4th row group (0-based index 3).
        let groups = scanner::row_groups_in_range(&md, "timestamp_ns", 301, 400).unwrap();
        assert_eq!(groups, vec![3]);

        // A single row on a boundary must still keep its own group.
        assert_eq!(
            scanner::row_groups_in_range(&md, "timestamp_ns", 100, 100).unwrap(),
            vec![0]
        );
        assert_eq!(
            scanner::row_groups_in_range(&md, "timestamp_ns", 101, 101).unwrap(),
            vec![1]
        );

        // A window covering everything keeps every group.
        assert_eq!(
            scanner::row_groups_in_range(&md, "timestamp_ns", 0, i64::MAX)
                .unwrap()
                .len(),
            10
        );

        // A window past the end prunes everything.
        assert!(
            scanner::row_groups_in_range(&md, "timestamp_ns", 5_000, 6_000)
                .unwrap()
                .is_empty()
        );

        // Unknown column: caller must fall back to reading every row group.
        assert!(scanner::row_groups_in_range(&md, "not_a_column", 0, 10).is_none());
    }

    /// Pruning must not change results: a narrow scan has to return exactly the
    /// rows a full scan filtered by hand would.
    #[tokio::test]
    async fn test_pruned_scan_matches_full_scan() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("prune_scan.parquet");
        write_metrics_parquet_rg(&path, &prune_test_metrics(1000), 100);

        let meta = BlockMetadata {
            path,
            start_timestamp_ns: 1,
            end_timestamp_ns: 1000,
            row_count: 1000,
            size_bytes: 0,
            metric_names: HashSet::from(["prune.cpu".into()]),
            label_names: HashSet::from(["host".into()]),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        };

        let all = Scanner::scan(vec![meta.clone()], "prune.cpu".into(), 0, i64::MAX)
            .await
            .unwrap();
        assert_eq!(all.len(), 1000);

        let expected: Vec<i64> = all
            .iter()
            .map(|p| p.timestamp_ns)
            .filter(|t| (301..=400).contains(t))
            .collect();

        // Picks row group 3 only, and must agree row for row.
        let pruned = Scanner::scan(vec![meta], "prune.cpu".into(), 301, 400)
            .await
            .unwrap();
        let got: Vec<i64> = pruned.iter().map(|p| p.timestamp_ns).collect();
        assert_eq!(got, expected);
        assert_eq!(got.len(), 100);
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
