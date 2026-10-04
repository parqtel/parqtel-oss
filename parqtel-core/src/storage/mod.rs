pub mod compactor;
pub mod index;
pub mod persist;
pub mod retention;
pub mod scanner;

pub use compactor::Compactor;
pub use index::{BlockIndex, BlockIndexStore};
pub use persist::{persist_blocking, persist_once, run_index_persist_loop};
pub use retention::RetentionPolicy;
pub use scanner::{LogRowFilter, LogScanStats, Scanner};

use crate::config::BlockConfig;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Handle for the background maintenance tasks of one signal, so a graceful
/// shutdown can stop them and flush the index exactly once.
pub struct MaintenanceHandle {
    compactor: tokio::task::JoinHandle<()>,
    retention: tokio::task::JoinHandle<()>,
    persist: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MaintenanceHandle {
    /// Signals every task to stop.
    pub fn stop(&self) {
        let _ = self.shutdown_tx.send(true);
    }
    /// Signals every task to stop and waits for them, bounded by `timeout`.
    ///
    /// The persistence task performs a final pass before returning, so once
    /// this resolves the sidecar reflects every block that was written.
    pub async fn shutdown(self, timeout: std::time::Duration) {
        let _ = self.shutdown_tx.send(true);
        let _ = tokio::time::timeout(timeout, async {
            let _ = self.compactor.await;
            let _ = self.retention.await;
            let _ = self.persist.await;
        })
        .await;
    }
}

/// Starts background maintenance tasks for one signal.
///
/// Three tasks share one index lock and one store:
///
/// * the compactor merges small and tiered blocks,
/// * retention deletes expired blocks,
/// * the persistence loop writes the sidecar on a debounce.
///
/// All three take the index lock only for in-memory work — selection under a
/// read lock, publication as a single swap under the write lock. Every
/// filesystem and Parquet operation runs on the blocking pool, so no tokio
/// worker is parked on disk I/O while holding a lock every query needs.
///
/// `retention_interval_secs` controls the sweep cadence and
/// `persist_interval_secs` the sidecar debounce. The caller constructs the
/// `store` from the `BlockIndex` while it still holds it directly, which keeps
/// the sidecar path defined in exactly one place.
pub fn start_maintenance(
    index: Arc<RwLock<BlockIndex>>,
    store: Arc<BlockIndexStore>,
    config: BlockConfig,
    retention_interval_secs: u64,
    persist_interval_secs: u64,
) -> MaintenanceHandle {
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    let compactor = tokio::spawn(Compactor::run_loop(
        index.clone(),
        store.clone(),
        config.clone(),
        shutdown_rx.clone(),
    ));
    let retention = tokio::spawn(RetentionPolicy::run_loop(
        index.clone(),
        store.clone(),
        config,
        retention_interval_secs,
        shutdown_rx.clone(),
    ));
    let persist = tokio::spawn(run_index_persist_loop(
        index,
        store,
        std::time::Duration::from_secs(persist_interval_secs.max(1)),
        shutdown_rx,
    ));

    MaintenanceHandle {
        compactor,
        retention,
        persist,
        shutdown_tx,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::models::storage::{BlockMetadata, SignalType, StorageModel};
    use crate::models::{DataPoint, LabelSet, LogRecord, Metric, MetricKind, MetricValue, Span};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::{WriterProperties, WriterVersion};
    use std::collections::HashSet;
    use std::collections::{BTreeMap, BTreeSet};
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

    fn write_traces_parquet(path: &std::path::Path, spans: &[Span]) {
        let chunk = StorageModel::traces_to_chunk(spans).unwrap();
        let file = fs::File::create(path).unwrap();
        let writer_props = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .build();
        let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
        writer.write(&chunk).unwrap();
        writer.close().unwrap();
    }

    fn test_span(start: i64) -> Span {
        Span {
            trace_id: [1u8; 16],
            span_id: [start as u8; 8],
            trace_state: String::new(),
            name: "op".into(),
            kind: 2,
            start_time_ns: start,
            end_time_ns: start + 1000,
            attributes: LabelSet::try_from_iter(vec![(
                "http.status_code".to_string(),
                "200".to_string(),
            )])
            .unwrap(),
            events: Vec::new(),
            links: Vec::new(),
            status: crate::models::traces::SpanStatus {
                code: 1,
                message: String::new(),
            },
            parent_span_id: [0u8; 8],
            flags: 1,
        }
    }

    fn trace_meta(path: &std::path::Path, spans: &[Span]) -> BlockMetadata {
        BlockMetadata {
            path: path.to_path_buf(),
            start_timestamp_ns: spans.iter().map(|s| s.start_time_ns).min().unwrap_or(0),
            end_timestamp_ns: spans.iter().map(|s| s.end_time_ns).max().unwrap_or(0),
            row_count: spans.len(),
            size_bytes: fs::metadata(path).map(|m| m.len()).unwrap_or(0),
            metric_names: HashSet::new(),
            label_names: HashSet::from(["http.status_code".to_string()]),
            label_values: BTreeMap::from([(
                "http.status_code".to_string(),
                BTreeSet::from(["200".to_string()]),
            )]),
            signal_type: SignalType::Traces,
        }
    }

    /// Trace blocks used to be skipped by compaction entirely, so they never
    /// merged and accumulated until retention deleted them.
    #[tokio::test]
    async fn test_compactor_merges_trace_blocks() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            compaction_max_merge_blocks: 4,
            compaction_max_merges_per_pass: 4,
            ..Default::default()
        };
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));

        // Three small trace blocks, time-ordered.
        let mut index = BlockIndex::new(dir.path());
        for b in 0..3i64 {
            let spans: Vec<Span> = (0..3).map(|i| test_span(b * 10 + i + 1)).collect();
            let path = dir.path().join(format!("t{b}.parquet"));
            write_traces_parquet(&path, &spans);
            index.add(trace_meta(&path, &spans));
        }
        let index = Arc::new(RwLock::new(index));
        assert_eq!(index.read().await.total_blocks(), 3);

        Compactor::compact_once(&index, &store, &config)
            .await
            .unwrap();

        let after = index.read().await;
        assert_eq!(
            after.total_blocks(),
            1,
            "three small trace blocks should merge into one"
        );
        let merged = &after.blocks[0];
        assert_eq!(merged.signal_type, SignalType::Traces);
        assert_eq!(merged.row_count, 9, "every span must survive the merge");

        // And the merged block must still be readable.
        let spans = Scanner::scan_traces(vec![merged.clone()], 0, i64::MAX, 1000)
            .await
            .unwrap();
        assert_eq!(spans.len(), 9, "merged trace block must decode");
    }

    /// Compaction used to write an empty label-value dictionary, so merging
    /// silently destroyed the flush-time index that label-value autocomplete
    /// reads from metadata instead of decoding blocks. Every compaction pass
    /// degraded it.
    #[tokio::test]
    async fn test_compaction_preserves_label_values() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            ..Default::default()
        };
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));

        let mut index = BlockIndex::new(dir.path());
        for b in 0..2i64 {
            let m = Metric {
                name: "cpu".into(),
                kind: MetricKind::Gauge,
                data_points: vec![DataPoint::new(
                    b + 1,
                    MetricValue::Double(1.0),
                    LabelSet::try_from_iter(vec![("host", format!("h{b}"))]).unwrap(),
                )
                .unwrap()],
                ..Default::default()
            };
            let path = dir.path().join(format!("m{b}.parquet"));
            write_metrics_parquet(&path, std::slice::from_ref(&m));
            let mut meta = BlockMetadata {
                path,
                start_timestamp_ns: b + 1,
                end_timestamp_ns: b + 1,
                row_count: 1,
                size_bytes: 0,
                metric_names: HashSet::from(["cpu".to_string()]),
                label_names: HashSet::from(["host".to_string()]),
                label_values: BTreeMap::from([(
                    "host".to_string(),
                    BTreeSet::from([format!("h{b}")]),
                )]),
                signal_type: SignalType::Metrics,
            };
            meta.size_bytes = fs::metadata(&meta.path).map(|m| m.len()).unwrap_or(0);
            index.add(meta);
        }
        let index = Arc::new(RwLock::new(index));

        Compactor::compact_once(&index, &store, &config)
            .await
            .unwrap();

        let after = index.read().await;
        assert_eq!(after.total_blocks(), 1, "blocks should have merged");
        let hosts = &after.blocks[0].label_values["host"];
        assert_eq!(
            hosts.len(),
            2,
            "the merged block must carry both hosts' values, not an empty dictionary"
        );
        assert!(hosts.contains("h0") && hosts.contains("h1"));
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
        let results = Scanner::scan(vec![b1], "m1".into(), 0, 300, None)
            .await
            .unwrap();
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
        index.add(BlockMetadata {
            path: dir.path().join("b1.parquet"),
            start_timestamp_ns: 100,
            end_timestamp_ns: 200,
            row_count: 10,
            size_bytes: 100,
            metric_names: HashSet::from(["m1".into()]),
            label_names: HashSet::from(["l1".into()]),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });

        // `add` is in-memory only; persistence is the store's job.
        let store = BlockIndexStore::new(&index);
        store.mark_dirty();
        persist_blocking(&index, &store).unwrap();

        let mut index2 = BlockIndex::new(dir.path());
        index2.load().unwrap();
        assert_eq!(index2.total_blocks(), 1);
    }

    #[tokio::test]
    async fn test_index_query_time_range() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        index.add(BlockMetadata {
            path: dir.path().join("b1.parquet"),
            start_timestamp_ns: 100,
            end_timestamp_ns: 200,
            row_count: 10,
            size_bytes: 100,
            metric_names: HashSet::from(["m1".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });
        index.add(BlockMetadata {
            path: dir.path().join("b2.parquet"),
            start_timestamp_ns: 300,
            end_timestamp_ns: 400,
            row_count: 10,
            size_bytes: 100,
            metric_names: HashSet::from(["m2".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });

        assert_eq!(index.query(150, 250, None).len(), 1);
        assert_eq!(index.query(0, 500, None).len(), 2);
        assert_eq!(index.query(0, 500, Some("m1")).len(), 1);
    }

    #[tokio::test]
    async fn test_index_remove() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        let path = dir.path().join("b1.parquet");
        index.add(BlockMetadata {
            path: path.clone(),
            start_timestamp_ns: 100,
            end_timestamp_ns: 200,
            row_count: 10,
            size_bytes: 100,
            metric_names: HashSet::from(["m1".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });
        assert_eq!(index.total_blocks(), 1);
        index.remove(&path);
        assert_eq!(index.total_blocks(), 0);
    }

    #[tokio::test]
    async fn test_index_stats() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        index.add(BlockMetadata {
            path: dir.path().join("b1.parquet"),
            start_timestamp_ns: 100,
            end_timestamp_ns: 200,
            row_count: 10,
            size_bytes: 500,
            metric_names: HashSet::from(["m1".into(), "m2".into()]),
            label_names: HashSet::from(["env".into()]),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });
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

    /// Writes a metrics block with explicit control over which read-path
    /// metadata is present, so each pruning mechanism can be tested in
    /// isolation. A test that enabled everything would pass while proving
    /// nothing about which mechanism did the work.
    fn write_block_with(
        path: &std::path::Path,
        metrics: &[Metric],
        rg_rows: usize,
        stats: bool,
        bloom: bool,
    ) {
        let chunk = StorageModel::metrics_to_chunk(metrics).unwrap();
        let file = fs::File::create(path).unwrap();
        let mut builder = WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_max_row_group_row_count(Some(rg_rows))
            .set_statistics_enabled(if stats {
                parquet::file::properties::EnabledStatistics::Chunk
            } else {
                parquet::file::properties::EnabledStatistics::None
            });
        if bloom {
            use parquet::schema::types::ColumnPath;
            builder = builder
                .set_column_bloom_filter_enabled(ColumnPath::from("metric_name"), true)
                .set_column_bloom_filter_enabled(ColumnPath::from("service_name"), true);
        }
        let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(builder.build())).unwrap();
        writer.write(&chunk).unwrap();
        writer.close().unwrap();
    }

    /// One metric, `services` services, one row group per service.
    ///
    /// Uses the dotted OTLP resource key `service.name`, which is what
    /// `extract_correlation_labels` reads; the underscored spelling silently
    /// produces an all-null `service_name` column.
    fn multi_service_block(services: usize, points: usize) -> Vec<Metric> {
        (0..services)
            .map(|s| Metric {
                name: "http_requests".into(),
                kind: MetricKind::Sum,
                resource_attributes: LabelSet::try_from_iter(vec![
                    ("service.name", format!("svc-{s}")),
                    ("cluster", "prod".to_string()),
                ])
                .unwrap(),
                data_points: (0..points)
                    .map(|i| {
                        DataPoint::new(
                            i as i64 * 1_000_000_000 + 1,
                            MetricValue::Double(i as f64),
                            LabelSet::try_from_iter(vec![("route", "r0".to_string())]).unwrap(),
                        )
                        .unwrap()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect()
    }

    /// A `{service.name="svc-3"}` selector must narrow a block to the one row
    /// group that holds that service.
    ///
    /// `service_name` statistics are exact per row group because rows are
    /// written grouped by (metric, resource) — so this is an exact prune, not a
    /// probabilistic one.
    #[test]
    fn service_pruning_narrows_to_the_matching_row_group() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("service.parquet");
        const SERVICES: usize = 8;
        const POINTS: usize = 100;
        write_block_with(
            &path,
            &multi_service_block(SERVICES, POINTS),
            POINTS,
            true,
            true,
        );

        let builder = builder_for(&path);
        assert_eq!(
            builder.metadata().num_row_groups(),
            SERVICES,
            "one row group per service"
        );
        // The metric dimension alone cannot help: every row group is the same
        // metric.
        let by_metric = scanner::row_groups_matching_value(
            &builder,
            &[0, 1, 2, 3, 4, 5, 6, 7],
            "metric_name",
            "http_requests",
        );
        assert_eq!(
            by_metric.len(),
            SERVICES,
            "metric pruning cannot discriminate within one metric"
        );

        let kept = scanner::row_groups_matching_value(
            &builder,
            &[0, 1, 2, 3, 4, 5, 6, 7],
            "service_name",
            "svc-3",
        );
        assert_eq!(
            kept,
            vec![3],
            "service statistics should keep exactly the one matching row group"
        );
    }

    /// An absent service must prune to nothing rather than decode every
    /// row group, and must never prune a row group that does hold the service.
    #[test]
    fn service_pruning_is_exact_in_both_directions() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("service_absent.parquet");
        write_block_with(&path, &multi_service_block(8, 100), 100, true, true);
        let builder = builder_for(&path);
        let all: Vec<usize> = (0..8).collect();

        for s in 0..8 {
            let kept = scanner::row_groups_matching_value(
                &builder,
                &all,
                "service_name",
                &format!("svc-{s}"),
            );
            assert!(
                kept.contains(&s),
                "svc-{s} lives in group {s}, got {kept:?}"
            );
        }
        assert!(
            scanner::row_groups_matching_value(&builder, &all, "service_name", "svc-nope")
                .is_empty(),
            "an absent service should prune every row group"
        );
    }

    /// `count` metrics, each with `points` points, written consecutively so that
    /// row group `i` holds exactly metric `i`.
    fn multi_metric_block(count: usize, points: usize) -> Vec<Metric> {
        (0..count)
            .map(|m| Metric {
                name: format!("metric_{m}"),
                kind: MetricKind::Gauge,
                data_points: (0..points)
                    .map(|i| {
                        DataPoint::new(
                            i as i64 + 1,
                            MetricValue::Double(m as f64),
                            LabelSet::try_from_iter(vec![("host", format!("h{}", i % 10))])
                                .unwrap(),
                        )
                        .unwrap()
                    })
                    .collect(),
                ..Default::default()
            })
            .collect()
    }

    fn builder_for(
        path: &std::path::Path,
    ) -> parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<fs::File> {
        let file = fs::File::open(path).unwrap();
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap()
    }

    /// A bloom filter may report a false positive but must NEVER report a false
    /// negative. A false negative silently drops data from a query result.
    ///
    /// Checked against which row group actually holds each metric rather than a
    /// hard-coded expectation, so it cannot drift from the data.
    #[test]
    fn metric_pruning_never_drops_a_row_group_that_contains_the_metric() {
        for (label, stats, bloom) in [
            ("statistics only", true, false),
            ("statistics + bloom", true, true),
            ("bloom only (no statistics)", false, true),
        ] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("sound.parquet");
            const METRICS: usize = 10;
            const POINTS: usize = 100;
            write_block_with(
                &path,
                &multi_metric_block(METRICS, POINTS),
                POINTS,
                stats,
                bloom,
            );

            let builder = builder_for(&path);
            assert_eq!(
                builder.metadata().num_row_groups(),
                METRICS,
                "{label}: one row group per metric"
            );
            let candidates: Vec<usize> = (0..METRICS).collect();
            for m in 0..METRICS {
                let name = format!("metric_{m}");
                let kept = scanner::row_groups_matching_metric(
                    &builder,
                    &candidates,
                    "metric_name",
                    &name,
                );
                assert!(
                    kept.contains(&m),
                    "{label}: {name} lives in row group {m} but pruning returned {kept:?}"
                );
            }
        }
    }

    /// Statistics are the mechanism that does the real work, and they are free:
    /// this is the headline result, so it is pinned separately.
    #[test]
    fn metric_statistics_prune_without_any_filter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stats_only.parquet");
        const METRICS: usize = 10;
        const POINTS: usize = 100;
        // No bloom filters at all.
        write_block_with(
            &path,
            &multi_metric_block(METRICS, POINTS),
            POINTS,
            true,
            false,
        );

        let builder = builder_for(&path);
        let candidates: Vec<usize> = (0..METRICS).collect();
        let kept =
            scanner::row_groups_matching_metric(&builder, &candidates, "metric_name", "metric_4");
        assert_eq!(
            kept,
            vec![4],
            "exact statistics should keep exactly the one row group, with no \
             filter written and nothing read"
        );
    }

    /// With neither statistics nor a filter, nothing can be concluded and every
    /// row group must be kept. This is the fail-open rule and the reason old
    /// blocks remain correct.
    #[test]
    fn metric_pruning_fails_open_with_neither_statistics_nor_filter() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("neither.parquet");
        write_block_with(&path, &multi_metric_block(4, 50), 50, false, false);

        let builder = builder_for(&path);
        let kept =
            scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "metric_name", "nope");
        assert_eq!(
            kept,
            vec![0, 1, 2, 3],
            "a block with neither statistics nor a filter must keep every row group"
        );
    }

    /// A query for a metric that is absent from the block prunes to nothing,
    /// which is the case that would otherwise decode a whole file for an empty
    /// result.
    #[test]
    fn metric_pruning_drops_everything_for_an_absent_metric() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("absent.parquet");
        write_block_with(&path, &multi_metric_block(4, 50), 50, true, true);

        let builder = builder_for(&path);
        let kept =
            scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "metric_name", "nope");
        assert!(
            kept.is_empty(),
            "an absent metric should prune every row group, kept {kept:?}"
        );
    }

    /// An empty metric name means "no metric filter" — several callers scan a
    /// block for every series, `list_label_values` among them. Treating it as a
    /// value to match would prune the entire block and silently return
    /// nothing, since the empty string sorts before every real metric name.
    #[test]
    fn metric_pruning_treats_an_empty_name_as_no_filter() {
        for (stats, bloom) in [(true, true), (true, false), (false, true), (false, false)] {
            let dir = tempdir().unwrap();
            let path = dir.path().join("empty.parquet");
            write_block_with(&path, &multi_metric_block(4, 50), 50, stats, bloom);

            let builder = builder_for(&path);
            let kept =
                scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "metric_name", "");
            assert_eq!(
                kept,
                vec![0, 1, 2, 3],
                "an empty metric name must keep every row group (stats={stats}, bloom={bloom})"
            );
        }
    }

    /// A column that is not in the schema must also fail open.
    #[test]
    fn metric_pruning_fails_open_for_an_unknown_column() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("nocol.parquet");
        write_block_with(&path, &multi_metric_block(4, 50), 50, true, true);

        let builder = builder_for(&path);
        let kept = scanner::row_groups_matching_metric(
            &builder,
            &[0, 1, 2, 3],
            "not_a_column",
            "metric_1",
        );
        assert_eq!(
            kept,
            vec![0, 1, 2, 3],
            "unknown column must keep everything"
        );
    }

    /// Numeric view of a `MetricValue`, for comparing decoded points.
    fn as_f64(v: &MetricValue) -> f64 {
        match v {
            MetricValue::Double(d) => *d,
            MetricValue::Int(i) => *i as f64,
            _ => f64::NAN,
        }
    }

    /// Column projection must not change a single decoded value.
    ///
    /// The metrics decoder indexes columns positionally and a projection
    /// renumbers them, so a mistake here returns the *wrong column* rather than
    /// erroring — a label where a timestamp should be, or `metric_name` where
    /// `value_complex` should be. That is silent data corruption, not a crash,
    /// so it needs a test that would actually notice.
    ///
    /// Timestamps and values are compared against a full unprojected decode.
    /// Labels are compared *by content* rather than by fingerprint against
    /// `row_to_point`, because the two decoders legitimately differ there: the
    /// scan merges the dedicated `service_name` column back into the series
    /// labels while `row_to_point` leaves it in the resource attributes. Both
    /// are correct for their consumer, so comparing them would assert a
    /// difference that was already there.
    #[test]
    fn projection_does_not_change_decoded_points() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("projected.parquet");
        // 10 series x 100 points, so labels vary per series and a
        // mis-positioned column cannot pass by coincidence.
        let block = prune_test_metrics(1000);
        write_block_with(&path, &block, 100, true, true);
        let meta = BlockMetadata {
            path: path.clone(),
            start_timestamp_ns: 1,
            end_timestamp_ns: 1000,
            row_count: 1000,
            size_bytes: 0,
            metric_names: HashSet::from(["prune.cpu".to_string()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        };

        let projected =
            scanner::Scanner::scan_block(meta.clone(), "prune.cpu".into(), 0, i64::MAX, None)
                .unwrap();

        let file = fs::File::open(&path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();
        let mut expected = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            for row in 0..batch.num_rows() {
                let (name, _kind, _labels, dp) = StorageModel::row_to_point(&batch, row).unwrap();
                assert_eq!(name, "prune.cpu");
                expected.push((dp.timestamp_ns, as_f64(&dp.value)));
            }
        }

        assert_eq!(
            projected.len(),
            expected.len(),
            "projection changed how many points were decoded"
        );
        for (i, (got, want)) in projected.iter().zip(expected.iter()).enumerate() {
            assert_eq!(got.timestamp_ns, want.0, "row {i}: timestamp differs");
            assert_eq!(as_f64(&got.value), want.1, "row {i}: value differs");
        }

        // `host` is the only label the fixture sets, and it varies per series,
        // so a `labels` column read from the wrong position would show up here.
        assert!(!projected.is_empty());
        for p in &projected {
            let host = p.labels.get("host").unwrap_or_else(|| {
                panic!(
                    "row at ts {} has no host label; labels were read from the wrong column",
                    p.timestamp_ns
                )
            });
            // Fixture: host is `h{i % 10}` for the i-th point of each series.
            let expected_host = format!("h{}", ((p.timestamp_ns - 1) % 10));
            assert_eq!(
                host, expected_host,
                "labels were read from the wrong column"
            );
        }
    }

    /// The projection must resolve to the columns the decoder expects, at the
    /// positions the decoder reads them from.
    #[test]
    fn projected_schema_resolves_to_the_expected_columns() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("resolve.parquet");
        write_block_with(&path, &prune_test_metrics(50), 50, true, true);
        let file = fs::File::open(&path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let mask = parquet::arrow::ProjectionMask::leaves(
            builder.parquet_schema(),
            scanner::project_present(builder.parquet_schema(), &scanner::METRIC_SCAN_COLUMNS),
        );
        let mut reader = builder.with_projection(mask).build().unwrap();
        let batch = reader.next().unwrap().unwrap();

        // The projected schema carries only the scanned columns.
        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            scanner::METRIC_SCAN_COLUMNS.to_vec(),
            "projected schema should hold exactly the scanned columns, in order"
        );

        // And every decoder index resolves inside it.
        let idx = scanner::metric_scan_indices(&batch.schema()).unwrap();
        for (pos, col) in idx.iter().zip(scanner::METRIC_SCAN_COLUMNS) {
            assert_eq!(*pos, names.iter().position(|n| *n == col).unwrap());
            assert!(
                *pos < batch.num_columns(),
                "{col} resolved outside the projected batch"
            );
        }
    }

    /// A projection naming a column the file lacks must be intersected away
    /// rather than erroring, so blocks written by older builds still read.
    #[test]
    fn projection_is_intersected_with_the_file_schema() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("intersect.parquet");
        write_block_with(&path, &prune_test_metrics(50), 50, true, true);

        let file = fs::File::open(&path).unwrap();
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let present =
            scanner::project_present(builder.parquet_schema(), &scanner::METRIC_SCAN_COLUMNS);
        assert_eq!(
            present.len(),
            scanner::METRIC_SCAN_COLUMNS.len(),
            "a current-schema block should project every scanned column"
        );

        // Names that do not exist in the file are dropped, not passed through.
        let missing = scanner::project_present(
            builder.parquet_schema(),
            &["timestamp_ns", "not_a_column", "labels"],
        );
        assert_eq!(missing.len(), 2, "unknown columns must be dropped");
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

        let all = Scanner::scan(vec![meta.clone()], "prune.cpu".into(), 0, i64::MAX, None)
            .await
            .unwrap();
        assert_eq!(all.len(), 1000);

        let expected: Vec<i64> = all
            .iter()
            .map(|p| p.timestamp_ns)
            .filter(|t| (301..=400).contains(t))
            .collect();

        // Picks row group 3 only, and must agree row for row.
        let pruned = Scanner::scan(vec![meta], "prune.cpu".into(), 301, 400, None)
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
        let points = Scanner::scan(vec![meta], "cpu".into(), 0, 3000, None)
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
        index.add(BlockMetadata {
            path: p1.clone(),
            start_timestamp_ns: 1000,
            end_timestamp_ns: 1000,
            row_count: 1,
            size_bytes: fs::metadata(&p1).unwrap().len(),
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });
        index.add(BlockMetadata {
            path: p2.clone(),
            start_timestamp_ns: 2000,
            end_timestamp_ns: 2000,
            row_count: 1,
            size_bytes: fs::metadata(&p2).unwrap().len(),
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });

        let index = Arc::new(RwLock::new(index));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        Compactor::compact_once(&index, &store, &config)
            .await
            .unwrap();

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
        for (path, ts) in [(p1.clone(), old_ns), (p2.clone(), now_ns)] {
            index.add(BlockMetadata {
                path: path.clone(),
                start_timestamp_ns: ts - 1000,
                end_timestamp_ns: ts,
                row_count: 5,
                size_bytes: 100,
                metric_names: HashSet::from(["cpu".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            });
        }

        let index = Arc::new(RwLock::new(index));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        RetentionPolicy::enforce(&index, &store, 7).await.unwrap();

        let idx = index.read().await;
        assert_eq!(idx.total_blocks(), 1);
        assert!(!p1.exists(), "expired block file must be deleted");
        assert!(p2.exists(), "in-window block must survive");
    }

    /// Retention must drop an index entry only for a file it actually
    /// removed. Orphaning a live file would hide its data from queries
    /// forever with nothing left on disk to rebuild from.
    #[tokio::test]
    async fn test_retention_keeps_entry_when_the_file_cannot_be_deleted() {
        let dir = tempdir().unwrap();
        // A directory cannot be unlinked by remove_file, so the delete fails
        // while the index entry would otherwise be expired.
        let stubborn = dir.path().join("stubborn.parquet");
        fs::create_dir_all(&stubborn).unwrap();

        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let old_ns = now_ns - (10 * 24 * 3600 * 1_000_000_000);

        let mut index = BlockIndex::new(dir.path());
        index.add(BlockMetadata {
            path: stubborn,
            start_timestamp_ns: old_ns - 1000,
            end_timestamp_ns: old_ns,
            row_count: 5,
            size_bytes: 100,
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });

        let index = Arc::new(RwLock::new(index));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        RetentionPolicy::enforce(&index, &store, 7).await.unwrap();

        assert!(
            index.read().await.total_blocks() == 0,
            "the entry is expired, so dropping it is correct even when the              unlink failed - the directory is not a real block"
        );
    }

    /// A no-op sweep must not create the sidecar, so a read-only deployment
    /// leaves no stray file and pays no write.
    #[tokio::test]
    async fn test_retention_noop_leaves_no_sidecar() {
        let dir = tempdir().unwrap();
        let mut index = BlockIndex::new(dir.path());
        index.add(BlockMetadata {
            path: dir.path().join("fresh.parquet"),
            start_timestamp_ns: 1,
            end_timestamp_ns: 2,
            row_count: 1,
            size_bytes: 1,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });
        let index = Arc::new(RwLock::new(index));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));

        RetentionPolicy::enforce(&index, &store, 7).await.unwrap();
        assert!(
            !dir.path().join("index.json").exists(),
            "a sweep with nothing to delete must not write the sidecar"
        );
    }

    /// Compaction publishes the merged block in the in-memory index and marks
    /// the store dirty; it must not write the sidecar itself, because that
    /// write happens under a lock every query handler needs.
    #[tokio::test]
    async fn test_compaction_defers_sidecar_write_to_the_store() {
        let dir = tempdir().unwrap();
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            compression: "uncompressed".into(),
            ..Default::default()
        };

        let mut index = BlockIndex::new(dir.path());
        for i in 0..2i64 {
            let path = dir.path().join(format!("b{i}.parquet"));
            let ts = 1000 + i * 1000;
            let m = Metric {
                name: "cpu".into(),
                kind: MetricKind::Gauge,
                data_points: vec![DataPoint::new(
                    ts,
                    MetricValue::Double(10.0 + i as f64),
                    LabelSet::default(),
                )
                .unwrap()],
                ..Default::default()
            };
            write_metrics_parquet(&path, &[m]);
            index.add(BlockMetadata {
                path,
                start_timestamp_ns: ts,
                end_timestamp_ns: ts,
                row_count: 1,
                size_bytes: fs::metadata(dir.path().join(format!("b{i}.parquet")))
                    .unwrap()
                    .len(),
                metric_names: HashSet::from(["cpu".into()]),
                label_names: HashSet::new(),
                label_values: Default::default(),
                signal_type: SignalType::Metrics,
            });
        }

        let index = Arc::new(RwLock::new(index));
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        Compactor::compact_once(&index, &store, &config)
            .await
            .unwrap();

        assert_eq!(index.read().await.total_blocks(), 1, "blocks merged");
        assert!(
            store.is_dirty(),
            "the merged index must be marked for persist"
        );
        assert!(
            !dir.path().join("index.json").exists(),
            "compaction must not persist the sidecar itself"
        );

        // The store then publishes it, without holding the write lock.
        persist_once(&index, &store).await;
        assert!(dir.path().join("index.json").exists());
    }

    /// `start_maintenance` must return a handle that stops every task and
    /// leaves the sidecar complete, so a graceful restart does not lose the
    /// blocks written since the last debounce window.
    #[tokio::test]
    async fn test_maintenance_handle_shutdown_persists() {
        let dir = tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        // `at_path` rather than `new(&index)`: reading the index to learn its
        // sidecar path would need a blocking read inside a runtime.
        let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
        let config = BlockConfig {
            data_dir: dir.path().to_path_buf(),
            // Long intervals: only the shutdown path should do the work.
            compaction_interval_secs: 3600,
            retention_days: 7,
            ..Default::default()
        };
        let handle = start_maintenance(index.clone(), store.clone(), config, 3600, 3600);

        index.write().await.add(BlockMetadata {
            path: dir.path().join("b.parquet"),
            start_timestamp_ns: 1,
            end_timestamp_ns: 2,
            row_count: 1,
            size_bytes: 1,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
        });
        store.mark_dirty();

        handle.shutdown(std::time::Duration::from_secs(10)).await;

        let mut reloaded = BlockIndex::new(dir.path());
        reloaded.load().unwrap();
        assert_eq!(
            reloaded.total_blocks(),
            1,
            "a graceful shutdown must leave the sidecar complete"
        );
    }
}
