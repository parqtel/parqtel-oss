use super::block_file::{sync_parent_dir, BlockFileWriter, FooterIndex};
use super::index::{BlockIndex, BlockIndexStore};
use super::ownership::OwnershipRegistry;
use crate::config::BlockConfig;
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, MetricKind};
use crate::models::storage::{BlockMetadata, MetricRun, SignalType, StorageModel};
use crate::models::traces::Span;
use crate::storage::{SeriesTable, SERIES_INDEX_FOOTER_KEY};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Points grouped with their metric metadata, plus log records and spans — the
/// decoded contents of source blocks awaiting compaction.
pub(crate) type DecodedBlocks = (
    Vec<(String, MetricKind, LabelSet, DataPoint)>,
    Vec<LogRecord>,
    Vec<Span>,
);

/// Per-field cap on the label-value dictionary, matching the flush path.
const MAX_LABEL_VALUES_PER_FIELD: usize = 10_000;

/// Label-value dictionaries of the blocks being merged, unioned.
///
/// Compaction used to emit an empty dictionary, so merging silently dropped
/// the flush-time index that `/api/v1/label/:name/values` reads from metadata
/// instead of decoding blocks. Every compaction pass degraded autocomplete until
/// the source blocks aged out.
pub(crate) type LabelValues = BTreeMap<String, std::collections::BTreeSet<String>>;

struct TempBlockFile(PathBuf);

impl Drop for TempBlockFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

/// Union of several blocks' label-value dictionaries, respecting the same
/// per-field cap the flush path applies so one high-cardinality label cannot
/// bloat the index.
pub(crate) fn union_label_values(blocks: &[BlockMetadata], max_per_field: usize) -> LabelValues {
    let mut out: LabelValues = BTreeMap::new();
    for b in blocks {
        for (field, values) in &b.label_values {
            let entry = out.entry(field.clone()).or_default();
            for v in values {
                if entry.len() >= max_per_field {
                    break;
                }
                entry.insert(v.clone());
            }
        }
    }
    out
}

/// Background task for small log-block merges and streaming metrics rollups.
/// Metrics use the bounded L0/L1 path; legacy warm/cold merging remains for logs.
pub struct Compactor;

impl Compactor {
    pub async fn run_loop(
        index: Arc<RwLock<BlockIndex>>,
        store: Arc<BlockIndexStore>,
        config: BlockConfig,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) {
        Self::run_loop_scoped(
            index,
            store,
            config,
            shutdown,
            Arc::new(OwnershipRegistry::everything()),
        )
        .await;
    }

    /// As [`Self::run_loop`], but only blocks this node owns are merged and
    /// retired. A sharded node shares the catalog with other writers.
    pub async fn run_loop_scoped(
        index: Arc<RwLock<BlockIndex>>,
        store: Arc<BlockIndexStore>,
        config: BlockConfig,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
        ownership: Arc<OwnershipRegistry>,
    ) {
        let interval = Duration::from_secs(config.compaction_interval_secs.max(60));
        tracing::debug!(interval_secs = interval.as_secs(), "compactor started");
        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        tracing::debug!("compactor stopping");
                        return;
                    }
                }
            }
            tracing::debug!("compaction cycle starting");
            let cycle_start = std::time::Instant::now();
            if let Err(e) = Self::compact_small_logs_once(&index, &store, &config, &ownership).await
            {
                tracing::error!("Log compaction failed: {}", e);
            }
            if let Err(e) =
                Self::compact_metrics_levels_once_scoped(&index, &store, &config, &ownership).await
            {
                tracing::error!("Metrics compaction failed: {}", e);
            }
            // Legacy bounded compaction remains for logs. Metrics use the
            // series-streaming leveled path above; traces compact separately.
            if let Err(e) = Self::compact_tiered(&index, &store, &config, &ownership).await {
                tracing::error!("Tiered compaction failed: {}", e);
            }
            tracing::debug!(
                duration_ms = cycle_start.elapsed().as_millis(),
                "compaction cycle complete"
            );
        }
    }

    /// Performs one bounded metrics compaction pass.
    pub async fn compact_metrics_levels_once(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
    ) -> Result<usize> {
        super::stream_compactor::compact_metrics_levels_once(index, store, config).await
    }

    /// As [`Self::compact_metrics_levels_once`], only merging owned blocks.
    pub async fn compact_metrics_levels_once_scoped(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
        ownership: &OwnershipRegistry,
    ) -> Result<usize> {
        super::stream_compactor::compact_metrics_levels_once_scoped(index, store, config, ownership)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn compact_once(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
    ) -> Result<()> {
        Self::compact_small_blocks_once(
            index,
            store,
            config,
            None,
            &OwnershipRegistry::everything(),
        )
        .await
    }

    async fn compact_small_logs_once(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
        ownership: &OwnershipRegistry,
    ) -> Result<()> {
        Self::compact_small_blocks_once(index, store, config, Some(SignalType::Logs), ownership)
            .await
    }

    async fn compact_small_blocks_once(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
        signal_filter: Option<SignalType>,
        ownership: &OwnershipRegistry,
    ) -> Result<()> {
        let (to_compact, original_paths, signal_type) = {
            let idx = index.read().await;
            let mut small_blocks: Vec<_> = idx
                .blocks
                .iter()
                .filter(|b| {
                    b.row_count < 10000
                        && signal_filter.is_none_or(|signal| b.signal_type == signal)
                        && ownership.may_retire(&b.path)
                })
                .cloned()
                .collect();
            if small_blocks.len() < 2 {
                tracing::debug!("compaction: fewer than 2 small blocks, skipping");
                return Ok(());
            }
            // Merge a time-adjacent run, not whichever blocks happen to be
            // first in the index. Non-contiguous outputs weaken row-group
            // pruning on exactly the data that is kept longest.
            small_blocks.sort_by_key(|b| b.start_timestamp_ns);
            let signal = small_blocks[0].signal_type;
            small_blocks.retain(|b| b.signal_type == signal);
            if small_blocks.len() < 2 {
                return Ok(());
            }
            let count = std::cmp::min(
                config.compaction_max_merge_blocks.max(2),
                small_blocks.len(),
            );
            small_blocks.truncate(count);
            let paths: Vec<_> = small_blocks.iter().map(|b| b.path.clone()).collect();
            (small_blocks, paths, signal)
        };

        // Decode + merge + encode are CPU and filesystem work; running them
        // inline parks a tokio worker for the whole cycle, stalling every
        // request multiplexed onto it. The index lock is not held here.
        let config = config.clone();
        let paths_for_merge = original_paths.clone();
        let merged = tokio::task::spawn_blocking(move || -> Result<Option<BlockMetadata>> {
            let (all_points, all_logs, all_spans) =
                Self::read_source_blocks(&to_compact, signal_type)?;
            if all_points.is_empty() && all_logs.is_empty() && all_spans.is_empty() {
                return Ok(None);
            }
            let label_values = union_label_values(&to_compact, MAX_LABEL_VALUES_PER_FIELD);
            Ok(Some(Self::write_merged(
                &config,
                signal_type,
                all_points,
                all_logs,
                all_spans,
                label_values,
            )?))
        })
        .await
        .map_err(|e| Error::Internal(format!("compaction task panicked: {e}")))??;

        // Publish under the write lock: a single swap, no I/O, and the sidecar
        // write is deferred to the persist task rather than done under lock.
        {
            let mut idx = index.write().await;
            idx.replace(&original_paths, merged.clone());
            store.mark_dirty();
        }

        let new_meta = match merged {
            Some(meta) => meta,
            None => {
                tracing::debug!(
                    signal = ?signal_type,
                    blocks_removed = original_paths.len(),
                    "compaction: empty blocks removed"
                );
                return Ok(());
            }
        };

        // Delete the sources after the merge is published and durable in the
        // in-memory index, so a crash here leaves redundant blocks (harmless)
        // rather than a dangling index entry (a query error).
        let _ = tokio::task::spawn_blocking(move || {
            for path in paths_for_merge {
                let _ = crate::storage::block_store::delete(&path);
            }
        })
        .await;

        tracing::debug!(
            signal = ?signal_type,
            merged_blocks = original_paths.len(),
            row_count = new_meta.row_count,
            "compaction: small blocks merged"
        );
        Ok(())
    }

    /// Legacy log compaction: merge older adjacent blocks into larger time frames.
    /// Warm tier (>6h old): target 6h blocks. Cold tier (>24h old): target 24h blocks.
    async fn compact_tiered(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
        ownership: &OwnershipRegistry,
    ) -> Result<()> {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let six_hours_ns = 6 * 3600 * 1_000_000_000i64;
        let twenty_four_hours_ns = 24 * 3600 * 1_000_000_000i64;
        tracing::debug!("tiered compaction cycle starting");

        // Bounded per cycle so one pass cannot monopolise the disk. Previously
        // this was `break` after the first merge per signal, so a cluster with
        // many small blocks could never converge: the small-block population
        // grew faster than one merge per hour removed it.
        let max_merges = config.compaction_max_merges_per_pass.max(1);
        let mut merges_done = 0usize;

        // Metrics use the streaming series merge; this path remains for logs.
        for signal_type in &[SignalType::Logs] {
            let candidates = {
                let idx = index.read().await;
                let mut blocks: Vec<_> = idx
                    .blocks
                    .iter()
                    .filter(|b| b.signal_type == *signal_type)
                    .filter(|b| now_ns - b.end_timestamp_ns > six_hours_ns)
                    .filter(|b| b.row_count < 500_000) // don't re-merge already large blocks
                    .filter(|b| ownership.may_retire(&b.path))
                    .cloned()
                    .collect();
                blocks.sort_by_key(|b| b.start_timestamp_ns);
                blocks
            };

            if candidates.len() < 2 {
                tracing::debug!(
                    signal = ?signal_type,
                    candidate_blocks = candidates.len(),
                    "tiered compaction: not enough candidates, skipping"
                );
                continue;
            }

            // Find adjacent blocks within a 6h window that can be merged
            let tier_window = if candidates
                .iter()
                .any(|b| now_ns - b.end_timestamp_ns > twenty_four_hours_ns)
            {
                twenty_four_hours_ns // cold tier: 24h target
            } else {
                six_hours_ns // warm tier: 6h target
            };

            let mut i = 0;
            while i < candidates.len() {
                let anchor_start = candidates[i].start_timestamp_ns;
                let window_end = anchor_start + tier_window;
                let mut group: Vec<BlockMetadata> = vec![candidates[i].clone()];
                let mut j = i + 1;
                while j < candidates.len() && candidates[j].start_timestamp_ns <= window_end {
                    group.push(candidates[j].clone());
                    j += 1;
                }
                i = j;

                if group.len() < 2 {
                    continue;
                }
                // Bounded by config, and at least 2 or a group can never merge.
                group.truncate(config.compaction_max_merge_blocks.max(2));

                let paths: Vec<_> = group.iter().map(|b| b.path.clone()).collect();

                if *signal_type == SignalType::Traces {
                    // read_source_blocks handles only metrics and logs; trace
                    // compaction reads spans directly.
                    continue;
                }

                let config = config.clone();
                let source_group = group.clone();
                let merge_paths = paths.clone();
                let merged =
                    tokio::task::spawn_blocking(move || -> Result<Option<BlockMetadata>> {
                        let (all_points, all_logs, all_spans) =
                            Self::read_source_blocks(&source_group, *signal_type)?;
                        if all_points.is_empty() && all_logs.is_empty() && all_spans.is_empty() {
                            return Ok(None);
                        }
                        let label_values =
                            union_label_values(&source_group, MAX_LABEL_VALUES_PER_FIELD);
                        Ok(Some(Self::write_merged(
                            &config,
                            *signal_type,
                            all_points,
                            all_logs,
                            all_spans,
                            label_values,
                        )?))
                    })
                    .await
                    .map_err(|e| {
                        Error::Internal(format!("tiered compaction task panicked: {e}"))
                    })??;

                {
                    let mut idx = index.write().await;
                    idx.replace(&paths, merged.clone());
                    store.mark_dirty();
                }

                if merged.is_some() {
                    // Delete only after the merged block is published in the
                    // in-memory index: a crash in between leaves redundant
                    // blocks (harmless) rather than a dangling index entry
                    // (a query error).
                    let _ = tokio::task::spawn_blocking(move || {
                        for path in merge_paths {
                            let _ = crate::storage::block_store::delete(&path);
                        }
                    })
                    .await;
                }

                merges_done += 1;
                if merges_done >= max_merges {
                    break;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn read_source_blocks(
        blocks: &[BlockMetadata],
        signal_type: SignalType,
    ) -> Result<DecodedBlocks> {
        let mut all_points = Vec::new();
        let mut all_logs = Vec::new();
        let mut all_spans = Vec::new();

        for meta in blocks {
            let file = match crate::storage::block_store::open(&meta.path) {
                Ok(f) => f,
                Err(Error::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!("Compaction source block not found: {:?}", meta.path);
                    continue;
                }
                Err(e) => return Err(e),
            };

            let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| Error::Parquet(e.to_string()))?;
            let series_table = if signal_type == SignalType::Metrics
                && reader_builder.schema().index_of("labels").is_err()
            {
                let footer = reader_builder
                    .metadata()
                    .file_metadata()
                    .key_value_metadata()
                    .and_then(|kv| kv.iter().find(|k| k.key == SERIES_INDEX_FOOTER_KEY))
                    .and_then(|kv| kv.value.as_deref())
                    .ok_or_else(|| {
                        Error::Internal(format!(
                            "metrics block {:?} has no labels column or series index",
                            meta.path
                        ))
                    })?;
                let table = SeriesTable::from_footer_value(footer)?;
                let series_count = table.series_count();
                let rows = reader_builder.metadata().file_metadata().num_rows().max(0) as u64;
                let mut next_row = 0u64;
                for series_id in 0..series_count {
                    let (start, series_rows, _, _) = table
                        .series_bounds(series_id)
                        .ok_or_else(|| Error::Internal("series index entry is missing".into()))?;
                    if start != next_row {
                        return Err(Error::Internal(
                            "series index row ranges are not contiguous".into(),
                        ));
                    }
                    next_row = start
                        .checked_add(u64::from(series_rows))
                        .ok_or_else(|| Error::Internal("series index row range overflow".into()))?;
                }
                if next_row != rows || table.total_rows() != rows {
                    return Err(Error::Internal(
                        "series index row count disagrees with compaction source".into(),
                    ));
                }
                Some(table)
            } else {
                None
            };

            let reader = reader_builder
                .build()
                .map_err(|e| Error::Parquet(e.to_string()))?;
            let mut block_row = 0u64;
            let mut series_cursor = 0usize;
            let mut current_series = None;

            for record_batch in reader {
                let record_batch = record_batch.map_err(|e| Error::Parquet(e.to_string()))?;

                // Cache keys borrow from this chunk — recreate per chunk.
                let mut attr_cache = std::collections::HashMap::new();
                let mut res_cache = std::collections::HashMap::new();
                for row in 0..record_batch.num_rows() {
                    match signal_type {
                        SignalType::Metrics => {
                            let point = if let Some(table) = &series_table {
                                let labels = loop {
                                    let (start, series_rows, _, _) =
                                        table.series_bounds(series_cursor).ok_or_else(|| {
                                            Error::Internal(
                                                "series index ended before compaction source rows"
                                                    .into(),
                                            )
                                        })?;
                                    let end = start
                                        .checked_add(u64::from(series_rows))
                                        .ok_or_else(|| {
                                            Error::Internal(
                                                "series index row range overflow".into(),
                                            )
                                        })?;
                                    if block_row < start {
                                        return Err(Error::Internal(
                                            "gap in series index row ranges".into(),
                                        ));
                                    }
                                    if block_row >= end {
                                        series_cursor += 1;
                                        current_series = None;
                                        continue;
                                    }
                                    if current_series.is_none() {
                                        current_series = Some(table.series_entry(series_cursor)?);
                                    }
                                    break current_series
                                        .as_ref()
                                        .map(|series| series.labels.clone())
                                        .ok_or_else(|| {
                                            Error::Internal(
                                                "series index ended before compaction source rows"
                                                    .into(),
                                            )
                                        })?;
                                };
                                StorageModel::row_to_point_with_labels(&record_batch, row, labels)?
                            } else {
                                StorageModel::row_to_point(&record_batch, row)?
                            };
                            all_points.push(point);
                            block_row += 1;
                        }
                        SignalType::Logs => {
                            all_logs.push(StorageModel::row_to_log(
                                &record_batch,
                                row,
                                &mut attr_cache,
                                &mut res_cache,
                            )?);
                        }
                        // Trace blocks used to be skipped entirely, so they
                        // never merged and accumulated until retention deleted
                        // them.
                        SignalType::Traces => {
                            all_spans.push(StorageModel::row_to_span(&record_batch, row)?);
                        }
                    }
                }
            }
            if let Some(table) = &series_table {
                if block_row != table.total_rows() {
                    return Err(Error::Internal(
                        "series index did not cover compaction source rows".into(),
                    ));
                }
            }
        }
        Ok((all_points, all_logs, all_spans))
    }

    #[allow(clippy::too_many_arguments)]
    fn write_merged(
        config: &BlockConfig,
        signal_type: SignalType,
        all_points: Vec<(String, MetricKind, LabelSet, DataPoint)>,
        all_logs: Vec<LogRecord>,
        all_spans: Vec<Span>,
        label_values: LabelValues,
    ) -> Result<BlockMetadata> {
        Self::write_merged_with_provenance(
            config,
            signal_type,
            all_points,
            all_logs,
            all_spans,
            label_values,
            None,
            None,
            &[],
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn write_merged_with_provenance(
        config: &BlockConfig,
        signal_type: SignalType,
        mut all_points: Vec<(String, MetricKind, LabelSet, DataPoint)>,
        mut all_logs: Vec<LogRecord>,
        mut all_spans: Vec<Span>,
        label_values: LabelValues,
        wal_range: Option<(u64, u64)>,
        filename_prefix: Option<&str>,
        compacted_sources: &[std::path::PathBuf],
    ) -> Result<BlockMetadata> {
        let mut series = None;
        let mut log_index = None;
        let (record_batch, start_ts, end_ts, row_count, metric_names, label_names) =
            match signal_type {
                SignalType::Metrics => {
                    all_points.sort_by_key(|(_, _, _, dp)| dp.timestamp_ns);
                    let start = all_points[0].3.timestamp_ns;
                    let end = all_points
                        .last()
                        .ok_or_else(|| Error::Internal("No points".into()))?
                        .3
                        .timestamp_ns;
                    let rows = all_points.len();
                    let mut m_names = HashSet::new();
                    let mut l_names = HashSet::new();
                    let mut groups: BTreeMap<(String, MetricKind, LabelSet), Vec<DataPoint>> =
                        BTreeMap::new();
                    for (name, kind, resource, dp) in all_points {
                        m_names.insert(name.clone());
                        for l in resource.keys() {
                            l_names.insert(l.clone());
                        }
                        for l in dp.labels.keys() {
                            l_names.insert(l.clone());
                        }
                        groups.entry((name, kind, resource)).or_default().push(dp);
                    }
                    // Series-contiguous, time-ordered inside each series: the
                    // layout the series table describes.
                    let mut ordered: Vec<_> = groups.into_iter().collect();
                    for ((_, _, resource), dps) in &mut ordered {
                        let service = resource
                            .get("service.name")
                            .filter(|value| !value.is_empty());
                        let mut fingerprints = HashMap::new();
                        for point in dps.iter() {
                            fingerprints.entry(point.labels.addr()).or_insert_with(|| {
                                service.map_or_else(
                                    || point.labels.fingerprint(),
                                    |service| {
                                        point.labels.with("service.name", service).fingerprint()
                                    },
                                )
                            });
                        }
                        dps.sort_by_key(|point| {
                            (fingerprints[&point.labels.addr()], point.timestamp_ns)
                        });
                    }
                    let runs: Vec<MetricRun<'_>> = ordered
                        .iter()
                        .map(|((name, kind, resource), dps)| MetricRun {
                            name,
                            kind: *kind,
                            resource,
                            points: dps.iter().collect(),
                        })
                        .collect();
                    let (chunk, table) = StorageModel::metric_runs_to_block(&runs)?;
                    series = Some(table);
                    (chunk, start, end, rows, m_names, l_names)
                }
                SignalType::Logs => {
                    all_logs.sort_by_key(|l| l.timestamp_ns);
                    let start = all_logs[0].timestamp_ns;
                    let end = all_logs
                        .last()
                        .ok_or_else(|| Error::Internal("No logs".into()))?
                        .timestamp_ns;
                    let rows = all_logs.len();
                    let mut l_names = HashSet::new();
                    for log in &all_logs {
                        for l in log.attributes.keys() {
                            l_names.insert(l.clone());
                        }
                        for l in log.resource_attributes.keys() {
                            l_names.insert(l.clone());
                        }
                    }
                    let (chunk, index) = StorageModel::logs_to_block(&all_logs)?;
                    log_index = Some(index);
                    (chunk, start, end, rows, HashSet::new(), l_names)
                }
                SignalType::Traces => {
                    // Spans sort by start time so the merged block keeps the
                    // time-ordered layout the scanner's row-group pruning relies on.
                    all_spans.sort_by_key(|s| s.start_time_ns);
                    let start = all_spans[0].start_time_ns;
                    let end = all_spans
                        .last()
                        .ok_or_else(|| Error::Internal("No spans".into()))?
                        .end_time_ns;
                    let rows = all_spans.len();
                    let mut l_names = HashSet::new();
                    for span in &all_spans {
                        l_names.extend(span.attributes.keys().cloned());
                    }
                    (
                        StorageModel::traces_to_chunk(&all_spans)?,
                        start,
                        end,
                        rows,
                        HashSet::new(),
                        l_names,
                    )
                }
            };

        let filename = format!(
            "{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let filename = filename_prefix
            .map(|prefix| format!("{prefix}_{filename}"))
            .unwrap_or(filename);
        let final_path = config.data_dir.join(&filename);
        let tmp_path = config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&config.data_dir)?;
        let _temp = TempBlockFile(tmp_path.clone());

        // The same writer as a flushed block, so a compacted block keeps the
        // bloom filters, page statistics and series table of its sources.
        let mut writer = BlockFileWriter::create(
            &tmp_path,
            record_batch.schema(),
            &config.compression,
            config.compression_level,
            config.row_group_size,
            wal_range,
            series
                .as_ref()
                .map(FooterIndex::Series)
                .or(log_index.as_ref().map(FooterIndex::Logs)),
            compacted_sources,
        )?;
        writer.write(&record_batch)?;
        writer.close_and_sync()?;

        fs::rename(&tmp_path, &final_path)?;
        sync_parent_dir(&final_path)?;
        crate::storage::block_store::store().publish(&final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();
        tracing::debug!(
            signal = ?signal_type,
            row_count = row_count,
            size_bytes = size_bytes,
            start_ts,
            end_ts,
            "compacted block written"
        );

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names,
            label_names,
            label_values,
            signal_type,
            wal_range,
        })
    }
}
