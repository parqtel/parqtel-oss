use super::index::{BlockIndex, BlockIndexStore};
use crate::config::{compression_from_name, BlockConfig};
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric, MetricKind};
use crate::models::storage::{BlockMetadata, SignalType, StorageModel};
use crate::models::traces::Span;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Points grouped with their metric metadata, plus log records and spans — the
/// decoded contents of source blocks awaiting compaction.
type DecodedBlocks = (
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
type LabelValues = BTreeMap<String, std::collections::BTreeSet<String>>;

/// Union of several blocks' label-value dictionaries, respecting the same
/// per-field cap the flush path applies so one high-cardinality label cannot
/// bloat the index.
fn union_label_values(blocks: &[BlockMetadata], max_per_field: usize) -> LabelValues {
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

/// Background task that merges small adjacent blocks and implements tiered compaction.
/// Tier strategy:
///   - Small blocks (< 10K rows): merge up to 8 into one (existing behavior)
///   - Warm tier (blocks > 6h old, same signal): merge adjacent into ~6h blocks
///   - Cold tier (blocks > 24h old): merge adjacent into ~24h blocks
pub struct Compactor;

impl Compactor {
    pub async fn run_loop(
        index: Arc<RwLock<BlockIndex>>,
        store: Arc<BlockIndexStore>,
        config: BlockConfig,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
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
            if let Err(e) = Self::compact_once(&index, &store, &config).await {
                tracing::error!("Compaction failed: {}", e);
            }
            // Tiered compaction for warm/cold data
            if let Err(e) = Self::compact_tiered(&index, &store, &config).await {
                tracing::error!("Tiered compaction failed: {}", e);
            }
            tracing::debug!(
                duration_ms = cycle_start.elapsed().as_millis(),
                "compaction cycle complete"
            );
        }
    }

    pub(crate) async fn compact_once(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
    ) -> Result<()> {
        let (to_compact, original_paths, signal_type) = {
            let idx = index.read().await;
            let mut small_blocks: Vec<_> = idx
                .blocks
                .iter()
                .filter(|b| b.row_count < 10000)
                .cloned()
                .collect();
            if small_blocks.len() < 2 {
                tracing::debug!("compaction: fewer than 2 small blocks, skipping");
                return Ok(());
            }
            let count = std::cmp::min(
                config.compaction_max_merge_blocks.max(2),
                small_blocks.len(),
            );
            small_blocks.truncate(count);
            let paths: Vec<_> = small_blocks.iter().map(|b| b.path.clone()).collect();
            let signal = small_blocks[0].signal_type;
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
                let _ = fs::remove_file(path);
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

    /// Tiered compaction: merge adjacent blocks of the same signal type into larger time frames.
    /// Warm tier (>6h old): target 6h blocks. Cold tier (>24h old): target 24h blocks.
    async fn compact_tiered(
        index: &Arc<RwLock<BlockIndex>>,
        store: &BlockIndexStore,
        config: &BlockConfig,
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

        // Process each signal type
        for signal_type in &[SignalType::Metrics, SignalType::Logs, SignalType::Traces] {
            let candidates = {
                let idx = index.read().await;
                let mut blocks: Vec<_> = idx
                    .blocks
                    .iter()
                    .filter(|b| b.signal_type == *signal_type)
                    .filter(|b| now_ns - b.end_timestamp_ns > six_hours_ns)
                    .filter(|b| b.row_count < 500_000) // don't re-merge already large blocks
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
                    // For traces, skip read_source_blocks (which only handles metrics/logs)
                    // and just leave them for now — trace compaction reads spans directly
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
                            let _ = fs::remove_file(path);
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

    fn read_source_blocks(
        blocks: &[BlockMetadata],
        signal_type: SignalType,
    ) -> Result<DecodedBlocks> {
        let mut all_points = Vec::new();
        let mut all_logs = Vec::new();
        let mut all_spans = Vec::new();

        for meta in blocks {
            let file = match File::open(&meta.path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!("Compaction source block not found: {:?}", meta.path);
                    continue;
                }
                Err(e) => return Err(Error::Io(e)),
            };

            let reader_builder = ParquetRecordBatchReaderBuilder::try_new(file)
                .map_err(|e| Error::Parquet(e.to_string()))?;

            let reader = reader_builder
                .build()
                .map_err(|e| Error::Parquet(e.to_string()))?;

            for record_batch in reader {
                let record_batch = record_batch.map_err(|e| Error::Parquet(e.to_string()))?;

                // Cache keys borrow from this chunk — recreate per chunk.
                let mut attr_cache = std::collections::HashMap::new();
                let mut res_cache = std::collections::HashMap::new();
                for row in 0..record_batch.num_rows() {
                    match signal_type {
                        SignalType::Metrics => {
                            all_points.push(StorageModel::row_to_point(&record_batch, row)?);
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
        }
        Ok((all_points, all_logs, all_spans))
    }

    #[allow(clippy::too_many_arguments)]
    fn write_merged(
        config: &BlockConfig,
        signal_type: SignalType,
        mut all_points: Vec<(String, MetricKind, LabelSet, DataPoint)>,
        mut all_logs: Vec<LogRecord>,
        mut all_spans: Vec<Span>,
        label_values: LabelValues,
    ) -> Result<BlockMetadata> {
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
                    let metrics: Vec<_> = groups
                        .into_iter()
                        .map(|((name, kind, resource), dps)| Metric {
                            name,
                            description: "".into(),
                            unit: "".into(),
                            kind,
                            resource_attributes: resource,
                            data_points: dps,
                        })
                        .collect();
                    (
                        StorageModel::metrics_to_chunk(&metrics)?,
                        start,
                        end,
                        rows,
                        m_names,
                        l_names,
                    )
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
                    (
                        StorageModel::logs_to_chunk(&all_logs)?,
                        start,
                        end,
                        rows,
                        HashSet::new(),
                        l_names,
                    )
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
        let final_path = config.data_dir.join(&filename);
        let tmp_path = config.data_dir.join(format!(".tmp_{}", filename));

        fs::create_dir_all(&config.data_dir)?;

        let writer_props = WriterProperties::builder()
            .set_compression(compression_from_name(
                &config.compression,
                config.compression_level,
            ))
            .set_writer_version(WriterVersion::PARQUET_2_0)
            // Compaction rewrites blocks, so it must preserve the row-group
            // layout the scanner prunes on — otherwise compacted blocks quietly
            // lose the narrow-query speedup that flushed blocks have.
            .set_max_row_group_row_count(Some(config.row_group_size.max(1)))
            .build();

        let mut writer = ArrowWriter::try_new(
            File::create(&tmp_path)?,
            record_batch.schema(),
            Some(writer_props),
        )
        .map_err(|e| Error::Parquet(e.to_string()))?;

        writer
            .write(&record_batch)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        writer.close().map_err(|e| Error::Parquet(e.to_string()))?;

        fs::rename(&tmp_path, &final_path)?;
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
        })
    }
}
