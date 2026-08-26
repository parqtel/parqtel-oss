use super::index::BlockIndex;
use crate::config::BlockConfig;
use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric, MetricKind};
use crate::models::storage::{BlockMetadata, SignalType, StorageModel};
use arrow2::io::parquet::read;
use arrow2::io::parquet::write::{self, CompressionOptions, Encoding, Version, WriteOptions};
use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;

/// Points grouped with their metric metadata, plus log records — the decoded
/// contents of source blocks awaiting compaction.
type DecodedBlocks = (
    Vec<(String, MetricKind, LabelSet, DataPoint)>,
    Vec<LogRecord>,
);

/// Background task that merges small adjacent blocks and implements tiered compaction.
/// Tier strategy:
///   - Small blocks (< 10K rows): merge up to 8 into one (existing behavior)
///   - Warm tier (blocks > 6h old, same signal): merge adjacent into ~6h blocks
///   - Cold tier (blocks > 24h old): merge adjacent into ~24h blocks
pub struct Compactor;

impl Compactor {
    pub async fn run_loop(index: Arc<RwLock<BlockIndex>>, config: BlockConfig) {
        let interval = Duration::from_secs(config.compaction_interval_secs.max(60));
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = Self::compact_once(&index, &config).await {
                tracing::error!("Compaction failed: {}", e);
            }
            // Tiered compaction for warm/cold data
            if let Err(e) = Self::compact_tiered(&index, &config).await {
                tracing::error!("Tiered compaction failed: {}", e);
            }
        }
    }

    pub(crate) async fn compact_once(
        index: &Arc<RwLock<BlockIndex>>,
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
                return Ok(());
            }
            let count = std::cmp::min(8, small_blocks.len());
            small_blocks.truncate(count);
            let paths: Vec<_> = small_blocks.iter().map(|b| b.path.clone()).collect();
            let signal = small_blocks[0].signal_type;
            (small_blocks, paths, signal)
        };

        let (all_points, all_logs) = Self::read_source_blocks(&to_compact, signal_type)?;

        if all_points.is_empty() && all_logs.is_empty() {
            let mut idx = index.write().await;
            for path in &original_paths {
                idx.blocks.retain(|b| &b.path != path);
            }
            idx.save()?;
            return Ok(());
        }

        let new_meta = Self::write_merged(config, signal_type, all_points, all_logs)?;

        let mut idx = index.write().await;
        for path in &original_paths {
            idx.blocks.retain(|b| &b.path != path);
        }
        idx.blocks.push(new_meta);
        idx.blocks.sort_by_key(|b| b.start_timestamp_ns);
        idx.save()?;

        for path in original_paths {
            let _ = fs::remove_file(path);
        }
        Ok(())
    }

    /// Tiered compaction: merge adjacent blocks of the same signal type into larger time frames.
    /// Warm tier (>6h old): target 6h blocks. Cold tier (>24h old): target 24h blocks.
    async fn compact_tiered(index: &Arc<RwLock<BlockIndex>>, config: &BlockConfig) -> Result<()> {
        let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let six_hours_ns = 6 * 3600 * 1_000_000_000i64;
        let twenty_four_hours_ns = 24 * 3600 * 1_000_000_000i64;

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
                // Limit merge group to 12 blocks per pass
                group.truncate(12);

                let paths: Vec<_> = group.iter().map(|b| b.path.clone()).collect();

                if *signal_type == SignalType::Traces {
                    // For traces, skip read_source_blocks (which only handles metrics/logs)
                    // and just leave them for now — trace compaction reads spans directly
                    continue;
                }

                let (all_points, all_logs) = Self::read_source_blocks(&group, *signal_type)?;
                if all_points.is_empty() && all_logs.is_empty() {
                    continue;
                }

                let new_meta = Self::write_merged(config, *signal_type, all_points, all_logs)?;

                let mut idx = index.write().await;
                for path in &paths {
                    idx.blocks.retain(|b| &b.path != path);
                }
                idx.blocks.push(new_meta);
                idx.blocks.sort_by_key(|b| b.start_timestamp_ns);
                idx.save()?;

                for path in paths {
                    let _ = fs::remove_file(path);
                }
                // Only one merge per signal per pass to avoid holding the lock too long
                break;
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

        for meta in blocks {
            let mut file = match File::open(&meta.path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!("Compaction source block not found: {:?}", meta.path);
                    continue;
                }
                Err(e) => return Err(Error::Io(e)),
            };
            let metadata =
                read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
            let schema = if signal_type == SignalType::Metrics {
                StorageModel::metrics_schema()
            } else {
                StorageModel::logs_schema()
            };
            let reader = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

            for chunk in reader {
                let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
                // Cache keys borrow from this chunk — recreate per chunk.
                let mut attr_cache = std::collections::HashMap::new();
                let mut res_cache = std::collections::HashMap::new();
                for row in 0..chunk.len() {
                    if signal_type == SignalType::Metrics {
                        all_points.push(StorageModel::row_to_point(&chunk, row)?);
                    } else {
                        all_logs.push(StorageModel::row_to_log(
                            &chunk,
                            row,
                            &mut attr_cache,
                            &mut res_cache,
                        )?);
                    }
                }
            }
        }
        Ok((all_points, all_logs))
    }

    fn write_merged(
        config: &BlockConfig,
        signal_type: SignalType,
        mut all_points: Vec<(String, MetricKind, LabelSet, DataPoint)>,
        mut all_logs: Vec<LogRecord>,
    ) -> Result<BlockMetadata> {
        let (chunk, start_ts, end_ts, row_count, metric_names, label_names) =
            if signal_type == SignalType::Metrics {
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
            } else {
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

        let file = File::create(&tmp_path)?;
        let schema = if signal_type == SignalType::Metrics {
            StorageModel::metrics_schema()
        } else {
            StorageModel::logs_schema()
        };
        let options = WriteOptions {
            write_statistics: true,
            compression: match config.compression.as_str() {
                "zstd" => CompressionOptions::Zstd(None),
                "snappy" => CompressionOptions::Snappy,
                "lz4" => CompressionOptions::Lz4Raw,
                _ => CompressionOptions::Uncompressed,
            },
            version: Version::V2,
            data_pagesize_limit: None,
        };
        let encodings: Vec<Vec<Encoding>> = schema
            .fields
            .iter()
            .map(|f| match f.data_type() {
                arrow2::datatypes::DataType::Dictionary(_, _, _) => vec![Encoding::RleDictionary],
                _ => vec![Encoding::Plain],
            })
            .collect();

        let row_groups = write::RowGroupIterator::try_new(
            std::iter::once(Ok(chunk)),
            &schema,
            options,
            encodings,
        )
        .map_err(|e| Error::Parquet(e.to_string()))?;

        let mut writer = write::FileWriter::try_new(file, schema, options)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        for group in row_groups {
            writer
                .write(group.map_err(|e| Error::Parquet(e.to_string()))?)
                .map_err(|e| Error::Parquet(e.to_string()))?;
        }
        writer
            .end(None)
            .map_err(|e| Error::Parquet(e.to_string()))?;

        fs::rename(&tmp_path, &final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names,
            label_names,
            signal_type,
        })
    }
}
