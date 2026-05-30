use std::collections::{HashSet, BTreeMap};
use std::fs::{self, File};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use uuid::Uuid;
use arrow2::io::parquet::read;
use arrow2::io::parquet::write::{self, CompressionOptions, Encoding, WriteOptions, Version};
use crate::error::{Error, Result};
use crate::models::storage::{BlockMetadata, StorageModel, SignalType};
use crate::models::metrics::{DataPoint, Metric, MetricKind};
use crate::models::logs::LogRecord;
use crate::models::labels::LabelSet;
use crate::config::BlockConfig;
use super::index::BlockIndex;

/// Background task that merges small adjacent blocks.
pub struct Compactor;

impl Compactor {
    pub async fn run_loop(index: Arc<RwLock<BlockIndex>>, config: BlockConfig) {
        let interval = Duration::from_secs(300);
        loop {
            tokio::time::sleep(interval).await;
            if let Err(e) = Self::compact_once(&index, &config).await {
                tracing::error!("Compaction failed: {}", e);
            }
        }
    }

    pub(crate) async fn compact_once(index: &Arc<RwLock<BlockIndex>>, config: &BlockConfig) -> Result<()> {
        let (to_compact, original_paths, signal_type) = {
            let idx = index.read().await;
            let mut small_blocks: Vec<_> = idx.blocks.iter()
                .filter(|b| b.row_count < 10000)
                .cloned()
                .collect();
            if small_blocks.len() < 2 { return Ok(()); }
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

    fn read_source_blocks(
        blocks: &[BlockMetadata],
        signal_type: SignalType,
    ) -> Result<(Vec<(String, MetricKind, LabelSet, DataPoint)>, Vec<LogRecord>)> {
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
            let metadata = read::read_metadata(&mut file).map_err(|e| Error::Parquet(e.to_string()))?;
            let schema = if signal_type == SignalType::Metrics {
                StorageModel::metrics_schema()
            } else {
                StorageModel::logs_schema()
            };
            let reader = read::FileReader::new(file, metadata.row_groups, schema, None, None, None);

            for chunk in reader {
                let chunk = chunk.map_err(|e| Error::Parquet(e.to_string()))?;
                for row in 0..chunk.len() {
                    if signal_type == SignalType::Metrics {
                        all_points.push(StorageModel::row_to_point(&chunk, row)?);
                    } else {
                        all_logs.push(StorageModel::row_to_log(&chunk, row)?);
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
                let end = all_points.last()
                    .ok_or_else(|| Error::Internal("No points".into()))?.3.timestamp_ns;
                let rows = all_points.len();
                let mut m_names = HashSet::new();
                let mut l_names = HashSet::new();
                let mut groups: BTreeMap<(String, MetricKind, LabelSet), Vec<DataPoint>> = BTreeMap::new();
                for (name, kind, resource, dp) in all_points {
                    m_names.insert(name.clone());
                    for l in resource.keys() { l_names.insert(l.clone()); }
                    for l in dp.labels.keys() { l_names.insert(l.clone()); }
                    groups.entry((name, kind, resource)).or_default().push(dp);
                }
                let metrics: Vec<_> = groups.into_iter().map(|((name, kind, resource), dps)| {
                    Metric { name, description: "".into(), unit: "".into(), kind, resource_attributes: resource, data_points: dps }
                }).collect();
                (StorageModel::metrics_to_chunk(&metrics)?, start, end, rows, m_names, l_names)
            } else {
                all_logs.sort_by_key(|l| l.timestamp_ns);
                let start = all_logs[0].timestamp_ns;
                let end = all_logs.last()
                    .ok_or_else(|| Error::Internal("No logs".into()))?.timestamp_ns;
                let rows = all_logs.len();
                let mut l_names = HashSet::new();
                for log in &all_logs {
                    for l in log.attributes.keys() { l_names.insert(l.clone()); }
                    for l in log.resource_attributes.keys() { l_names.insert(l.clone()); }
                }
                (StorageModel::logs_to_chunk(&all_logs)?, start, end, rows, HashSet::new(), l_names)
            };

        let filename = format!("{}_{}_{}.parquet", start_ts, end_ts, Uuid::new_v4().simple());
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
        let encodings: Vec<Vec<Encoding>> = schema.fields.iter().map(|f| {
            match f.data_type() {
                arrow2::datatypes::DataType::Dictionary(_, _, _) => vec![Encoding::RleDictionary],
                _ => vec![Encoding::Plain],
            }
        }).collect();

        let row_groups = write::RowGroupIterator::try_new(
            std::iter::once(Ok(chunk)), &schema, options, encodings,
        ).map_err(|e| Error::Parquet(e.to_string()))?;

        let mut writer = write::FileWriter::try_new(file, schema, options)
            .map_err(|e| Error::Parquet(e.to_string()))?;
        for group in row_groups {
            writer.write(group.map_err(|e| Error::Parquet(e.to_string()))?)
                .map_err(|e| Error::Parquet(e.to_string()))?;
        }
        writer.end(None).map_err(|e| Error::Parquet(e.to_string()))?;

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
