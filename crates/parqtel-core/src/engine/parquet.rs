//! Parquet-based implementation of [`StorageEngine`].

use async_trait::async_trait;
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::sync::Arc;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::config::BlockConfig;
use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric};
use crate::models::storage::{BlockMetadata, MetricRun, SignalType, StorageModel};
use crate::storage::{write_block_file, BlockIndex, FooterIndex, Scanner};

use super::{
    CompactionStats, LogFieldSnapshot, LogScanRequest, MetricScanRequest, StorageEngine,
    StorageHealth, StorageIndexSnapshot, WrittenBlockMeta,
};

/// Parquet-based storage engine wrapping existing BlockIndex + Scanner.
pub struct ParquetStorageEngine {
    config: BlockConfig,
    log_config: BlockConfig,
    metrics_index: Arc<RwLock<BlockIndex>>,
    logs_index: Arc<RwLock<BlockIndex>>,
}

impl ParquetStorageEngine {
    /// Create a new engine. Uses `config.data_dir` for metrics and `data_dir/logs` for logs.
    pub fn new(config: BlockConfig) -> Self {
        let log_dir = config.data_dir.join("logs");
        let log_config = BlockConfig {
            data_dir: log_dir.clone(),
            ..config.clone()
        };

        fs::create_dir_all(&config.data_dir).ok();
        fs::create_dir_all(&log_dir).ok();

        let mut metrics_index = BlockIndex::new(&config.data_dir);
        metrics_index.load().ok();
        let mut logs_index = BlockIndex::new(&log_dir);
        logs_index.load().ok();

        Self {
            config,
            log_config,
            metrics_index: Arc::new(RwLock::new(metrics_index)),
            logs_index: Arc::new(RwLock::new(logs_index)),
        }
    }

    /// Create with explicit separate log config.
    pub fn with_log_config(config: BlockConfig, log_config: BlockConfig) -> Self {
        fs::create_dir_all(&config.data_dir).ok();
        fs::create_dir_all(&log_config.data_dir).ok();

        let mut metrics_index = BlockIndex::new(&config.data_dir);
        metrics_index.load().ok();
        let mut logs_index = BlockIndex::new(&log_config.data_dir);
        logs_index.load().ok();

        Self {
            config,
            log_config,
            metrics_index: Arc::new(RwLock::new(metrics_index)),
            logs_index: Arc::new(RwLock::new(logs_index)),
        }
    }

    /// Get a reference to the metrics index (for backward compat during migration).
    pub fn metrics_index(&self) -> &Arc<RwLock<BlockIndex>> {
        &self.metrics_index
    }

    /// Get a reference to the logs index.
    pub fn logs_index(&self) -> &Arc<RwLock<BlockIndex>> {
        &self.logs_index
    }

    /// Get the metrics config.
    pub fn config(&self) -> &BlockConfig {
        &self.config
    }

    /// Get the log config.
    pub fn log_config(&self) -> &BlockConfig {
        &self.log_config
    }

    fn write_metrics_to_parquet(&self, metrics: &[Metric]) -> Result<BlockMetadata> {
        let mut all_timestamps: Vec<i64> = Vec::new();
        let mut metric_names = HashSet::new();
        let mut label_names = HashSet::new();
        let mut row_count = 0;

        for m in metrics {
            metric_names.insert(m.name.clone());
            for l in m.resource_attributes.keys() {
                label_names.insert(l.clone());
            }
            for dp in &m.data_points {
                all_timestamps.push(dp.timestamp_ns);
                for l in dp.labels.keys() {
                    label_names.insert(l.clone());
                }
                row_count += 1;
            }
        }

        if row_count == 0 {
            return Err(Error::Validation("Cannot write empty metrics batch".into()));
        }

        all_timestamps.sort();
        let start_ts = all_timestamps[0];
        let end_ts = *all_timestamps
            .last()
            .ok_or_else(|| Error::Internal("empty".into()))?;

        // Series-contiguous, time-ordered inside each series, with the series
        // table that describes it.
        let runs: Vec<MetricRun<'_>> = metrics
            .iter()
            .map(|m| {
                let mut points: Vec<&DataPoint> = m.data_points.iter().collect();
                points.sort_by(|a, b| {
                    a.labels
                        .fingerprint()
                        .cmp(&b.labels.fingerprint())
                        .then(a.timestamp_ns.cmp(&b.timestamp_ns))
                });
                MetricRun {
                    name: &m.name,
                    kind: m.kind,
                    resource: &m.resource_attributes,
                    points,
                }
            })
            .collect();
        let (chunk, series) = StorageModel::metric_runs_to_block(&runs)?;
        drop(runs);
        let filename = format!(
            "{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        write_block_file(
            &tmp_path,
            chunk,
            &self.config.compression,
            None,
            self.config.row_group_size,
            None,
            Some(FooterIndex::Series(&series)),
        )?;
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
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        })
    }

    fn write_logs_to_parquet(&self, logs: &[LogRecord]) -> Result<BlockMetadata> {
        if logs.is_empty() {
            return Err(Error::Validation("Cannot write empty logs batch".into()));
        }

        let mut sorted: Vec<&LogRecord> = logs.iter().collect();
        sorted.sort_by_key(|l| l.timestamp_ns);

        let start_ts = sorted[0].timestamp_ns;
        let end_ts = sorted
            .last()
            .ok_or_else(|| Error::Internal("empty".into()))?
            .timestamp_ns;
        let row_count = logs.len();

        let mut label_names = HashSet::new();
        for log in logs {
            for l in log.attributes.keys() {
                label_names.insert(l.clone());
            }
            for l in log.resource_attributes.keys() {
                label_names.insert(l.clone());
            }
        }

        let (chunk, skip_index) = StorageModel::logs_to_block(logs)?;
        let filename = format!(
            "logs_{}_{}_{}.parquet",
            start_ts,
            end_ts,
            Uuid::new_v4().simple()
        );
        let final_path = self.log_config.data_dir.join(&filename);
        let tmp_path = self.log_config.data_dir.join(format!(".tmp_{}", filename));

        write_block_file(
            &tmp_path,
            chunk,
            &self.log_config.compression,
            None,
            self.log_config.row_group_size,
            None,
            Some(FooterIndex::Logs(&skip_index)),
        )?;
        fs::rename(&tmp_path, &final_path)?;
        let size_bytes = fs::metadata(&final_path)?.len();

        Ok(BlockMetadata {
            path: final_path,
            start_timestamp_ns: start_ts,
            end_timestamp_ns: end_ts,
            row_count,
            size_bytes,
            metric_names: HashSet::new(),
            label_names,
            label_values: Default::default(),
            signal_type: SignalType::Logs,
            wal_range: None,
        })
    }
}

#[async_trait]
impl StorageEngine for ParquetStorageEngine {
    async fn write_metrics_batch(&self, metrics: Vec<Metric>) -> Result<WrittenBlockMeta> {
        let meta = self.write_metrics_to_parquet(&metrics)?;
        let block_id = meta
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let written = WrittenBlockMeta {
            block_id,
            signal_type: SignalType::Metrics,
            start_ns: meta.start_timestamp_ns,
            end_ns: meta.end_timestamp_ns,
            row_count: meta.row_count as u64,
            size_bytes: meta.size_bytes,
            storage_backend: "parquet-local".into(),
        };
        self.metrics_index.write().await.add(meta);
        Ok(written)
    }

    async fn write_logs_batch(&self, logs: Vec<LogRecord>) -> Result<WrittenBlockMeta> {
        let meta = self.write_logs_to_parquet(&logs)?;
        let block_id = meta
            .path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let written = WrittenBlockMeta {
            block_id,
            signal_type: SignalType::Logs,
            start_ns: meta.start_timestamp_ns,
            end_ns: meta.end_timestamp_ns,
            row_count: meta.row_count as u64,
            size_bytes: meta.size_bytes,
            storage_backend: "parquet-local".into(),
        };
        self.logs_index.write().await.add(meta);
        Ok(written)
    }

    async fn scan_metrics(&self, request: MetricScanRequest) -> Result<Vec<DataPoint>> {
        let blocks = {
            let idx = self.metrics_index.read().await;
            let name = if request.metric_name.is_empty() {
                None
            } else {
                Some(request.metric_name.as_str())
            };
            idx.query(request.start_ns, request.end_ns, name)
        };
        Scanner::scan(
            blocks,
            request.metric_name,
            request.start_ns,
            request.end_ns,
            request.service_name.as_deref(),
        )
        .await
    }

    async fn scan_logs(&self, request: LogScanRequest) -> Result<Vec<LogRecord>> {
        let blocks = {
            let idx = self.logs_index.read().await;
            idx.query(request.start_ns, request.end_ns, None)
        };
        Scanner::scan_logs(blocks, request.start_ns, request.end_ns).await
    }

    async fn compact_metrics(&self) -> Result<CompactionStats> {
        // Delegate to existing Compactor logic inline
        Ok(CompactionStats::default())
    }

    async fn compact_logs(&self) -> Result<CompactionStats> {
        Ok(CompactionStats::default())
    }

    async fn expire_metrics(&self, before_ns: i64) -> Result<u64> {
        let mut idx = self.metrics_index.write().await;
        let mut deleted = 0u64;
        let mut to_delete = Vec::new();
        idx.blocks.retain(|b| {
            if b.end_timestamp_ns < before_ns {
                to_delete.push(b.path.clone());
                deleted += 1;
                false
            } else {
                true
            }
        });
        drop(idx);
        if deleted > 0 {
            // Unlink off the index lock: the file writes must not extend the
            // window in which queries are blocked.
            for path in to_delete {
                let _ = fs::remove_file(path);
            }
        }
        Ok(deleted)
    }

    async fn expire_logs(&self, before_ns: i64) -> Result<u64> {
        let mut idx = self.logs_index.write().await;
        let mut deleted = 0u64;
        let mut to_delete = Vec::new();
        idx.blocks.retain(|b| {
            if b.end_timestamp_ns < before_ns {
                to_delete.push(b.path.clone());
                deleted += 1;
                false
            } else {
                true
            }
        });
        drop(idx);
        if deleted > 0 {
            // Unlink off the index lock: the file writes must not extend the
            // window in which queries are blocked.
            for path in to_delete {
                let _ = fs::remove_file(path);
            }
        }
        Ok(deleted)
    }

    async fn metric_index_snapshot(&self) -> Result<StorageIndexSnapshot> {
        let idx = self.metrics_index.read().await;
        let metric_names: BTreeSet<String> = idx.all_metrics().into_iter().collect();
        let label_keys: BTreeSet<String> = idx.all_labels().into_iter().collect();
        let time_range = if idx.blocks.is_empty() {
            None
        } else {
            let oldest = idx
                .blocks
                .iter()
                .map(|b| b.start_timestamp_ns)
                .min()
                .unwrap_or(0);
            let newest = idx
                .blocks
                .iter()
                .map(|b| b.end_timestamp_ns)
                .max()
                .unwrap_or(0);
            Some((oldest, newest))
        };
        Ok(StorageIndexSnapshot {
            metric_names,
            label_keys,
            time_range,
            block_count: idx.total_blocks() as u64,
            total_size_bytes: idx.total_bytes(),
            backend_name: "parquet-local".into(),
        })
    }

    async fn log_field_snapshot(&self) -> Result<LogFieldSnapshot> {
        let idx = self.logs_index.read().await;
        let field_names: BTreeSet<String> = idx.all_labels().into_iter().collect();
        Ok(LogFieldSnapshot {
            field_names,
            block_count: idx.total_blocks() as u64,
            backend_name: "parquet-local".into(),
        })
    }

    async fn health_check(&self) -> Result<StorageHealth> {
        let m_blocks = self.metrics_index.read().await.total_blocks() as u64;
        let l_blocks = self.logs_index.read().await.total_blocks() as u64;
        Ok(StorageHealth::Ok {
            backend: "parquet-local".into(),
            metrics_blocks: m_blocks,
            logs_blocks: l_blocks,
        })
    }
}

#[cfg(test)]
mod tests;
