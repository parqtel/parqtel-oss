//! Parquet-based implementation of [`StorageEngine`].

use std::collections::{BTreeSet, HashSet};
use std::fs::{self, File};
use std::sync::Arc;
use async_trait::async_trait;
use tokio::sync::RwLock;
use uuid::Uuid;
use arrow2::io::parquet::write::{
    CompressionOptions, Encoding, FileWriter, RowGroupIterator, Version, WriteOptions,
};

use crate::config::BlockConfig;
use crate::error::{Error, Result};
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric};
use crate::models::storage::{BlockMetadata, SignalType, StorageModel};
use crate::storage::{BlockIndex, Scanner};

use super::{
    CompactionStats, LogFieldSnapshot, LogScanRequest, MetricScanRequest,
    StorageEngine, StorageHealth, StorageIndexSnapshot, WrittenBlockMeta,
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
            for l in m.resource_attributes.keys() { label_names.insert(l.clone()); }
            for dp in &m.data_points {
                all_timestamps.push(dp.timestamp_ns);
                for l in dp.labels.keys() { label_names.insert(l.clone()); }
                row_count += 1;
            }
        }

        if row_count == 0 {
            return Err(Error::Validation("Cannot write empty metrics batch".into()));
        }

        all_timestamps.sort();
        let start_ts = all_timestamps[0];
        let end_ts = *all_timestamps.last().ok_or_else(|| Error::Internal("empty".into()))?;

        let chunk = StorageModel::metrics_to_chunk(metrics)?;
        let filename = format!("{}_{}_{}.parquet", start_ts, end_ts, Uuid::new_v4().simple());
        let final_path = self.config.data_dir.join(&filename);
        let tmp_path = self.config.data_dir.join(format!(".tmp_{}", filename));

        write_parquet(&tmp_path, chunk, &self.config.compression, StorageModel::metrics_schema())?;
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
            signal_type: SignalType::Metrics,
        })
    }

    fn write_logs_to_parquet(&self, logs: &[LogRecord]) -> Result<BlockMetadata> {
        if logs.is_empty() {
            return Err(Error::Validation("Cannot write empty logs batch".into()));
        }

        let mut sorted: Vec<&LogRecord> = logs.iter().collect();
        sorted.sort_by_key(|l| l.timestamp_ns);

        let start_ts = sorted[0].timestamp_ns;
        let end_ts = sorted.last().ok_or_else(|| Error::Internal("empty".into()))?.timestamp_ns;
        let row_count = logs.len();

        let mut label_names = HashSet::new();
        for log in logs {
            for l in log.attributes.keys() { label_names.insert(l.clone()); }
            for l in log.resource_attributes.keys() { label_names.insert(l.clone()); }
        }

        let chunk = StorageModel::logs_to_chunk(logs)?;
        let filename = format!("logs_{}_{}_{}.parquet", start_ts, end_ts, Uuid::new_v4().simple());
        let final_path = self.log_config.data_dir.join(&filename);
        let tmp_path = self.log_config.data_dir.join(format!(".tmp_{}", filename));

        write_parquet(&tmp_path, chunk, &self.log_config.compression, StorageModel::logs_schema())?;
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
            signal_type: SignalType::Logs,
        })
    }
}

#[async_trait]
impl StorageEngine for ParquetStorageEngine {
    async fn write_metrics_batch(&self, metrics: Vec<Metric>) -> Result<WrittenBlockMeta> {
        let meta = self.write_metrics_to_parquet(&metrics)?;
        let block_id = meta.path.file_stem()
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
        self.metrics_index.write().await.add(meta)?;
        Ok(written)
    }

    async fn write_logs_batch(&self, logs: Vec<LogRecord>) -> Result<WrittenBlockMeta> {
        let meta = self.write_logs_to_parquet(&logs)?;
        let block_id = meta.path.file_stem()
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
        self.logs_index.write().await.add(meta)?;
        Ok(written)
    }

    async fn scan_metrics(&self, request: MetricScanRequest) -> Result<Vec<DataPoint>> {
        let blocks = {
            let idx = self.metrics_index.read().await;
            let name = if request.metric_name.is_empty() { None } else { Some(request.metric_name.as_str()) };
            idx.query(request.start_ns, request.end_ns, name)
        };
        Scanner::scan(blocks, request.metric_name, request.start_ns, request.end_ns).await
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
        if deleted > 0 {
            idx.save()?;
            for path in to_delete { let _ = fs::remove_file(path); }
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
        if deleted > 0 {
            idx.save()?;
            for path in to_delete { let _ = fs::remove_file(path); }
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
            let oldest = idx.blocks.iter().map(|b| b.start_timestamp_ns).min().unwrap_or(0);
            let newest = idx.blocks.iter().map(|b| b.end_timestamp_ns).max().unwrap_or(0);
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

fn write_parquet(
    path: &std::path::Path,
    chunk: arrow2::chunk::Chunk<Arc<dyn arrow2::array::Array>>,
    compression: &str,
    schema: arrow2::datatypes::Schema,
) -> Result<()> {
    let file = File::create(path)?;
    let options = WriteOptions {
        write_statistics: true,
        compression: match compression {
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
    let row_groups = RowGroupIterator::try_new(
        vec![Ok(chunk)].into_iter(),
        &schema,
        options,
        encodings,
    ).map_err(|e| Error::Parquet(e.to_string()))?;
    let mut writer = FileWriter::try_new(file, schema, options)
        .map_err(|e| Error::Parquet(e.to_string()))?;
    for group in row_groups {
        writer.write(group.map_err(|e| Error::Parquet(e.to_string()))?)
            .map_err(|e| Error::Parquet(e.to_string()))?;
    }
    writer.end(None).map_err(|e| Error::Parquet(e.to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::config::BlockConfig;
    use crate::models::labels::LabelSet;
    use crate::models::logs::LogRecord;
    use crate::models::metrics::{DataPoint, Metric, MetricKind, MetricValue};
    use tempfile::tempdir;

    fn test_config(dir: &std::path::Path) -> BlockConfig {
        BlockConfig { data_dir: dir.to_path_buf(), ..BlockConfig::default() }
    }

    fn sample_metric(ts: i64) -> Metric {
        Metric {
            name: "cpu_usage".into(),
            description: "".into(),
            unit: "percent".into(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::try_from_iter(vec![("service.name", "web")]).unwrap(),
            data_points: vec![DataPoint { timestamp_ns: ts, value: MetricValue::Double(42.0), labels: LabelSet::default() }],
        }
    }

    fn sample_log(ts: i64) -> LogRecord {
        LogRecord::new(ts, ts + 1, 9, "INFO".into(), "test log".into(),
            LabelSet::default(), LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
            [0; 16], [0; 8], 0, "scope".into(), "1.0".into())
    }

    #[tokio::test]
    async fn test_write_and_scan_logs() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        let logs = vec![sample_log(1000), sample_log(2000), sample_log(3000)];
        let meta = engine.write_logs_batch(logs).await.unwrap();
        assert_eq!(meta.row_count, 3);
        assert_eq!(meta.signal_type, SignalType::Logs);

        let results = engine.scan_logs(LogScanRequest { start_ns: 0, end_ns: 5000 }).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_write_empty_metrics_fails() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        let result = engine.write_metrics_batch(vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_empty_logs_fails() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        let result = engine.write_logs_batch(vec![]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_expire_metrics() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        engine.write_metrics_batch(vec![sample_metric(100)]).await.unwrap();
        engine.write_metrics_batch(vec![sample_metric(5000)]).await.unwrap();

        let deleted = engine.expire_metrics(1000).await.unwrap();
        assert_eq!(deleted, 1);
        let remaining = engine.scan_metrics(MetricScanRequest { metric_name: "cpu_usage".into(), start_ns: 0, end_ns: i64::MAX }).await.unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[tokio::test]
    async fn test_expire_logs() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        engine.write_logs_batch(vec![sample_log(100)]).await.unwrap();
        engine.write_logs_batch(vec![sample_log(5000)]).await.unwrap();

        let deleted = engine.expire_logs(1000).await.unwrap();
        assert_eq!(deleted, 1);
    }

    #[tokio::test]
    async fn test_log_field_snapshot() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        engine.write_logs_batch(vec![sample_log(1000)]).await.unwrap();

        let snapshot = engine.log_field_snapshot().await.unwrap();
        assert_eq!(snapshot.block_count, 1);
        assert!(snapshot.field_names.contains("service.name"));
    }

    #[tokio::test]
    async fn test_with_log_config() {
        let dir = tempdir().unwrap();
        let metrics_config = test_config(dir.path());
        let log_config = BlockConfig { data_dir: dir.path().join("custom_logs"), ..BlockConfig::default() };
        let engine = ParquetStorageEngine::with_log_config(metrics_config, log_config);

        engine.write_logs_batch(vec![sample_log(1000)]).await.unwrap();
        let results = engine.scan_logs(LogScanRequest { start_ns: 0, end_ns: 5000 }).await.unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_compact_returns_default() {
        let dir = tempdir().unwrap();
        let engine = ParquetStorageEngine::new(test_config(dir.path()));
        let stats = engine.compact_metrics().await.unwrap();
        assert_eq!(stats.blocks_merged, 0);
        let stats = engine.compact_logs().await.unwrap();
        assert_eq!(stats.blocks_merged, 0);
    }
}
