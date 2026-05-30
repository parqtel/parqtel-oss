//! Storage engine abstraction layer.
//!
//! Provides the [`StorageEngine`] trait that decouples consumers from
//! the underlying storage implementation (Parquet, Lance, Iceberg, etc.).

pub mod parquet;
pub mod registry;

use std::collections::BTreeSet;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use crate::error::Result;
use crate::models::metrics::{DataPoint, Metric};
use crate::models::logs::LogRecord;
use crate::models::storage::SignalType;

/// Metadata returned after writing a batch of data.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WrittenBlockMeta {
    pub block_id: String,
    pub signal_type: SignalType,
    pub start_ns: i64,
    pub end_ns: i64,
    pub row_count: u64,
    pub size_bytes: u64,
    pub storage_backend: String,
}

/// Request parameters for scanning metrics.
#[derive(Debug, Clone)]
pub struct MetricScanRequest {
    pub metric_name: String,
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Request parameters for scanning logs.
#[derive(Debug, Clone)]
pub struct LogScanRequest {
    pub start_ns: i64,
    pub end_ns: i64,
}

/// Statistics from a compaction pass.
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    pub blocks_merged: u64,
    pub blocks_produced: u64,
    pub bytes_reclaimed: u64,
}

/// Snapshot of the storage index for metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageIndexSnapshot {
    pub metric_names: BTreeSet<String>,
    pub label_keys: BTreeSet<String>,
    pub time_range: Option<(i64, i64)>,
    pub block_count: u64,
    pub total_size_bytes: u64,
    pub backend_name: String,
}

/// Snapshot of log field information.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogFieldSnapshot {
    pub field_names: BTreeSet<String>,
    pub block_count: u64,
    pub backend_name: String,
}

/// Health status of the storage engine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageHealth {
    Ok { backend: String, metrics_blocks: u64, logs_blocks: u64 },
    Degraded { backend: String, reason: String },
}

/// The primary storage abstraction. All consumers depend on this trait,
/// never on a concrete implementation.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    /// Write a batch of metrics durably.
    async fn write_metrics_batch(&self, metrics: Vec<Metric>) -> Result<WrittenBlockMeta>;

    /// Write a batch of logs durably.
    async fn write_logs_batch(&self, logs: Vec<LogRecord>) -> Result<WrittenBlockMeta>;

    /// Scan metrics matching the request, returning all matching data points.
    async fn scan_metrics(&self, request: MetricScanRequest) -> Result<Vec<DataPoint>>;

    /// Scan logs matching the request, returning all matching log records.
    async fn scan_logs(&self, request: LogScanRequest) -> Result<Vec<LogRecord>>;

    /// Run one compaction pass on metrics.
    async fn compact_metrics(&self) -> Result<CompactionStats>;

    /// Run one compaction pass on logs.
    async fn compact_logs(&self) -> Result<CompactionStats>;

    /// Delete metrics older than the given nanosecond timestamp.
    async fn expire_metrics(&self, before_ns: i64) -> Result<u64>;

    /// Delete logs older than the given nanosecond timestamp.
    async fn expire_logs(&self, before_ns: i64) -> Result<u64>;

    /// Return a snapshot of the metric index.
    async fn metric_index_snapshot(&self) -> Result<StorageIndexSnapshot>;

    /// Return known log field names.
    async fn log_field_snapshot(&self) -> Result<LogFieldSnapshot>;

    /// Check storage health.
    async fn health_check(&self) -> Result<StorageHealth>;
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use super::parquet::ParquetStorageEngine;
    use super::registry::StorageEngineRegistry;
    use crate::config::BlockConfig;
    use crate::models::metrics::{MetricValue, MetricKind};
    use crate::models::labels::LabelSet;
    use tempfile::tempdir;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_parquet_engine_implements_trait() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();

        let engine = ParquetStorageEngine::new(config);
        let engine: Arc<dyn StorageEngine> = Arc::new(engine);

        let metrics: Vec<Metric> = (0..10).map(|i| Metric {
            name: "test_metric".into(),
            description: "".into(),
            unit: "".into(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::default(),
            data_points: vec![DataPoint {
                timestamp_ns: 1_000_000_000 + i * 1_000_000,
                value: MetricValue::Double(i as f64),
                labels: LabelSet::default(),
            }],
        }).collect();

        let meta = engine.write_metrics_batch(metrics).await.unwrap();
        assert_eq!(meta.row_count, 10);
        assert_eq!(meta.signal_type, SignalType::Metrics);

        let results = engine.scan_metrics(MetricScanRequest {
            metric_name: "test_metric".into(),
            start_ns: 0,
            end_ns: i64::MAX,
        }).await.unwrap();
        assert_eq!(results.len(), 10);
    }

    #[tokio::test]
    async fn test_health_check_returns_ok() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();

        let engine = ParquetStorageEngine::new(config);
        let health = engine.health_check().await.unwrap();
        assert!(matches!(health, StorageHealth::Ok { .. }));
    }

    #[tokio::test]
    async fn test_registry_resolves_parquet() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();

        let mut registry = StorageEngineRegistry::new();
        registry.register_parquet();
        let engine = registry.build("parquet", config);
        assert!(engine.is_ok());
    }

    #[tokio::test]
    async fn test_registry_rejects_unknown_backend() {
        let config = BlockConfig::default();
        let mut registry = StorageEngineRegistry::new();
        registry.register_parquet();
        let result = registry.build("clickhouse", config);
        assert!(result.is_err());
        let err = result.err().unwrap().to_string();
        assert!(err.to_lowercase().contains("unsupported"));
    }

    #[tokio::test]
    async fn test_index_snapshot_reflects_written_data() {
        let dir = tempdir().unwrap();
        let mut config = BlockConfig::default();
        config.data_dir = dir.path().to_path_buf();

        let engine = ParquetStorageEngine::new(config);

        let metrics = vec![Metric {
            name: "snapshot_metric".into(),
            description: "".into(),
            unit: "".into(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::default(),
            data_points: vec![DataPoint {
                timestamp_ns: 1_000_000_000,
                value: MetricValue::Double(42.0),
                labels: LabelSet::default(),
            }],
        }];

        engine.write_metrics_batch(metrics).await.unwrap();
        let snapshot = engine.metric_index_snapshot().await.unwrap();
        assert!(snapshot.metric_names.contains("snapshot_metric"));
        assert_eq!(snapshot.block_count, 1);
    }
}
