//! Tests for the `parquet` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::config::BlockConfig;
use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::{DataPoint, Metric, MetricKind, MetricValue};
use tempfile::tempdir;

fn test_config(dir: &std::path::Path) -> BlockConfig {
    BlockConfig {
        data_dir: dir.to_path_buf(),
        ..BlockConfig::default()
    }
}

fn sample_metric(ts: i64) -> Metric {
    Metric {
        name: "cpu_usage".into(),
        description: "".into(),
        unit: "percent".into(),
        kind: MetricKind::Gauge,
        resource_attributes: LabelSet::try_from_iter(vec![("service.name", "web")]).unwrap(),
        data_points: vec![DataPoint {
            timestamp_ns: ts,
            value: MetricValue::Double(42.0),
            labels: LabelSet::default(),
        }],
    }
}

fn sample_log(ts: i64) -> LogRecord {
    LogRecord::new(
        ts,
        ts + 1,
        9,
        "INFO".into(),
        "test log".into(),
        LabelSet::default(),
        LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
        [0; 16],
        [0; 8],
        0,
        "scope".into(),
        "1.0".into(),
    )
}

#[tokio::test]
async fn test_write_and_scan_logs() {
    let dir = tempdir().unwrap();
    let engine = ParquetStorageEngine::new(test_config(dir.path()));
    let logs = vec![sample_log(1000), sample_log(2000), sample_log(3000)];
    let meta = engine.write_logs_batch(logs).await.unwrap();
    assert_eq!(meta.row_count, 3);
    assert_eq!(meta.signal_type, SignalType::Logs);

    let results = engine
        .scan_logs(LogScanRequest {
            start_ns: 0,
            end_ns: 5000,
        })
        .await
        .unwrap();
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
    engine
        .write_metrics_batch(vec![sample_metric(100)])
        .await
        .unwrap();
    engine
        .write_metrics_batch(vec![sample_metric(5000)])
        .await
        .unwrap();

    let deleted = engine.expire_metrics(1000).await.unwrap();
    assert_eq!(deleted, 1);
    let remaining = engine
        .scan_metrics(MetricScanRequest {
            metric_name: "cpu_usage".into(),
            start_ns: 0,
            end_ns: i64::MAX,
            service_name: None,
        })
        .await
        .unwrap();
    assert_eq!(remaining.len(), 1);
}

#[tokio::test]
async fn test_expire_logs() {
    let dir = tempdir().unwrap();
    let engine = ParquetStorageEngine::new(test_config(dir.path()));
    engine
        .write_logs_batch(vec![sample_log(100)])
        .await
        .unwrap();
    engine
        .write_logs_batch(vec![sample_log(5000)])
        .await
        .unwrap();

    let deleted = engine.expire_logs(1000).await.unwrap();
    assert_eq!(deleted, 1);
}

#[tokio::test]
async fn test_log_field_snapshot() {
    let dir = tempdir().unwrap();
    let engine = ParquetStorageEngine::new(test_config(dir.path()));
    engine
        .write_logs_batch(vec![sample_log(1000)])
        .await
        .unwrap();

    let snapshot = engine.log_field_snapshot().await.unwrap();
    assert_eq!(snapshot.block_count, 1);
    assert!(snapshot.field_names.contains("service.name"));
}

#[tokio::test]
async fn test_with_log_config() {
    let dir = tempdir().unwrap();
    let metrics_config = test_config(dir.path());
    let log_config = BlockConfig {
        data_dir: dir.path().join("custom_logs"),
        ..BlockConfig::default()
    };
    let engine = ParquetStorageEngine::with_log_config(metrics_config, log_config);

    engine
        .write_logs_batch(vec![sample_log(1000)])
        .await
        .unwrap();
    let results = engine
        .scan_logs(LogScanRequest {
            start_ns: 0,
            end_ns: 5000,
        })
        .await
        .unwrap();
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
