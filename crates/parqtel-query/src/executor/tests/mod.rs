//! Query executor tests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::series::{build_series_data, v_to_f64, BuiltSeries};
use super::*;
use crate::matcher::LabelMatcher;
use crate::plan::QueryPlan;
use parqtel_core::engine::parquet::ParquetStorageEngine;
use parqtel_core::{BlockConfig, StorageEngine};
use parqtel_core::{LabelSet, MetricValue};
use std::path::{Path, PathBuf};
use tempfile::tempdir;

mod correlate;
mod labels;
mod logs;
mod metrics;
mod series;

/// Persists one of the storage engine's in-memory indexes to its sidecar.
///
/// `BlockIndex::add` is in-memory only — the sidecar write belongs to
/// `BlockIndexStore` so it never runs under the query read path — so a
/// test that loads an index back from disk must persist it explicitly,
/// exactly as the server's persist loop does at runtime.
async fn persist_engine_index(index: &Arc<RwLock<BlockIndex>>) {
    let index = index.clone();
    tokio::task::spawn_blocking(move || {
        let idx = index.blocking_read();
        let store = parqtel_core::BlockIndexStore::new(&idx);
        store.mark_dirty();
        parqtel_core::storage::persist_blocking(&idx, &store)
    })
    .await
    .unwrap()
    .unwrap();
}

/// Helper: create a ParquetStorageEngine-backed executor with real parquet files.
async fn setup_with_data() -> (QueryExecutor, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    };
    // Kept as a concrete handle so the test can reach the engine's own
    // indexes to persist them before loading them back from disk.
    let engine = Arc::new(ParquetStorageEngine::new(config));
    let storage: Arc<dyn StorageEngine> = engine.clone();

    // Write metrics
    let m1 = parqtel_core::Metric {
        name: "cpu".into(),
        kind: parqtel_core::MetricKind::Gauge,
        resource_attributes: LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap(),
        data_points: vec![
            parqtel_core::DataPoint::new(
                1000,
                parqtel_core::MetricValue::Double(10.0),
                LabelSet::try_from_iter(vec![("host", "h1")]).unwrap(),
            )
            .unwrap(),
            parqtel_core::DataPoint::new(
                2000,
                parqtel_core::MetricValue::Double(20.0),
                LabelSet::try_from_iter(vec![("host", "h1")]).unwrap(),
            )
            .unwrap(),
            parqtel_core::DataPoint::new(
                3000,
                parqtel_core::MetricValue::Double(30.0),
                LabelSet::try_from_iter(vec![("host", "h2")]).unwrap(),
            )
            .unwrap(),
        ],
        ..Default::default()
    };
    let m2 = parqtel_core::Metric {
        name: "mem".into(),
        kind: parqtel_core::MetricKind::Gauge,
        data_points: vec![parqtel_core::DataPoint::new(
            1500,
            parqtel_core::MetricValue::Double(512.0),
            LabelSet::try_from_iter(vec![("host", "h1")]).unwrap(),
        )
        .unwrap()],
        ..Default::default()
    };
    storage.write_metrics_batch(vec![m1, m2]).await.unwrap();

    // Write logs
    let log = parqtel_core::LogRecord::new(
        2000,
        2000,
        9,
        "INFO".into(),
        "request handled".into(),
        LabelSet::try_from_iter(vec![("service_name", "web")]).unwrap(),
        LabelSet::try_from_iter(vec![("k8s_namespace", "prod")]).unwrap(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    );
    storage.write_logs_batch(vec![log]).await.unwrap();

    let _snapshot = storage.metric_index_snapshot().await.unwrap();
    persist_engine_index(engine.metrics_index()).await;
    let mut index = BlockIndex::new(dir.path());
    index.load().unwrap();

    let log_dir = dir.path().join("logs");
    persist_engine_index(engine.logs_index()).await;
    let mut log_index = BlockIndex::new(&log_dir);
    log_index.load().unwrap();

    let index = Arc::new(RwLock::new(index));
    let log_index = Arc::new(RwLock::new(log_index));
    let trace_data_dir = dir.path().join("traces");
    std::fs::create_dir_all(&trace_data_dir).ok();
    let exec = QueryExecutor::new(index, log_index, trace_data_dir);
    (exec, dir)
}

fn point(ts: i64, host: &str, v: MetricValue) -> parqtel_core::DataPoint {
    parqtel_core::DataPoint::new(
        ts,
        v,
        // A fresh handle every call, as separate blocks produce.
        LabelSet::try_from_iter(vec![("host", host)]).unwrap(),
    )
    .unwrap()
}
