//! Ingestion service tests.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::common::shard_for;
use super::flush::FlushOrder;
use super::*;
use bytes::Bytes;
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::WalSyncMode;
use parqtel_core::wal::{WalPosition, WalWriter};
use parqtel_core::{BlockConfig, ContentionMetrics, LogBlockConfig, Metric, SignalType, Span};
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::mpsc;

mod flush;
mod rotator;
mod services;
mod wal;

/// Small block config so a handful of points forces a flush.
fn tiny_metrics_config(dir: &std::path::Path) -> BlockConfig {
    BlockConfig {
        data_dir: dir.to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    }
}

/// Runs an index task the way `main.rs` does: add the block, then release
/// the flush waiting on durability.
///
/// A flush now blocks until the sidecar holding its block is written, so a
/// test that produces blocks without one would stall until the handshake
/// times out. Reproducing the real task keeps the tests honest about the
/// ordering as well.
fn spawn_index_task(mut rx: mpsc::UnboundedReceiver<PendingIndex>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(pending) = rx.recv().await {
            if let Some(ack) = pending.durable {
                let _ = ack.send(());
            }
        }
    })
}

fn log_at(ts: i64, body: &str) -> parqtel_core::LogRecord {
    parqtel_core::LogRecord::new(
        ts,
        ts,
        9,
        "INFO".into(),
        body.into(),
        parqtel_core::LabelSet::default(),
        parqtel_core::LabelSet::default(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    )
}

/// A minimal span, for the WAL tests.
fn test_span(start: i64) -> Span {
    Span {
        trace_id: [7u8; 16],
        span_id: [start as u8; 8],
        trace_state: String::new(),
        name: "op".into(),
        kind: 2,
        start_time_ns: start,
        end_time_ns: start + 50,
        attributes: parqtel_core::LabelSet::default(),
        events: Vec::new(),
        links: Vec::new(),
        status: parqtel_core::models::traces::SpanStatus {
            code: 1,
            message: String::new(),
        },
        parent_span_id: [0u8; 8],
        flags: 1,
    }
}

/// A one-point metric at `ts` nanoseconds, named `name`.
fn metric_named(name: &str, ts: i64) -> Metric {
    Metric {
        name: name.into(),
        kind: parqtel_core::MetricKind::Gauge,
        data_points: vec![parqtel_core::DataPoint::new(
            ts.max(1),
            parqtel_core::MetricValue::Double(ts as f64),
            parqtel_core::LabelSet::default(),
        )
        .unwrap()],
        ..Default::default()
    }
}

fn one_point_metric(name: &str, ts: i64) -> Metric {
    Metric {
        name: name.into(),
        kind: parqtel_core::MetricKind::Gauge,
        data_points: vec![parqtel_core::DataPoint::new(
            ts,
            parqtel_core::MetricValue::Double(1.0),
            parqtel_core::LabelSet::default(),
        )
        .unwrap()],
        ..Default::default()
    }
}

#[tokio::test]
async fn live_log_ingest_applies_pipeline_and_forwards_metrics() {
    use parqtel_pipeline::rule::registry::RuleRegistry;
    use parqtel_pipeline::rule::schema::{
        MaskRule, PipelineDefinition, PipelineMatch, PipelineStage, StageType,
    };

    let buffer = parqtel_core::MemoryBuffer::new();
    let (tx, _rx) = mpsc::unbounded_channel::<PendingIndex>();
    let (metric_tx, mut metric_rx) = mpsc::channel(4);

    let registry = RuleRegistry::new();
    registry
        .add_pipeline(PipelineDefinition {
            name: "mask-and-count".into(),
            description: None,
            match_config: Some(PipelineMatch {
                signal: "logs".into(),
                conditions: Vec::new(),
            }),
            stages: vec![
                PipelineStage {
                    stage_type: StageType::Masker,
                    name: "mask".into(),
                    processor: None,
                    source_field: None,
                    pattern: None,
                    target_fields: None,
                    on_parse_failure: None,
                    fields: None,
                    conditions: None,
                    metric_name: None,
                    metric_type: None,
                    value_field: None,
                    dimensions: None,
                    condition: None,
                    histogram_buckets: None,
                    rules: Some(vec![MaskRule {
                        field: "body".into(),
                        pattern: "secret-\\d+".into(),
                        replacement: "[MASKED]".into(),
                    }]),
                    action: None,
                    destination: None,
                },
                PipelineStage {
                    stage_type: StageType::MetricExtract,
                    name: "count".into(),
                    processor: None,
                    source_field: None,
                    pattern: None,
                    target_fields: None,
                    on_parse_failure: None,
                    fields: None,
                    conditions: None,
                    metric_name: Some("log_lines_total".into()),
                    metric_type: Some("counter".into()),
                    value_field: Some("constant:1".into()),
                    dimensions: Some(vec!["service_name".into()]),
                    condition: None,
                    histogram_buckets: None,
                    rules: None,
                    action: None,
                    destination: None,
                },
            ],
        })
        .unwrap();

    let svc = LogIngestionService::new(LogBlockConfig::default(), tx)
        .with_memory_buffer(buffer.clone())
        .with_pipelines(registry, metric_tx);
    svc.ingest_logs_live(vec![log_at(1_000, "token secret-123")])
        .await
        .unwrap();

    let stored = buffer.scan_logs(0, i64::MAX).await;
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].body, "token [MASKED]");

    let metrics = metric_rx.recv().await.unwrap();
    assert_eq!(metrics.len(), 1);
    assert_eq!(metrics[0].name, "log_lines_total");
    assert_eq!(metrics[0].data_points.len(), 1);
}
