//! Storage-layer tests: scanning, pruning, indexes, compaction and retention.
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::models::storage::{BlockMetadata, SignalType, StorageModel};
use crate::models::{DataPoint, LabelSet, LogRecord, Metric, MetricKind, MetricValue, Span};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};
use std::collections::HashSet;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use tempfile::tempdir;

mod compaction;
mod index;
mod logs;
mod projection;
mod pruning;
mod retention;
mod scan;

/// Helper: write a real parquet metrics file using new parquet/arrow APIs.
fn write_metrics_parquet(path: &std::path::Path, metrics: &[Metric]) {
    let chunk = StorageModel::metrics_to_chunk(metrics).unwrap();
    let file = fs::File::create(path).unwrap();

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
    writer.write(&chunk).unwrap();
    writer.close().unwrap();
}

/// Helper: write a real parquet logs file.
fn write_logs_parquet(path: &std::path::Path, logs: &[LogRecord]) {
    let chunk = StorageModel::logs_to_chunk(logs).unwrap();
    let file = fs::File::create(path).unwrap();

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
    writer.write(&chunk).unwrap();
    writer.close().unwrap();
}

fn write_traces_parquet(path: &std::path::Path, spans: &[Span]) {
    let chunk = StorageModel::traces_to_chunk(spans).unwrap();
    let file = fs::File::create(path).unwrap();
    let writer_props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();
    let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
    writer.write(&chunk).unwrap();
    writer.close().unwrap();
}

fn test_span(start: i64) -> Span {
    Span {
        trace_id: [1u8; 16],
        span_id: [start as u8; 8],
        trace_state: String::new(),
        name: "op".into(),
        kind: 2,
        start_time_ns: start,
        end_time_ns: start + 1000,
        attributes: LabelSet::try_from_iter(vec![(
            "http.status_code".to_string(),
            "200".to_string(),
        )])
        .unwrap(),
        events: Vec::new(),
        links: Vec::new(),
        status: crate::models::traces::SpanStatus {
            code: 1,
            message: String::new(),
        },
        parent_span_id: [0u8; 8],
        flags: 1,
    }
}

/// Helper: write a real parquet metrics file with an explicit row-group
/// size, so tests can create blocks that span several row groups.
fn write_metrics_parquet_rg(path: &std::path::Path, metrics: &[Metric], rg_rows: usize) {
    let chunk = StorageModel::metrics_to_chunk(metrics).unwrap();
    let file = fs::File::create(path).unwrap();

    let writer_props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(rg_rows))
        .build();

    let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(writer_props)).unwrap();
    writer.write(&chunk).unwrap();
    writer.close().unwrap();
}

/// Builds a metrics block of `rows` points, 1 ns apart, in 10 series.
fn prune_test_metrics(rows: usize) -> Vec<Metric> {
    vec![Metric {
        name: "prune.cpu".into(),
        kind: MetricKind::Gauge,
        data_points: (0..rows)
            .map(|i| {
                DataPoint::new(
                    i as i64 + 1,
                    MetricValue::Double(i as f64),
                    LabelSet::try_from_iter(vec![("host", format!("h{}", i % 10))]).unwrap(),
                )
                .unwrap()
            })
            .collect(),
        ..Default::default()
    }]
}

/// Writes a metrics block with explicit control over which read-path
/// metadata is present, so each pruning mechanism can be tested in
/// isolation. A test that enabled everything would pass while proving
/// nothing about which mechanism did the work.
fn write_block_with(
    path: &std::path::Path,
    metrics: &[Metric],
    rg_rows: usize,
    stats: bool,
    bloom: bool,
) {
    let chunk = StorageModel::metrics_to_chunk(metrics).unwrap();
    let file = fs::File::create(path).unwrap();
    let mut builder = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(rg_rows))
        .set_statistics_enabled(if stats {
            parquet::file::properties::EnabledStatistics::Chunk
        } else {
            parquet::file::properties::EnabledStatistics::None
        });
    if bloom {
        use parquet::schema::types::ColumnPath;
        builder = builder
            .set_column_bloom_filter_enabled(ColumnPath::from("metric_name"), true)
            .set_column_bloom_filter_enabled(ColumnPath::from("service_name"), true);
    }
    let mut writer = ArrowWriter::try_new(file, chunk.schema(), Some(builder.build())).unwrap();
    writer.write(&chunk).unwrap();
    writer.close().unwrap();
}
