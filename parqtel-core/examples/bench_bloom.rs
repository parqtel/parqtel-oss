//! Measures what bloom filters buy a single-metric query, and what they cost.
//!
//! Row-group statistics only narrow on **time**. A metrics block interleaves
//! every metric it holds, so a query for one metric finds every row group in
//! the window and decodes all of them. A bloom filter on `metric_name` answers
//! "could this row group contain this value?" without decoding a page.
//!
//! Both blocks are read through the production `Scanner::scan` path, so the
//! comparison is the real query path and not a reimplementation of it. The
//! block written *without* bloom filters also stands in for every block written
//! before this change, which is exactly the case that must keep working.
//!
//! Run: cargo run --release -p parqtel-core --example bench_bloom
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_core::error::Result;
use parqtel_core::models::metrics::{DataPoint, Metric, MetricKind, MetricValue};
use parqtel_core::models::storage::{BlockMetadata, SignalType, StorageModel};
use parqtel_core::storage::Scanner;
use std::collections::HashSet;
use std::time::{Duration, Instant};

/// One metric name per row group, so a single-metric query is the worst case
/// for time-only pruning: every row group overlaps the query window.
const METRICS: usize = 200;
const POINTS_PER_METRIC: usize = 500;
const ROW_GROUP: usize = 500;

fn build_metrics() -> Vec<Metric> {
    (0..METRICS)
        .map(|m| Metric {
            name: format!("metric_{m}"),
            kind: MetricKind::Gauge,
            data_points: (0..POINTS_PER_METRIC)
                .map(|i| {
                    DataPoint::new(
                        i as i64 * 1_000_000_000 + 1,
                        MetricValue::Double(m as f64 + i as f64 * 0.001),
                        parqtel_core::LabelSet::try_from_iter(vec![
                            ("host", format!("h{}", i % 20)),
                            ("region", format!("r{}", i % 5)),
                        ])
                        .unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

fn write_block(path: &std::path::Path, metrics: &[Metric], with_bloom: bool) -> Result<Duration> {
    let chunk = StorageModel::metrics_to_chunk(metrics)?;
    let file = std::fs::File::create(path)?;
    let mut builder = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(ROW_GROUP));
    if with_bloom {
        use parquet::schema::types::ColumnPath;
        builder = builder
            .set_column_bloom_filter_enabled(ColumnPath::from("metric_name"), true)
            .set_column_bloom_filter_enabled(ColumnPath::from("service_name"), true)
            .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
            .set_column_index_truncate_length(Some(64));
    }
    let props = builder.build();
    let started = Instant::now();
    let mut writer =
        parquet::arrow::arrow_writer::ArrowWriter::try_new(file, chunk.schema(), Some(props))
            .map_err(|e| parqtel_core::Error::Parquet(e.to_string()))?;
    writer
        .write(&chunk)
        .map_err(|e| parqtel_core::Error::Parquet(e.to_string()))?;
    writer
        .close()
        .map_err(|e| parqtel_core::Error::Parquet(e.to_string()))?;
    Ok(started.elapsed())
}

fn metadata(path: &std::path::Path, rows: usize) -> BlockMetadata {
    BlockMetadata {
        path: path.to_path_buf(),
        start_timestamp_ns: 1,
        end_timestamp_ns: (POINTS_PER_METRIC as i64) * 1_000_000_000,
        row_count: rows,
        size_bytes: std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
    }
}

/// Runs one single-metric scan through the production path, returning the rows
/// decoded and the best wall time over `runs`.
async fn best_scan(path: &std::path::Path, metric: &str, runs: usize) -> Result<(usize, Duration)> {
    let meta = metadata(path, METRICS * POINTS_PER_METRIC);
    let mut best = Duration::from_secs(u64::MAX);
    let mut points = 0;
    for _ in 0..runs {
        let started = Instant::now();
        let out = Scanner::scan(vec![meta.clone()], metric.to_string(), 0, i64::MAX).await?;
        let elapsed = started.elapsed();
        if elapsed < best {
            best = elapsed;
        }
        points = out.len();
    }
    Ok((points, best))
}

#[tokio::main]
async fn main() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let plain = dir.path().join("plain.parquet");
    let bloomed = dir.path().join("bloom.parquet");
    let metrics = build_metrics();
    let rows = METRICS * POINTS_PER_METRIC;

    let plain_write = write_block(&plain, &metrics, false)?;
    let bloom_write = write_block(&bloomed, &metrics, true)?;
    let plain_size = std::fs::metadata(&plain)
        .map_err(parqtel_core::Error::Io)?
        .len();
    let bloom_size = std::fs::metadata(&bloomed)
        .map_err(parqtel_core::Error::Io)?
        .len();

    println!("block: {METRICS} metrics x {POINTS_PER_METRIC} points = {rows} rows");
    println!(
        "  size  : {:.2} MB without bloom -> {:.2} MB with bloom ({:+.1}%)",
        plain_size as f64 / 1e6,
        bloom_size as f64 / 1e6,
        100.0 * (bloom_size as f64 - plain_size as f64) / plain_size as f64
    );
    println!(
        "  write : {:.0} ms without bloom -> {:.0} ms with bloom ({:+.1}%)",
        plain_write.as_secs_f64() * 1e3,
        bloom_write.as_secs_f64() * 1e3,
        100.0 * (bloom_write.as_secs_f64() - plain_write.as_secs_f64()) / plain_write.as_secs_f64()
    );

    println!("\nsingle-metric query (one metric of {METRICS}), best of 4:");
    let (plain_pts, plain_t) = best_scan(&plain, "metric_7", 4).await?;
    let (bloom_pts, bloom_t) = best_scan(&bloomed, "metric_7", 4).await?;
    println!(
        "  without bloom: {plain_pts} points decoded, {:.2} ms",
        plain_t.as_secs_f64() * 1e3
    );
    println!(
        "  with bloom   : {bloom_pts} points decoded, {:.2} ms",
        bloom_t.as_secs_f64() * 1e3
    );
    println!(
        "  speedup      : {:.1}x  (both must decode the same {} points)",
        plain_t.as_secs_f64() / bloom_t.as_secs_f64(),
        plain_pts
    );
    assert_eq!(
        plain_pts, bloom_pts,
        "bloom filters must not change how many points a query returns"
    );

    // A metric that is not in the block: with bloom filters this should not
    // decode anything at all.
    let (absent_pts, absent_t) = best_scan(&bloomed, "no_such_metric", 2).await?;
    println!(
        "\nabsent metric with bloom: {absent_pts} points, {:.2} ms",
        absent_t.as_secs_f64() * 1e3
    );
    assert_eq!(absent_pts, 0);

    Ok(())
}
