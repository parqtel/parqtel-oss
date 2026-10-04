//! Measures what prunes a single-metric query, and what each mechanism costs.
//!
//! Two mechanisms exist, and the order matters:
//!
//! 1. **Row-group statistics on `metric_name`.** Rows are written grouped by
//!    metric name, so a row group's min and max for that column are usually the
//!    same value — an exact, free answer, with nothing read from the filter
//!    region. This is what actually prunes.
//! 2. **Bloom filters**, consulted only when the statistics cannot decide.
//!
//! So the interesting comparison is three-way: neither (the old world),
//! statistics alone, and statistics + bloom. Blocks are read through the
//! production `Scanner::scan` path, so this measures the real query rather than
//! a reimplementation of it.
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

fn write_block(
    path: &std::path::Path,
    metrics: &[Metric],
    with_stats: bool,
    with_bloom: bool,
) -> Result<Duration> {
    let chunk = StorageModel::metrics_to_chunk(metrics)?;
    let file = std::fs::File::create(path)?;
    let mut builder = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(
            parquet::basic::ZstdLevel::default(),
        ))
        .set_writer_version(parquet::file::properties::WriterVersion::PARQUET_2_0)
        .set_max_row_group_row_count(Some(ROW_GROUP))
        .set_statistics_enabled(if with_stats {
            parquet::file::properties::EnabledStatistics::Page
        } else {
            parquet::file::properties::EnabledStatistics::None
        });
    if with_bloom {
        use parquet::schema::types::ColumnPath;
        builder = builder
            .set_column_bloom_filter_enabled(ColumnPath::from("metric_name"), true)
            .set_column_bloom_filter_enabled(ColumnPath::from("service_name"), true);
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
        let out = Scanner::scan(vec![meta.clone()], metric.to_string(), 0, i64::MAX, None).await?;
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
    let metrics = build_metrics();
    let rows = METRICS * POINTS_PER_METRIC;

    // Three configurations, oldest first.
    let configs: [(&str, bool, bool); 3] = [
        ("neither (old blocks)", false, false),
        ("statistics only", true, false),
        ("statistics + bloom", true, true),
    ];

    println!("block: {METRICS} metrics x {POINTS_PER_METRIC} points = {rows} rows");
    println!(
        "
{:<24} {:>10} {:>10} {:>10}",
        "configuration", "size", "write", "query"
    );
    let mut baseline: Option<(u64, Duration)> = None;
    // Kept for the full-block scan below, which needs a real path.
    let mut stats_only_path = dir.path().join("placeholder.parquet");
    for (label, stats, bloom) in configs {
        let path = dir
            .path()
            .join(format!("{}.parquet", label.replace([' ', '(', ')'], "_")));
        let write = write_block(&path, &metrics, stats, bloom)?;
        if stats && !bloom {
            stats_only_path = path.clone();
        }
        let size = std::fs::metadata(&path)
            .map_err(parqtel_core::Error::Io)?
            .len();
        let (points, query) = best_scan(&path, "metric_7", 4).await?;
        assert_eq!(
            points, POINTS_PER_METRIC,
            "{label}: must return the same points regardless of pruning metadata"
        );
        println!(
            "{label:<24} {:>8.2} MB {:>8.0} ms {:>8.2} ms",
            size as f64 / 1e6,
            write.as_secs_f64() * 1e3,
            query.as_secs_f64() * 1e3
        );
        if baseline.is_none() {
            baseline = Some((size, query));
        }
    }

    let (base_size, base_query) = baseline.expect("at least one configuration");
    println!("\nvs \"neither\":");
    for (label, _, _) in configs.iter().skip(1) {
        let path = dir
            .path()
            .join(format!("{}.parquet", label.replace([' ', '(', ')'], "_")));
        let size = std::fs::metadata(&path)
            .map_err(parqtel_core::Error::Io)?
            .len();
        let (_, query) = best_scan(&path, "metric_7", 4).await?;
        println!(
            "  {label:<24} {:>8.1}% size, {:>6.1}x query",
            100.0 * (size as f64 - base_size as f64) / base_size as f64,
            base_query.as_secs_f64() / query.as_secs_f64()
        );
    }

    // Multi-tenant shape: one metric, 8 services, one row group each. This is
    // where metric pruning cannot help and service pruning is the selective
    // dimension - the common case for a shared cluster.
    let svc_metrics: Vec<Metric> = (0..8)
        .map(|s| Metric {
            name: "http_requests".to_string(),
            kind: parqtel_core::models::metrics::MetricKind::Sum,
            resource_attributes: parqtel_core::LabelSet::try_from_iter(vec![
                ("service.name", format!("svc-{s}")),
                ("cluster", "prod".to_string()),
            ])
            .unwrap(),
            data_points: (0..POINTS_PER_METRIC)
                .map(|i| {
                    DataPoint::new(
                        i as i64 * 1_000_000_000 + 1,
                        MetricValue::Double(i as f64),
                        parqtel_core::LabelSet::try_from_iter(vec![(
                            "route".to_string(),
                            "r0".to_string(),
                        )])
                        .unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        })
        .collect();
    let svc_path = dir.path().join("services.parquet");
    write_block(&svc_path, &svc_metrics, true, true)?;
    let svc_meta = metadata(
        &svc_path,
        svc_metrics.iter().map(|m| m.data_points.len()).sum(),
    );
    let mut svc_best = f64::MAX;
    let mut metric_only_best = f64::MAX;
    for _ in 0..4 {
        // With a service: metric pruning cannot discriminate (all 8 row groups
        // are the same metric), so the service predicate is what narrows it.
        let started = Instant::now();
        let out = Scanner::scan(
            vec![svc_meta.clone()],
            "http_requests".to_string(),
            0,
            i64::MAX,
            Some("svc-3"),
        )
        .await?;
        svc_best = svc_best.min(started.elapsed().as_secs_f64());
        assert_eq!(out.len(), POINTS_PER_METRIC, "svc-3's rows only");

        // Same query without the service predicate: decodes the whole block.
        let started = Instant::now();
        let out = Scanner::scan(
            vec![svc_meta.clone()],
            "http_requests".to_string(),
            0,
            i64::MAX,
            None,
        )
        .await?;
        metric_only_best = metric_only_best.min(started.elapsed().as_secs_f64());
        assert_eq!(out.len(), POINTS_PER_METRIC * 8, "all eight services");
    }
    println!("\nmulti-tenant: 1 metric x 8 services, querying service=svc-3");
    println!(
        "  without the service predicate: {:.2} ms (decodes all {} rows)",
        metric_only_best * 1e3,
        POINTS_PER_METRIC * 8
    );
    println!(
        "  with service pruning       : {:.2} ms ({} rows)",
        svc_best * 1e3,
        POINTS_PER_METRIC
    );
    println!(
        "  speedup                   : {:.1}x",
        metric_only_best / svc_best
    );

    // Column projection: the metrics scan reads 7 of the block's 15 columns.
    // `bench_bloom` is run twice with PARQTEL_NO_PROJECTION toggled, so the
    // comparison is the real scan path either way.
    let _ = &svc_metrics;

    // Worst case for projection: a scan that must decode every row group, so
    // the eight skipped columns are actually paid for. This is the shape where
    // projection should matter most - a dashboard panel that groups many
    // metrics, or an unindexed old block.
    let meta_all = metadata(&stats_only_path, rows);
    let mut wide_best = f64::MAX;
    for _ in 0..4 {
        let started = Instant::now();
        let out = Scanner::scan(vec![meta_all.clone()], String::new(), 0, i64::MAX, None).await?;
        wide_best = wide_best.min(started.elapsed().as_secs_f64());
        assert_eq!(out.len(), rows, "a full scan returns every row");
    }
    println!(
        "\nfull-block scan (no metric filter, all {rows} rows): {:.2} ms",
        wide_best * 1e3
    );

    // A metric that is not in the block decodes nothing at all.
    let bloomed = dir.path().join("statistics___bloom.parquet");
    let (absent, t) = best_scan(&bloomed, "no_such_metric", 2).await?;
    println!(
        "\nabsent metric: {absent} points, {:.2} ms",
        t.as_secs_f64() * 1e3
    );
    assert_eq!(absent, 0);

    Ok(())
}
