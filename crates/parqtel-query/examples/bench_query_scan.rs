//! On-disk, high-cardinality metric scan benchmark for Phase 3.
//!
//! Run each mode in a fresh process:
//! `cargo run --release -p parqtel-query --example bench_query_scan -- fixture <dir>`
//! `cargo run --release -p parqtel-query --example bench_query_scan -- measure target <dir>`
#![allow(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use parqtel_core::models::storage::MetricRun;
use parqtel_core::models::storage::{BlockMetadata, SignalType};
use parqtel_core::storage::{write_block_file, FooterIndex};
use parqtel_core::{BlockIndex, DataPoint, LabelSet, MetricKind, MetricValue, Scanner};
use parqtel_query::executor::QueryExecutor;
use std::collections::{BTreeMap, HashSet};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

const TARGET: &str = "erlang_vm_allocators";
const TARGET_SERIES: usize = 40_000;
const FILLER_METRICS: usize = 3_000;
const SERIES_PER_FILLER: usize = 70;
const SERIES_PER_BLOCK: usize = TARGET_SERIES + FILLER_METRICS * SERIES_PER_FILLER;
const ROWS_PER_SERIES: usize = 10;
const ROWS_PER_BLOCK: usize = SERIES_PER_BLOCK * ROWS_PER_SERIES;
const ROW_STEP_NS: i64 = 30_000_000_000;
const BLOCK_SPACING_NS: i64 = 300_000_000_000;
const WINDOW_NS: i64 = 900_000_000_000;
const STEP_NS: i64 = 30_000_000_000;
const BASE_NS: i64 = 1_700_000_000_000_000_000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.as_slice() {
        [mode, dir] if mode == "fixture" => write_fixture(Path::new(dir)),
        [mode, query, dir] if mode == "measure" => measure(query, Path::new(dir)).await,
        _ => Err("usage: bench_query_scan fixture <dir> | measure <query> <dir>".into()),
    }
}

fn write_fixture(dir: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(dir)?;
    let resource = LabelSet::default();
    let metric_names: Vec<String> = std::iter::once(TARGET.to_string())
        .chain((0..FILLER_METRICS).map(|i| format!("filler_{i:04}")))
        .collect();
    let mut index = BlockIndex::new(dir);

    for block in 0..4 {
        let base_ns = BASE_NS + block as i64 * BLOCK_SPACING_NS;
        let mut points = Vec::with_capacity(ROWS_PER_BLOCK);
        let mut ranges = Vec::with_capacity(metric_names.len());

        let first = points.len();
        for series in 0..TARGET_SERIES {
            let pod = if series % 100 == 7 {
                "pod-7".to_string()
            } else {
                format!("pod-{}", series % 100)
            };
            let labels = LabelSet::try_from_iter(vec![
                ("instance", format!("target-{series:05}")),
                ("pod", pod),
            ])?;
            push_series(&mut points, labels, base_ns, series);
        }
        ranges.push((first, points.len()));

        for metric in 0..FILLER_METRICS {
            let first = points.len();
            for series in 0..SERIES_PER_FILLER {
                let labels = LabelSet::try_from_iter(vec![
                    ("instance", format!("f{metric:04}-{series:02}")),
                    ("pod", format!("pod-f{metric:04}")),
                ])?;
                push_series(&mut points, labels, base_ns, series);
            }
            ranges.push((first, points.len()));
        }

        let runs: Vec<MetricRun<'_>> = ranges
            .iter()
            .enumerate()
            .map(|(metric, (first, end))| MetricRun {
                name: &metric_names[metric],
                kind: MetricKind::Gauge,
                resource: &resource,
                points: points[*first..*end].iter().collect(),
            })
            .collect();
        let (batch, table) =
            parqtel_core::models::storage::StorageModel::metric_runs_to_block(&runs)?;
        let path = dir.join(format!("metrics-{block}.parquet"));
        write_block_file(
            &path,
            batch,
            "zstd",
            None,
            100_000,
            None,
            Some(FooterIndex::Series(&table)),
        )?;
        let end_ns = base_ns + (ROWS_PER_SERIES as i64 - 1) * ROW_STEP_NS;
        index.add(BlockMetadata {
            path: path.clone(),
            start_timestamp_ns: base_ns,
            end_timestamp_ns: end_ns,
            row_count: ROWS_PER_BLOCK,
            size_bytes: fs::metadata(&path)?.len(),
            metric_names: metric_names.iter().cloned().collect(),
            label_names: HashSet::from(["instance".to_string(), "pod".to_string()]),
            label_values: BTreeMap::new(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        });
    }

    fs::write(index.sidecar_path(), index.serialize()?)?;
    Ok(())
}

fn push_series(points: &mut Vec<DataPoint>, labels: LabelSet, base_ns: i64, series: usize) {
    for row in 0..ROWS_PER_SERIES {
        points.push(
            DataPoint::new(
                base_ns + row as i64 * ROW_STEP_NS,
                MetricValue::Double((series + row) as f64),
                labels.clone(),
            )
            .expect("benchmark timestamps are positive"),
        );
    }
}

async fn measure(query: &str, dir: &Path) -> Result<(), Box<dyn Error>> {
    let mut index = BlockIndex::new(dir);
    index.load()?;
    let index = Arc::new(RwLock::new(index));
    let executor = Arc::new(
        QueryExecutor::new(
            index,
            Arc::new(RwLock::new(BlockIndex::new(dir))),
            dir.join("traces"),
        )
        .with_result_limits(1_000, 10_000),
    );
    let before = Scanner::scan_metrics_snapshot();
    let started = Instant::now();

    let (rows_considered, points, series, total_series) = match query {
        "target" | "selective" => {
            let expression = if query == "target" {
                TARGET.to_string()
            } else {
                format!("{TARGET}{{pod=\"pod-7\"}}")
            };
            let expr = parqtel_query::parser::parse_expr(&expression)?;
            let result = executor
                .execute_ast(&expr, BASE_NS, BASE_NS + WINDOW_NS, Some(STEP_NS))
                .await?;
            result_counts(&[result])
        }
        "two-panels" => {
            let expr = parqtel_query::parser::parse_expr(TARGET)?;
            let second = BASE_NS + BLOCK_SPACING_NS;
            let (left, right) = tokio::join!(
                executor.execute_ast(&expr, BASE_NS, BASE_NS + WINDOW_NS, Some(STEP_NS)),
                executor.execute_ast(&expr, second, second + WINDOW_NS, Some(STEP_NS)),
            );
            result_counts(&[left?, right?])
        }
        _ => return Err(format!("unknown query: {query}").into()),
    };

    let elapsed_ms = started.elapsed().as_millis();
    let after = Scanner::scan_metrics_snapshot();
    let hwm_mib = peak_hwm_bytes()? as f64 / (1024.0 * 1024.0);
    println!(
        "query={query} ms={elapsed_ms} hwm_mib={hwm_mib:.1} rows_considered={rows_considered} points={points} footer_misses={} series={series} total_series={total_series}",
        after.footer_cache_misses.saturating_sub(before.footer_cache_misses),
    );
    Ok(())
}

fn result_counts(results: &[parqtel_query::models::QueryResult]) -> (u64, usize, usize, usize) {
    results.iter().fold((0, 0, 0, 0), |mut total, result| {
        total.0 += result.points_scanned;
        total.1 += result.series.iter().map(|s| s.samples.len()).sum::<usize>();
        total.2 += result.series.len();
        total.3 += result.total_series_count;
        total
    })
}

fn peak_hwm_bytes() -> Result<u64, Box<dyn Error>> {
    let status = fs::read_to_string("/proc/self/status")?;
    let kib = status
        .lines()
        .find(|line| line.starts_with("VmHWM:"))
        .and_then(|line| line.split_whitespace().nth(1))
        .ok_or("VmHWM missing from /proc/self/status")?
        .parse::<u64>()?;
    Ok(kib * 1024)
}
