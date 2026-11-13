//! Measures peak resident memory of one wide range query, with and without
//! streaming step evaluation.
//!
//! `eval_steps` retained one instant vector per step until the last step
//! finished, so peak memory scaled with steps x series rather than series.
//!
//! Run: cargo run --release -p parqtel-query --example bench_query_memory
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_core::{DataPoint, LabelSet, MetricValue};
use parqtel_query::executor::QueryExecutor;
use std::sync::Arc;
use std::time::Instant;

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let exec = QueryExecutor::new(
        Arc::new(tokio::sync::RwLock::new(parqtel_core::BlockIndex::new(
            dir.path(),
        ))),
        Arc::new(tokio::sync::RwLock::new(parqtel_core::BlockIndex::new(
            dir.path(),
        ))),
        dir.path().join("traces"),
    );

    const SERIES: usize = 1000;
    const POINTS: usize = 60;
    let buffer = exec.memory_buffer();
    for s in 0..SERIES {
        let points: Vec<DataPoint> = (0..POINTS)
            .map(|i| {
                DataPoint::new(
                    (i as i64 + 1) * 60_000_000_000,
                    MetricValue::Double(i as f64),
                    LabelSet::try_from_iter(vec![
                        ("host", format!("h{}", s % 50)),
                        ("region", format!("r{}", s % 10)),
                        ("idx", s.to_string()),
                    ])
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();
        buffer.push_metrics("m", &points).await;
    }

    // A plain selector, so every input series is also an *output* series: the
    // retained per-step vectors scale with output cardinality, which is where
    // streaming matters. An aggregating query would collapse 1000 inputs to 50
    // outputs and understate the effect.
    let expr = parqtel_query::parser::parse_expr("m").unwrap();
    let start_ns = 1_000_000_000;
    let end_ns = 60 * 60_000_000_000;
    let step_ns = 1_000_000_000; // 1s -> ~3600 steps

    // No warm-up, deliberately. A warm-up query touches exactly the pages the
    // measured query then wants, the allocator keeps them, and the second
    // query's growth no longer reflects its own peak. One query per process,
    // measured once.
    let before_rss = peak_rss_bytes();
    let t0 = Instant::now();
    let res = exec
        .execute_ast(&expr, start_ns, end_ns, Some(step_ns))
        .await
        .unwrap();
    let elapsed = t0.elapsed();
    let peak = peak_rss_bytes();

    println!(
        "series={} samples_per_series={}",
        res.series.len(),
        res.series[0].samples.len()
    );
    println!("steps            : {}", (end_ns - start_ns) / step_ns);
    println!("wall             : {elapsed:?}");
    println!("rss before query : {:.1} MB", before_rss as f64 / 1e6);
    println!("rss after query  : {:.1} MB", peak as f64 / 1e6);
    println!(
        "resident growth  : {:.1} MB",
        peak.saturating_sub(before_rss) as f64 / 1e6
    );
    println!(
        "output size      : {:.1} MB ({} series x {} samples)",
        (res.series.len() * res.series[0].samples.len() * 16) as f64 / 1e6,
        res.series.len(),
        res.series[0].samples.len()
    );
}

/// Peak RSS from /proc/self/status, in bytes.
fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}
