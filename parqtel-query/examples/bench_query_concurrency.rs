//! Measures how concurrent queries interact on a single-threaded runtime.
//!
//! Before the CPU half was offloaded, one query occupied the only worker for
//! its whole duration, so concurrent queries ran strictly one at a time. With
//! the offload they overlap on the blocking pool.
//!
//! A single-threaded runtime is deliberate: that is the configuration where
//! inline CPU work does the most damage, and it makes the effect measurable
//! without needing several cores.
//!
//! Run: cargo run --release -p parqtel-query --example bench_query_concurrency
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_core::{DataPoint, LabelSet, MetricValue};
use parqtel_query::executor::QueryExecutor;
use std::sync::Arc;
use std::time::Instant;

const START_NS: i64 = 1_000_000_000;
/// Wide window with a fine step: ~20 000 evaluation steps across every series.
/// The point is to make the CPU half *dominate* - with only a few hundred steps
/// the buffer scan and clone dominate, and the benchmark cannot tell whether
/// the evaluator was offloaded.
const END_NS: i64 = 21_000_000_000;
const STEP_NS: i64 = 1_000_000;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let dir = tempfile::tempdir().unwrap();
    let exec = Arc::new(QueryExecutor::new(
        Arc::new(tokio::sync::RwLock::new(parqtel_core::BlockIndex::new(
            dir.path(),
        ))),
        Arc::new(tokio::sync::RwLock::new(parqtel_core::BlockIndex::new(
            dir.path(),
        ))),
        dir.path().join("traces"),
    ));

    const SERIES: usize = 300;
    const POINTS: usize = 400;
    let buffer = exec.memory_buffer();
    for s in 0..SERIES {
        let points: Vec<DataPoint> = (0..POINTS)
            .map(|i| {
                DataPoint::new(
                    (i as i64 + 1) * 1_000_000_000,
                    MetricValue::Double(i as f64),
                    LabelSet::try_from_iter(vec![
                        ("host", format!("h{}", s % 20)),
                        ("idx", s.to_string()),
                    ])
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();
        buffer.push_metrics("bench", &points).await;
    }

    let expr = parqtel_query::parser::parse_expr("sum by (host) (bench)").unwrap();

    // Warm up so neither measurement pays first-call costs.
    for _ in 0..2 {
        exec.execute_ast(&expr, START_NS, END_NS, Some(STEP_NS))
            .await
            .unwrap();
    }

    // Single query, for reference.
    let t0 = Instant::now();
    let single_result = exec
        .execute_ast(&expr, START_NS, END_NS, Some(STEP_NS))
        .await
        .unwrap();
    let single = t0.elapsed();

    const CONCURRENT: usize = 4;

    // Four queries one after another on the same runtime: the serial cost.
    // Concurrent wall time is only meaningful against this.
    let tseq = Instant::now();
    for _ in 0..CONCURRENT {
        exec.execute_ast(&expr, START_NS, END_NS, Some(STEP_NS))
            .await
            .unwrap();
    }
    let sequential = tseq.elapsed();

    let t1 = Instant::now();
    let mut handles = Vec::with_capacity(CONCURRENT);
    for _ in 0..CONCURRENT {
        let exec = exec.clone();
        let expr = expr.clone();
        handles.push(tokio::spawn(async move {
            exec.execute_ast(&expr, START_NS, END_NS, Some(STEP_NS))
                .await
                .unwrap()
        }));
    }
    let mut results = Vec::with_capacity(CONCURRENT);
    for h in handles {
        results.push(h.await.unwrap());
    }
    let concurrent = t1.elapsed();

    // Every concurrent query must return exactly the same series as the single
    // one, or the overlap is producing wrong answers.
    for r in &results {
        assert_eq!(r.series.len(), single_result.series.len());
        assert_eq!(r.total_series_count, single_result.total_series_count);
    }

    println!(
        "series={} series_count={} (all {} concurrent results identical)",
        single_result.series.len(),
        single_result.total_series_count,
        results.len()
    );
    println!("steps={}", (END_NS - START_NS) / STEP_NS);
    println!("single query        : {single:?}");
    println!("{CONCURRENT} sequential        : {sequential:?}");
    println!("{CONCURRENT} concurrent        : {concurrent:?}");
    println!(
        "per-query in {CONCURRENT}x       : {:?}",
        concurrent / CONCURRENT as u32
    );
    println!(
        "concurrent / sequential: {:.2}x  (1.0 = free parallelism, {:.0} = no overlap)",
        concurrent.as_secs_f64() / sequential.as_secs_f64(),
        CONCURRENT
    );
    println!(
        "speedup from concurrency: {:.2}x",
        sequential.as_secs_f64() / concurrent.as_secs_f64()
    );
}
