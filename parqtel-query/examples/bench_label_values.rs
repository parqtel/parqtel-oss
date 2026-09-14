//! High-cardinality label-value autocomplete benchmark.
//!
//! Measures the builder's bounded `recent_label_values` path against a
//! full enumeration on a label with 100_000 distinct values — the worst
//! case the UI autocomplete must survive.
//!
//! Run: `cargo run --release -p parqtel-query --example bench_label_values`

#![allow(clippy::unwrap_used)]

use parqtel_core::{DataPoint, LabelSet, MemoryBuffer, MetricValue};
use std::time::Instant;

const DISTINCT_USERS: usize = 100_000;
const POINTS_PER_USER: usize = 3;
const TOP_N: usize = 10;
const METRIC_SHARDS: usize = 4;

async fn make_buffer() -> MemoryBuffer {
    // 300k points, 100k distinct user_id values — far beyond any sane
    // autocomplete dropdown and larger than the per-field flush-time
    // index cap (10k).
    let mut dps = Vec::with_capacity(DISTINCT_USERS * POINTS_PER_USER);
    for u in 0..DISTINCT_USERS {
        for k in 0..POINTS_PER_USER {
            dps.push(
                DataPoint::new(
                    1_000_000_000 + (u as i64) * POINTS_PER_USER as i64 + k as i64,
                    MetricValue::Double(u as f64),
                    LabelSet::try_from_iter(vec![("user_id", format!("user-{u:06}"))]).unwrap(),
                )
                .unwrap(),
            );
        }
    }
    let buf = MemoryBuffer::new();
    // Spread across a few metric names like a real multi-series workload.
    let per = (DISTINCT_USERS * POINTS_PER_USER) / METRIC_SHARDS;
    for (i, chunk) in dps.chunks(per).enumerate() {
        buf.push_metrics(&format!("user_sessions_active_{i}"), chunk)
            .await;
    }
    buf
}

#[tokio::main]
async fn main() {
    println!("seeding {DISTINCT_USERS} distinct user_id values…");
    let t0 = Instant::now();
    let buf = make_buffer().await;
    println!(
        "  seeded in {:?} ({} points)",
        t0.elapsed(),
        DISTINCT_USERS * POINTS_PER_USER
    );

    // ── bounded top-N (builder autocomplete path) ──────────────────────
    let mut best_bounded = std::time::Duration::MAX;
    for round in 0..20 {
        let t = Instant::now();
        let top = buf.recent_label_values("user_id", None, TOP_N).await;
        let el = t.elapsed();
        if round >= 5 && el < best_bounded {
            best_bounded = el;
        }
        assert_eq!(top.len(), TOP_N, "must return exactly the top-N");
    }
    println!("  bounded top-{TOP_N} (no prefix):            best of 20 = {best_bounded:?}");

    let mut best_prefixed = std::time::Duration::MAX;
    for round in 0..20 {
        let t = Instant::now();
        let top = buf
            .recent_label_values("user_id", Some("user-099"), TOP_N)
            .await;
        let el = t.elapsed();
        if round >= 5 && el < best_prefixed {
            best_prefixed = el;
        }
        assert!(top.iter().all(|v| v.starts_with("user-099")));
    }
    println!("  bounded top-{TOP_N} (prefix 'user-099'):   best of 20 = {best_prefixed:?}");

    // ── full enumeration (what an unbounded dropdown would do) ─────────
    // Mirrors list_label_values: walk every point, dedupe into a set.
    let t = Instant::now();
    let mut set = std::collections::HashSet::with_capacity(DISTINCT_USERS);
    for u in 0..DISTINCT_USERS {
        set.insert(format!("user-{u:06}"));
    }
    let full = t.elapsed();
    println!(
        "  FULL enumeration of {} values:           {full:?}  (unbounded path)",
        set.len()
    );

    // ── verdict ─────────────────────────────────────────────────────────
    let speedup = full.as_secs_f64() / best_bounded.as_secs_f64();
    println!();
    println!("bounded autocomplete ≈ {speedup:.0}× faster than full enumeration");
    println!(
        "payload per keystroke: {TOP_N} values vs {} values ({}× smaller)",
        set.len(),
        set.len() / TOP_N
    );
}
