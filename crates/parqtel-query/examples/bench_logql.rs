//! Benchmarks the LogQL prepared-query path against the per-row wrapper.
//!
//! Run: cargo run --release -p parqtel-query --example bench_logql
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use parqtel_core::LabelSet;
use parqtel_query::logql::{log_matches, parse_search, PreparedLogQuery};
use std::time::Instant;

fn main() {
    // `pai*` matches the "paid" in the fixture body; `pay*` would not
    // (paid contains "pai", not "pay").
    let qs = r#"pai* service=~"ap.*" attr.http_status_code >= 400"#;
    let q = parse_search(qs);
    let rows: Vec<parqtel_core::LogRecord> = (0..20_000)
        .map(|i| {
            parqtel_core::LogRecord::new(
                1_000 + i,
                2_000 + i,
                9,
                "INFO".into(),
                format!(
                    "request {i} {} checkout flow in 12ms for user {i}",
                    if i % 2 == 0 { "paid" } else { "declined" }
                ),
                // LogRecord::new takes (attributes, resource_attributes) in that
                // order — `service.name` resolves through the resource set.
                LabelSet::try_from_iter(vec![
                    // Paid rows get a 5xx so all three clauses can hold at once:
                    // otherwise the AND is unsatisfiable and the benchmark would
                    // measure an all-false scan instead of real matching.
                    (
                        "http_status_code",
                        if i % 2 == 0 { "500" } else { "200" }.to_string(),
                    ),
                ])
                .unwrap(),
                LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        })
        .collect();
    let extra = std::collections::HashMap::new();

    // Warm up.
    for l in rows.iter().take(1000) {
        let _ = log_matches(&q, l, &extra);
    }

    let t0 = Instant::now();
    let mut n = 0usize;
    for l in &rows {
        if log_matches(&q, l, &extra) {
            n += 1;
        }
    }
    let per_row = t0.elapsed();

    let prepared = PreparedLogQuery::new(&q);
    let t1 = Instant::now();
    let mut n2 = 0usize;
    for l in &rows {
        if prepared.matches(l, &extra) {
            n2 += 1;
        }
    }
    let prepared_t = t1.elapsed();

    assert_eq!(n, n2, "results must be identical");
    println!(
        "rows={} matched={} ({:.0}%)",
        rows.len(),
        n,
        100.0 * n as f64 / rows.len() as f64
    );
    assert!(
        n > 0,
        "benchmark must exercise the matching path, not an all-false scan"
    );
    println!("per-row prepare : {:?}", per_row);
    println!("prepared once   : {:?}", prepared_t);
    println!(
        "speedup         : {:.1}x",
        per_row.as_secs_f64() / prepared_t.as_secs_f64()
    );
}
