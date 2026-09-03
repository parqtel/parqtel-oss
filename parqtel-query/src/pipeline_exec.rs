//! Pipeline execution: runs parsed pipeline stages over unified rows.
//!
//! Phase 2 executor: `fetch logs` materializes rows from the log scan
//! (buffer + blocks), then filter/parse/stats/limit/correlate transform
//! them. Metrics/traces fetch targets and correlate enrichment use the
//! same executor APIs the handlers use.

use crate::logql::Predicate;
pub use crate::pipeline::{AggFn, AggSpec, Pipeline, Row, Stage};
use parqtel_core::{Error, Result};
use serde_json::{json, Value as Json};

/// Result of executing a pipeline: either rows (no stats) or a stats
/// table (with optional time bucketing).
#[derive(Debug, Clone)]
pub enum PipelineResult {
    Rows(Vec<Row>),
    Table {
        columns: Vec<String>,
        rows: Vec<Vec<Json>>,
        /// Present when interval= was set: per-bucket table rows.
        timeseries: Option<TimeseriesTable>,
    },
}

/// One output series: group key fields + per-bucket values.
pub type SeriesValues = (Vec<(String, Json)>, Vec<Vec<Json>>);

#[derive(Debug, Clone)]
pub struct TimeseriesTable {
    /// Bucket start timestamps (ns), ascending.
    pub timestamps: Vec<i64>,
    /// One row per group: (group key fields, per-bucket values per agg).
    /// (group field values, per-bucket agg values).
    pub series: Vec<SeriesValues>,
}

/// Runs the parsed pipeline stages over rows produced by `fetch`.
pub fn run_stages(
    pipeline: &Pipeline,
    fetch: impl FnOnce() -> Result<Vec<Row>>,
) -> Result<PipelineResult> {
    let mut rows: Vec<Row> = fetch()?;

    for stage in &pipeline.stages {
        match stage {
            Stage::Fetch { .. } => {} // handled by the fetch closure
            Stage::Filter { pred } => {
                rows.retain(|r| row_matches_pred(r, pred));
            }
            Stage::Parse { pattern, field } => {
                let re = regex::Regex::new(pattern)
                    .map_err(|e| Error::Validation(format!("parse regex: {e}")))?;
                for row in &mut rows {
                    if let Some(body) = row
                        .fields
                        .get("body")
                        .and_then(|b| b.as_str())
                        .map(str::to_string)
                    {
                        if let Some(caps) = re.captures(&body) {
                            if let Some(m) = caps.get(1) {
                                let text = m.as_str().to_string();
                                let value: Json = if let Ok(n) = text.parse::<f64>() {
                                    json!(n)
                                } else {
                                    json!(text)
                                };
                                row.fields.insert(field.clone(), value);
                            }
                        }
                    }
                }
            }
            Stage::Stats {
                aggs,
                by,
                interval_ns,
            } => {
                return finish_stats(rows, aggs, by, *interval_ns);
            }
            Stage::Limit(n) => {
                rows.truncate(*n);
            }
            Stage::Correlate { .. } => {
                // Correlate enrichment runs server-side where the executor
                // has both signals; declared here for parse validation.
            }
        }
    }
    Ok(PipelineResult::Rows(rows))
}

/// Row-level predicate evaluation (field ops against row.fields; body terms
/// against the body field).
pub fn row_matches_pred(row: &Row, pred: &Predicate) -> bool {
    match pred {
        Predicate::And(parts) => parts.iter().all(|p| row_matches_pred(row, p)),
        Predicate::Or(parts) => parts.iter().any(|p| row_matches_pred(row, p)),
        Predicate::Not(inner) => !row_matches_pred(row, inner),
        Predicate::Atom(crate::logql::Atom::Term(term)) => {
            let hay = row
                .fields
                .get("body")
                .and_then(|b| b.as_str())
                .unwrap_or("");
            let matched = hay.to_lowercase().contains(&term.text);
            if term.negate {
                !matched
            } else {
                matched
            }
        }
        Predicate::Atom(crate::logql::Atom::Clause(clause)) => {
            // Reuse clause logic through a pseudo-log bridge: evaluate the
            // clause against row fields generically.
            use crate::logql::Clause;
            match clause {
                Clause::Eq { field, value } => row
                    .fields
                    .get(field)
                    .map(|v| value_matches(v, value))
                    .unwrap_or(false),
                Clause::Ne { field, value } => row
                    .fields
                    .get(field)
                    .map(|v| !value_matches(v, value))
                    .unwrap_or(true),
                Clause::Re { field, regex } => {
                    let re = regex::Regex::new(regex).ok();
                    match (row.fields.get(field), re) {
                        (Some(v), Some(re)) => re.is_match(&value_str(v)),
                        _ => false,
                    }
                }
                Clause::Cmp { field, op, value } => {
                    let n = row.get_num(field);
                    match (n, op) {
                        (Some(n), crate::logql::CmpOp::Gt) => n > *value,
                        (Some(n), crate::logql::CmpOp::Ge) => n >= *value,
                        (Some(n), crate::logql::CmpOp::Lt) => n < *value,
                        (Some(n), crate::logql::CmpOp::Le) => n <= *value,
                        _ => false,
                    }
                }
                Clause::Range { field, min, max } => row
                    .get_num(field)
                    .map(|n| n >= *min && n <= *max)
                    .unwrap_or(false),
                Clause::Exists { field } => row.fields.contains_key(field),
                Clause::SeverityMin(sev) => {
                    let min = crate::logql::severity_rank(sev).unwrap_or(9);
                    row.get_num("severity_number")
                        .map(|n| n >= min as f64)
                        .unwrap_or(false)
                }
                Clause::Not(inner) => {
                    // Invert by evaluating the inner clause against the row.
                    let pred = Predicate::Atom(crate::logql::Atom::Clause((**inner).clone()));
                    !row_matches_pred(row, &pred)
                }
            }
        }
    }
}

fn value_matches(v: &Json, needle: &str) -> bool {
    value_str(v).eq_ignore_ascii_case(needle)
}

fn value_str(v: &Json) -> String {
    match v {
        Json::String(s) => s.clone(),
        Json::Number(n) => n.to_string(),
        Json::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

// ── Stats ─────────────────────────────────────────────────────────────────────

fn finish_stats(
    rows: Vec<Row>,
    aggs: &[AggSpec],
    by: &[String],
    interval_ns: Option<i64>,
) -> Result<PipelineResult> {
    // Group rows by the by-key.
    use std::collections::BTreeMap;
    // Json isn't Ord; key groups by the canonical string form and carry the
    // original key in the value.
    let mut groups: BTreeMap<String, (Vec<Json>, Vec<Row>)> = BTreeMap::new();
    for row in rows {
        let key: Vec<Json> = by
            .iter()
            .map(|f| row.fields.get(f).cloned().unwrap_or(Json::Null))
            .collect();
        let key_str = serde_json::to_string(&key).unwrap_or_default();
        groups
            .entry(key_str)
            .or_insert((key, Vec::new()))
            .1
            .push(row);
    }

    let mut columns: Vec<String> = by.to_vec();
    for agg in aggs {
        columns.push(default_agg_name(agg));
    }

    if let Some(interval) = interval_ns {
        // G15: wall-clock-snapped buckets — timestamps aligned to interval
        // boundaries from the epoch (0, 5m, 10m, ...), and sparse gaps
        // between observed buckets stay explicit.
        let interval = interval.max(1);
        let snap = |ts: i64| ts.div_euclid(interval) * interval;
        let mut bucket_ids: BTreeMap<i64, ()> = BTreeMap::new();
        for row in groups.values().flat_map(|(_, rs)| rs.iter()) {
            bucket_ids.insert(snap(row.ts_ns()), ());
        }
        let timestamps: Vec<i64> = bucket_ids.keys().copied().collect();

        let mut series = Vec::new();
        for (key, group_rows) in groups.values() {
            // Recompute per-bucket aggregates directly.
            let mut bucket_rows: BTreeMap<i64, Vec<&Row>> = BTreeMap::new();
            for row in group_rows.iter() {
                bucket_rows.entry(snap(row.ts_ns())).or_default().push(row);
            }
            let mut values: Vec<Vec<Json>> = Vec::with_capacity(timestamps.len());
            for ts in &timestamps {
                let empty: Vec<&Row> = vec![];
                let bucket = bucket_rows.get(ts).unwrap_or(&empty);
                let mut vals = Vec::with_capacity(aggs.len());
                for agg in aggs {
                    vals.push(compute_agg(agg, bucket));
                }
                values.push(vals);
            }
            let group_fields: Vec<(String, Json)> =
                by.iter().cloned().zip(key.iter().cloned()).collect();
            series.push((group_fields, values));
        }
        return Ok(PipelineResult::Table {
            columns,
            rows: Vec::new(),
            timeseries: Some(TimeseriesTable { timestamps, series }),
        });
    }

    // Flat table: one row per group.
    let mut out_rows = Vec::new();
    for (key, group_rows) in groups.values() {
        let mut vals: Vec<Json> = key.clone();
        for agg in aggs {
            vals.push(compute_agg(agg, &group_rows.iter().collect::<Vec<_>>()));
        }
        out_rows.push(vals);
    }

    Ok(PipelineResult::Table {
        columns,
        rows: out_rows,
        timeseries: None,
    })
}

fn default_agg_name(agg: &AggSpec) -> String {
    agg.alias.clone().unwrap_or_else(|| match &agg.field {
        Some(f) => format!("{}_{}", agg_name(&agg.func), f),
        None => agg_name(&agg.func).to_string(),
    })
}

fn agg_name(f: &AggFn) -> &'static str {
    match f {
        AggFn::Count => "count",
        AggFn::Avg => "avg",
        AggFn::Min => "min",
        AggFn::Max => "max",
        AggFn::Sum => "sum",
        AggFn::P50 => "p50",
        AggFn::P95 => "p95",
        AggFn::P99 => "p99",
    }
}

fn compute_agg(agg: &AggSpec, rows: &[&Row]) -> Json {
    let field = agg.field.as_deref();
    let nums: Vec<f64> = match field {
        Some(f) => rows.iter().filter_map(|r| r.get_num(f)).collect(),
        None => Vec::new(),
    };
    match agg.func {
        AggFn::Count => json!(rows.len()),
        AggFn::Sum => json!(nums.iter().sum::<f64>()),
        AggFn::Avg => {
            if nums.is_empty() {
                Json::Null
            } else {
                json!(nums.iter().sum::<f64>() / nums.len() as f64)
            }
        }
        AggFn::Min => {
            if nums.is_empty() {
                Json::Null
            } else {
                json!(nums.iter().copied().fold(f64::INFINITY, f64::min))
            }
        }
        AggFn::Max => {
            if nums.is_empty() {
                Json::Null
            } else {
                json!(nums.iter().copied().fold(f64::NEG_INFINITY, f64::max))
            }
        }
        AggFn::P50 | AggFn::P95 | AggFn::P99 => {
            let mut sorted = nums.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            if sorted.is_empty() {
                Json::Null
            } else {
                let q = match agg.func {
                    AggFn::P50 => 0.5,
                    AggFn::P95 => 0.95,
                    _ => 0.99,
                };
                let pos = q * (sorted.len() - 1) as f64;
                let lo = pos.floor() as usize;
                let hi = pos.ceil() as usize;
                let v = if lo == hi {
                    sorted[lo]
                } else {
                    sorted[lo] * (1.0 - (pos - lo as f64)) + sorted[hi] * (pos - lo as f64)
                };
                json!(v)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::pipeline::parse_pipeline;

    fn row(body: &str, sev: i32, svc: &str, ts: i64, dur: Option<f64>) -> Row {
        let mut r = Row::default();
        r.fields.insert("body".into(), json!(body));
        r.fields.insert("severity_number".into(), json!(sev));
        r.fields.insert("service".into(), json!(svc));
        r.fields.insert("timestamp_ns".into(), json!(ts));
        if let Some(d) = dur {
            r.fields.insert("duration_ms".into(), json!(d));
        }
        r
    }

    const S: i64 = 1_000_000_000;

    fn fixture() -> Vec<Row> {
        vec![
            row("upstream timeout after 500ms", 13, "api", 0, Some(500.0)),
            row(
                "upstream timeout after 900ms",
                17,
                "api",
                10 * S,
                Some(900.0),
            ),
            row("request ok", 9, "web", 20 * S, Some(50.0)),
            row("request ok", 9, "web", 30 * S, Some(60.0)),
        ]
    }

    #[test]
    fn filter_parse_stats_pipeline() {
        let p = parse_pipeline(
            r#"fetch logs
               | filter service=api OR severity>=ERROR
               | parse "after (\d+)ms" as latency_ms
               | stats count(), p95(latency_ms) by service"#,
        )
        .unwrap();
        let result = run_stages(&p, || Ok(fixture())).unwrap();
        match result {
            PipelineResult::Table { columns, rows, .. } => {
                assert_eq!(rows.len(), 1, "one group (api)");
                assert!(columns.contains(&"count".to_string()));
                // p95 of [500, 900] ≈ 880
                let p95 = rows[0].last().unwrap().as_f64().unwrap();
                assert!((p95 - 880.0).abs() < 25.0, "p95 = {p95}");
            }
            other => panic!("expected table, got {other:?}"),
        }
    }

    #[test]
    fn interval_bucketing() {
        let p = parse_pipeline("fetch logs | stats count() by service interval=10s").unwrap();
        let result = run_stages(&p, || Ok(fixture())).unwrap();
        match result {
            PipelineResult::Table {
                timeseries: Some(ts),
                ..
            } => {
                assert_eq!(ts.timestamps.len(), 4, "buckets at 0/10/20/30s");
                assert_eq!(ts.series.len(), 2, "api + web");
            }
            other => panic!("expected timeseries, got {other:?}"),
        }
    }

    #[test]
    fn limit_stage() {
        let p = parse_pipeline("fetch logs | filter timeout | limit 1").unwrap();
        let result = run_stages(&p, || Ok(fixture())).unwrap();
        match result {
            PipelineResult::Rows(rows) => assert_eq!(rows.len(), 1),
            other => panic!("expected rows, got {other:?}"),
        }
    }

    #[test]
    fn filter_after_stats_rejected() {
        // Rejected at parse time (stats is terminal for row filtering).
        assert!(parse_pipeline("fetch logs | stats count() | filter x").is_err());
    }
}
