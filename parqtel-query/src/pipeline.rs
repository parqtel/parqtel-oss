//! ParqtelQL Pipeline — Grail-shaped command language over unified rows.
//!
//! ```text
//! fetch logs
//!   | filter service=api OR severity>=ERROR
//!   | parse "duration_ms=(\d+)" as duration_ms
//!   | stats count() by service, p95(duration_ms)
//!   | limit 20
//!   | correlate traces window=5m
//! ```
//!
//! Design (Phase 2 of the query-language plan):
//! - Stage pipeline over a unified `Row` shape (logs first; metrics/traces
//!   as later fetch targets)
//! - `filter` uses the ParqtelQL boolean predicate tree (OR/AND/NOT)
//! - `parse` extracts ephemeral fields on-read (regex capture groups)
//! - `stats` aggregates: count/avg/min/max/sum/p50/p95/p99, optional
//!   by-grouping, optional interval buckets (`interval=5m` → timeseries)
//! - `correlate` enriches rows with related signal data via the
//!   dimension-priority join (trace_id → service)
//! - Lenient: unknown stage names produce a validation error listing the
//!   supported stages (pipelines are authored, not typed into search bars)

use crate::logql::{parse_predicate, Predicate};
use parqtel_core::{Error, Result};
use serde_json::Value as Json;
use std::collections::BTreeMap;

/// One unified pipeline row: field map + the originating log/trace/metric.
#[derive(Debug, Clone, Default)]
pub struct Row {
    pub fields: BTreeMap<String, Json>,
}

impl Row {
    pub fn get_str(&self, k: &str) -> Option<&str> {
        self.fields.get(k).and_then(|v| v.as_str())
    }
    pub fn get_num(&self, k: &str) -> Option<f64> {
        match self.fields.get(k) {
            Some(Json::Number(n)) => n.as_f64(),
            Some(Json::String(s)) => s.parse().ok(),
            _ => None,
        }
    }
    /// Timestamp in ns (for time bucketing/ordering).
    pub fn ts_ns(&self) -> i64 {
        self.fields
            .get("timestamp_ns")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    }
}

// ── Pipeline AST ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Stage {
    Fetch {
        signal: String,
    },
    Filter {
        pred: Predicate,
    },
    Parse {
        pattern: String,
        field: String,
    },
    Stats {
        aggs: Vec<AggSpec>,
        by: Vec<String>,
        /// Optional bucket interval in ns (makesTimeseries-style output).
        interval_ns: Option<i64>,
    },
    Limit(usize),
    Correlate {
        signal: String,
        window_ns: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggSpec {
    pub func: AggFn,
    pub field: Option<String>,
    /// Output column name (defaults to `func(field)`).
    pub alias: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFn {
    Count,
    Avg,
    Min,
    Max,
    Sum,
    P50,
    P95,
    P99,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Pipeline {
    pub stages: Vec<Stage>,
}

impl Pipeline {
    pub fn fetch_signal(&self) -> String {
        self.stages
            .iter()
            .find_map(|s| match s {
                Stage::Fetch { signal } => Some(signal.clone()),
                _ => None,
            })
            .unwrap_or_else(|| "logs".into())
    }
}

// ── Parser ───────────────────────────────────────────────────────────────────

/// Parses a pipeline query: `fetch <signal> [| stage]...`
pub fn parse_pipeline(input: &str) -> Result<Pipeline> {
    let trimmed = input.trim();
    if !trimmed.starts_with("fetch ") {
        return Err(Error::Validation(
            "pipelines must start with `fetch <signal>` (e.g. `fetch logs`)".into(),
        ));
    }
    // Split into stages on top-level `|` (not inside quotes/braces).
    let parts = split_stages(trimmed)?;
    let mut stages = Vec::new();
    for (idx, part) in parts.into_iter().enumerate() {
        let stage = parse_stage(&part, idx == 0)?;
        stages.push(stage);
    }
    if !stages.iter().any(|s| matches!(s, Stage::Fetch { .. })) {
        return Err(Error::Validation("missing fetch stage".into()));
    }
    // Stage-order validation: stats is terminal for row transforms.
    if let Some(stats_pos) = stages.iter().position(|s| matches!(s, Stage::Stats { .. })) {
        for later in &stages[stats_pos + 1..] {
            if matches!(later, Stage::Filter { .. } | Stage::Parse { .. }) {
                return Err(Error::Validation(
                    "filter/parse cannot follow stats (stats is terminal for row filtering)".into(),
                ));
            }
        }
    }
    Ok(Pipeline { stages })
}

/// Splits on `|` that are not inside quotes.
fn split_stages(input: &str) -> Result<Vec<String>> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut in_quote: Option<char> = None;
    for ch in input.chars() {
        match in_quote {
            Some(q) => {
                current.push(ch);
                if ch == q {
                    in_quote = None;
                }
            }
            None => {
                if ch == '"' {
                    in_quote = Some('"');
                    current.push(ch);
                } else if ch == '|' {
                    parts.push(current.trim().to_string());
                    current = String::new();
                } else {
                    current.push(ch);
                }
            }
        }
    }
    if in_quote.is_some() {
        return Err(Error::Validation("unterminated string in pipeline".into()));
    }
    parts.push(current.trim().to_string());
    Ok(parts)
}

fn parse_stage(part: &str, is_first: bool) -> Result<Stage> {
    let part = part.trim();
    let (name, rest) = part.split_once(char::is_whitespace).unwrap_or((part, ""));
    match name {
        "fetch" => {
            let signal = rest.trim();
            if !matches!(signal, "logs" | "traces" | "metrics") {
                return Err(Error::Validation(format!(
                    "unsupported fetch signal {signal:?} (logs | traces | metrics)"
                )));
            }
            Ok(Stage::Fetch {
                signal: signal.to_string(),
            })
        }
        "filter" => Ok(Stage::Filter {
            pred: parse_predicate(rest)?,
        }),
        "parse" => parse_parse_stage(rest),
        "stats" => parse_stats_stage(rest),
        "limit" => {
            let n: usize = rest
                .trim()
                .parse()
                .map_err(|_| Error::Validation(format!("limit needs a number, got {rest:?}")))?;
            Ok(Stage::Limit(n))
        }
        "correlate" => parse_correlate_stage(rest),
        other => Err(Error::Validation(format!(
            "unknown stage {other:?}; supported: fetch, filter, parse, stats, limit, correlate"
        ))),
    }
    .and_then(|s| {
        if is_first && !matches!(s, Stage::Fetch { .. }) {
            return Err(Error::Validation(
                "pipeline must start with a fetch stage".into(),
            ));
        }
        if !is_first && matches!(s, Stage::Fetch { .. }) {
            return Err(Error::Validation(
                "fetch is only allowed as the first stage".into(),
            ));
        }
        Ok(s)
    })
}

/// `parse "<pattern>" as <field>` — first capture group extracts the field.
fn parse_parse_stage(rest: &str) -> Result<Stage> {
    let rest = rest.trim();
    let Some(open) = rest.find('"') else {
        return Err(Error::Validation(
            "parse needs a quoted regex pattern".into(),
        ));
    };
    let Some(close_rel) = rest[open + 1..].find('"') else {
        return Err(Error::Validation("unterminated parse pattern".into()));
    };
    let pattern = rest[open + 1..open + 1 + close_rel].to_string();
    let after = rest[open + 1 + close_rel + 1..].trim();
    let field = after
        .strip_prefix("as ")
        .map(|f| f.trim().to_string())
        .ok_or_else(|| Error::Validation("parse needs `as <field>`".into()))?;
    if field.is_empty() {
        return Err(Error::Validation(
            "parse needs a field name after `as`".into(),
        ));
    }
    regex::Regex::new(&pattern)
        .map_err(|e| Error::Validation(format!("invalid parse regex: {e}")))?;
    Ok(Stage::Parse { pattern, field })
}

/// `stats count() [as x][, p95(dur)] [by a, b] [interval=5m]`
fn parse_stats_stage(rest: &str) -> Result<Stage> {
    let mut aggs = Vec::new();
    let mut by = Vec::new();
    let mut interval_ns = None;

    // Extract `by <list>` and `interval=<dur>` first.
    let mut body = rest.trim().to_string();
    if let Some(interval_pos) = body.find("interval=") {
        let dur_str = body[interval_pos + "interval=".len()..]
            .split_whitespace()
            .next()
            .unwrap_or("")
            .to_string();
        interval_ns = Some(crate::matcher::parse_duration_str(&dur_str)?);
        body = body[..interval_pos].trim().to_string();
    }
    if let Some(by_pos) = body.find(" by ") {
        let by_str = body[by_pos + 4..].trim().to_string();
        by = by_str
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        body = body[..by_pos].trim().to_string();
    }

    for spec in body.split(',') {
        let spec = spec.trim();
        if spec.is_empty() {
            continue;
        }
        let (call, alias) = match spec.split_once(" as ") {
            Some((c, a)) => (c.trim(), Some(a.trim().to_string())),
            None => (spec, None),
        };
        let Some(open) = call.find('(') else {
            return Err(Error::Validation(format!(
                "stats needs function calls like count(), got {spec:?}"
            )));
        };
        let func = &call[..open];
        let inner = call[open + 1..].trim_end_matches(')').trim();
        let field = if inner.is_empty() {
            None
        } else {
            Some(inner.to_string())
        };
        let func = match func {
            "count" => AggFn::Count,
            "avg" => AggFn::Avg,
            "min" => AggFn::Min,
            "max" => AggFn::Max,
            "sum" => AggFn::Sum,
            "p50" => AggFn::P50,
            "p95" => AggFn::P95,
            "p99" => AggFn::P99,
            other => {
                return Err(Error::Validation(format!(
                    "unknown stats function {other:?} (count/avg/min/max/sum/p50/p95/p99)"
                )))
            }
        };
        if !matches!(func, AggFn::Count) && field.is_none() {
            return Err(Error::Validation(format!(
                "{func:?} requires a field argument"
            )));
        }
        aggs.push(AggSpec { func, field, alias });
    }
    if aggs.is_empty() {
        return Err(Error::Validation(
            "stats needs at least one aggregation".into(),
        ));
    }
    Ok(Stage::Stats {
        aggs,
        by,
        interval_ns,
    })
}

/// `correlate <signal> [window=5m]`
fn parse_correlate_stage(rest: &str) -> Result<Stage> {
    let mut parts = rest.split_whitespace();
    let signal = parts
        .next()
        .ok_or_else(|| Error::Validation("correlate needs a signal (traces|logs)".into()))?;
    if !matches!(signal, "traces" | "logs") {
        return Err(Error::Validation(
            "correlate supports traces or logs today".into(),
        ));
    }
    let mut window_ns = 300_000_000_000i64; // 5m default
    for p in parts {
        if let Some(dur) = p.strip_prefix("window=") {
            window_ns = crate::matcher::parse_duration_str(dur)?;
        }
    }
    Ok(Stage::Correlate {
        signal: signal.to_string(),
        window_ns,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn parses_full_pipeline() {
        let p = parse_pipeline(
            r#"fetch logs
               | filter service=api OR severity>=ERROR
               | parse "duration_ms=(\d+)" as duration_ms
               | stats count(), p95(duration_ms) by service interval=5m
               | limit 10
               | correlate traces window=2m"#,
        )
        .unwrap();
        assert_eq!(p.stages.len(), 6);
        assert!(matches!(p.stages[0], Stage::Fetch { .. }));
        assert!(matches!(p.stages[1], Stage::Filter { .. }));
        assert!(matches!(p.stages[2], Stage::Parse { .. }));
        match &p.stages[3] {
            Stage::Stats {
                aggs,
                by,
                interval_ns,
            } => {
                assert_eq!(aggs.len(), 2);
                assert_eq!(by, &vec!["service".to_string()]);
                assert_eq!(*interval_ns, Some(300_000_000_000));
            }
            _ => panic!(),
        }
        assert!(matches!(p.stages[4], Stage::Limit(10)));
        match &p.stages[5] {
            Stage::Correlate { signal, window_ns } => {
                assert_eq!(signal, "traces");
                assert_eq!(*window_ns, 120_000_000_000);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn filter_or_tree() {
        let p = parse_pipeline("fetch logs | filter error OR timeout").unwrap();
        match &p.stages[1] {
            Stage::Filter { pred } => assert!(matches!(pred, Predicate::Or(_))),
            _ => panic!(),
        }
    }

    #[test]
    fn rejects_unknown_stage() {
        assert!(parse_pipeline("fetch logs | frobnicate x").is_err());
    }

    #[test]
    fn rejects_missing_fetch() {
        assert!(parse_pipeline("filter error").is_err());
        assert!(parse_pipeline("fetch logs | fetch logs").is_err());
    }

    #[test]
    fn stats_validation() {
        assert!(parse_pipeline("fetch logs | stats").is_err());
        assert!(parse_pipeline("fetch logs | stats p95()").is_err());
        assert!(parse_pipeline("fetch logs | stats median(x)").is_err());
        assert!(parse_pipeline("fetch logs | stats count()").is_ok());
    }

    #[test]
    fn parse_stage_requires_as() {
        assert!(parse_pipeline(r#"fetch logs | parse "(\d+)""#).is_err());
        assert!(parse_pipeline(r#"fetch logs | parse "(\d+)" as x"#).is_ok());
    }
}
