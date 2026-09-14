//! PromQL-compatible AST for composed queries.
//!
//! Phase 1A of the query-language buildout (docs/QUERY_ENGINE_ANALYSIS.md):
//! replaces the single-function dispatch with a real grammar so queries
//! compose — `sum by (job) (rate(x[5m]))`, `a / b`, `on()` vector matching,
//! subqueries, and function pipelines evaluate on a per-step matrix model.

use crate::models::Sample;
use parqtel_core::LabelSet;
use std::collections::HashMap;

/// A parsed PromQL expression.
#[derive(Debug, Clone)]
pub enum Expr {
    /// `metric_name{matchers}` or bare `metric_name`
    Selector(SelectorExpr),
    /// `<number>` scalar literal
    Number(f64),
    /// `"text"` string literal — valid only as function/aggregation
    /// parameters (label_replace, label_join, count_values,
    /// sort_by_label). PromQL has no string vectors.
    Str(String),
    /// `fn(args...)` — instant-vector transforms and scalar helpers.
    Call(CallExpr),
    /// `agg [by|without (labels)] (expr)`
    Aggregation(AggregationExpr),
    /// `expr[range][[offset]]` or subquery `expr[range:step]`
    Range(RangeExpr),
    /// `a op b` and `a op ignoring/on(...) b`
    Binary(BinaryExpr),
    /// `(expr)` — parenthesized grouping (dropped after parsing)
    Paren(Box<Expr>),
}

/// Metric selector with optional label matchers.
#[derive(Debug, Clone)]
pub struct SelectorExpr {
    pub metric_name: Option<String>,
    pub matchers: Vec<crate::matcher::LabelMatcher>,
}

/// Function call. Args can themselves be any expression.
#[derive(Debug, Clone)]
pub struct CallExpr {
    pub name: String,
    pub args: Vec<Expr>,
}

/// Aggregation over an instant vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    Sum,
    Avg,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    CountValues,
    TopK,
    BottomK,
    Quantile,
    Group,
}

/// `sum by (l1, l2) (...)` grouping modifier.
#[derive(Debug, Clone)]
pub enum Grouping {
    /// No modifier: aggregate across all series into one result.
    None,
    /// `by (labels)` — keep only these labels.
    By(Vec<String>),
    /// `without (labels)` — keep all except these labels (plus __name__).
    Without(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct AggregationExpr {
    pub op: AggregationOp,
    pub grouping: Grouping,
    pub param: Option<Box<Expr>>, // topk/bottomk N, quantile φ, count_values label
    pub expr: Box<Expr>,
}

/// Range selector / subquery.
#[derive(Debug, Clone)]
pub struct RangeExpr {
    pub expr: Box<Expr>,
    /// Window duration in nanoseconds.
    pub range_ns: i64,
    /// Subquery resolution step (None = plain range selector).
    pub step_ns: Option<i64>,
    pub offset_ns: i64,
}

/// Binary operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
    And,
    Or,
    Unless,
}

impl BinaryOp {
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            BinaryOp::Eq | BinaryOp::Ne | BinaryOp::Gt | BinaryOp::Lt | BinaryOp::Ge | BinaryOp::Le
        )
    }
}

/// Vector-to-vector matching modifier.
#[derive(Debug, Clone)]
pub enum VectorMatch {
    /// No modifier: match on ALL labels (except __name__).
    All,
    /// `on (labels)` — match only on these labels.
    On(Vec<String>),
    /// `ignoring (labels)` — match on all except these.
    Ignoring(Vec<String>),
}

/// `group_left` / `group_right` with optional result labels.
#[derive(Debug, Clone)]
pub enum MatchCardinality {
    OneToOne,
    ManyToOne(Vec<String>),
    OneToMany(Vec<String>),
}

#[derive(Debug, Clone)]
pub struct BinaryExpr {
    pub op: BinaryOp,
    pub lhs: Box<Expr>,
    pub rhs: Box<Expr>,
    pub matching: Option<(VectorMatch, MatchCardinality)>,
    /// `bool` modifier: comparisons return 0/1 instead of filtering.
    pub return_bool: bool,
}

/// An evaluated instant vector at one timestamp: series → value.
/// Ordered map semantics come from sorting at the end of evaluation.
#[derive(Debug, Clone, Default)]
pub struct InstantVector {
    pub series: Vec<(LabelSet, f64)>,
}

impl InstantVector {
    pub fn get(&self, labels: &LabelSet) -> Option<f64> {
        self.series
            .iter()
            .find(|(ls, _)| ls == labels)
            .map(|(_, v)| *v)
    }
    pub fn is_empty(&self) -> bool {
        self.series.is_empty()
    }
}

/// Evaluated range vector: per-series windows for a single step.
pub type RangeVector = Vec<(LabelSet, Vec<Sample>)>;

/// Evaluation context for one step of a range query.
#[derive(Debug, Clone, Copy)]
pub struct EvalContext {
    /// Timestamp of this evaluation step.
    pub ts_ns: i64,
    /// Range-selector window duration.
    pub range_ns: i64,
    pub offset_ns: i64,
    /// Subquery resolution step (if inside a subquery).
    pub subquery_step_ns: Option<i64>,
}

/// Raw sample source for the evaluator: metric name → per-series points
/// (already filtered by matchers, sorted by timestamp).
pub type SeriesData = HashMap<String, Vec<(LabelSet, Vec<(i64, f64)>)>>;

/// One native/OTLP histogram sample (explicit-bucket form).
#[derive(Debug, Clone)]
pub struct HistSample {
    pub timestamp_ns: i64,
    pub count: u64,
    pub sum: f64,
    /// Explicit upper bounds, ascending. `counts.len() == boundaries.len() + 1`
    /// — the final bucket is the +Inf catch-all.
    pub boundaries: Vec<f64>,
    pub counts: Vec<u64>,
}

impl HistSample {
    /// Interpolated quantile over the explicit buckets (Prometheus
    /// histogram_quantile semantics: linear interpolation inside the
    /// bucket that first reaches the target rank).
    pub fn quantile(&self, q: f64) -> f64 {
        let total = self.count as f64;
        if total <= 0.0 || !(0.0..=1.0).contains(&q) {
            return f64::NAN;
        }
        let target = q * total;
        let mut prev_count = 0.0f64;
        let mut prev_bound = 0.0f64; // lower bound of the first bucket
        for (i, bound) in self.boundaries.iter().enumerate() {
            let c = *self.counts.get(i).unwrap_or(&0) as f64;
            if prev_count + c >= target {
                if c <= 0.0 {
                    return *bound;
                }
                let frac = (target - prev_count) / c;
                return prev_bound + (*bound - prev_bound) * frac;
            }
            prev_count += c;
            prev_bound = *bound;
        }
        // Target falls in the +Inf bucket — return the last finite bound.
        self.boundaries.last().copied().unwrap_or(f64::INFINITY)
    }

    /// Fraction of observations with value in [lower, upper) via bucket
    /// interpolation (histogram_fraction semantics).
    pub fn fraction(&self, lower: f64, upper: f64) -> f64 {
        let total = self.count as f64;
        if total <= 0.0 {
            return f64::NAN;
        }
        // Count observations strictly below a value via interpolation.
        let below = |v: f64| -> f64 {
            let mut acc = 0.0;
            let mut prev_bound = 0.0f64;
            for (i, bound) in self.boundaries.iter().enumerate() {
                let c = *self.counts.get(i).unwrap_or(&0) as f64;
                if v <= prev_bound {
                    break;
                }
                if v >= *bound {
                    acc += c;
                } else {
                    // v falls inside this bucket — interpolate linearly.
                    let frac = if *bound > prev_bound {
                        (v - prev_bound) / (*bound - prev_bound)
                    } else {
                        0.0
                    };
                    acc += c * frac;
                    break;
                }
                prev_bound = *bound;
            }
            acc.min(total)
        };
        (below(upper) - below(lower)).max(0.0) / total
    }

    /// Population variance over bucket midpoints (finite buckets only),
    /// weighted by counts — histogram_stdvar semantics.
    pub fn variance(&self) -> f64 {
        let mut weight_sum = 0.0;
        let mut mean = 0.0;
        let mut prev_bound = 0.0f64;
        for (i, bound) in self.boundaries.iter().enumerate() {
            let c = *self.counts.get(i).unwrap_or(&0) as f64;
            if c > 0.0 && bound.is_finite() {
                let mid = (prev_bound + bound) / 2.0;
                weight_sum += c;
                mean += c * mid;
            }
            prev_bound = *bound;
        }
        if weight_sum <= 0.0 {
            return f64::NAN;
        }
        let mu = mean / weight_sum;
        let mut var = 0.0;
        prev_bound = 0.0;
        for (i, bound) in self.boundaries.iter().enumerate() {
            let c = *self.counts.get(i).unwrap_or(&0) as f64;
            if c > 0.0 && bound.is_finite() {
                let mid = (prev_bound + bound) / 2.0;
                var += c * (mid - mu).powi(2);
            }
            prev_bound = *bound;
        }
        var / weight_sum
    }
}

/// Raw histogram sample source: metric name → per-series histogram points.
pub type HistData = HashMap<String, Vec<(LabelSet, Vec<HistSample>)>>;

pub mod keywords {
    pub const AGGREGATIONS: &[&str] = &[
        "sum",
        "avg",
        "min",
        "max",
        "count",
        "stddev",
        "stdvar",
        "count_values",
        "topk",
        "bottomk",
        "quantile",
        "group",
    ];
    pub const MATCH_KEYWORDS: &[&str] = &["on", "ignoring", "group_left", "group_right", "bool"];
}
