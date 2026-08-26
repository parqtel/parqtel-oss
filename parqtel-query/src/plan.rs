use crate::matcher::LabelMatcher;
use parqtel_core::{Error, Result};

/// A validated, normalized representation of a metrics query.
#[derive(Debug)]
pub struct QueryPlan {
    pub metric_name: String,
    pub matchers: Vec<LabelMatcher>,
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: Option<i64>,
    pub max_series: usize,
    pub max_samples_per_series: usize,
    pub aggregation: Option<AggregationOp>,
    pub quantile: Option<f64>,
    /// `topk`/`bottomk` N parameter.
    pub topk_n: Option<usize>,
    /// `by (label, ...)` grouping labels. Empty = no grouping.
    pub group_by: Vec<String>,
    /// `without (label, ...)` exclusion labels.
    pub group_without: Vec<String>,
    /// `label_replace` parameters: (dst_label, replacement, src_label, regex).
    pub label_replace: Option<(String, String, String, String)>,
    /// Scalar parameter for `round(metric, to_nearest)`.
    pub scalar_param: Option<f64>,
    /// Clamp bounds: (min, max).
    pub clamp: Option<(Option<f64>, Option<f64>)>,
}

/// All supported aggregation / transform operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    // ── Range aggregations ────────────────────────────────────────────────────
    Avg,
    Sum,
    Min,
    Max,
    Count,
    Stddev,
    Stdvar,
    // ── Range functions ───────────────────────────────────────────────────────
    Rate,
    Irate,
    Increase,
    Delta,
    // ── Histogram ─────────────────────────────────────────────────────────────
    HistogramQuantile,
    // ── Instant transforms (per-sample, applied after windowing) ─────────────
    Abs,
    Ceil,
    Floor,
    Round,
    ClampMin,
    ClampMax,
    // ── Ranking (applied to result-set after all per-series aggregation) ──────
    TopK,
    BottomK,
    // ── Label manipulation ────────────────────────────────────────────────────
    LabelReplace,
}

impl QueryPlan {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        metric_name: String,
        matchers: Vec<LabelMatcher>,
        start_ns: i64,
        end_ns: i64,
        step_ns: Option<i64>,
        max_series: usize,
        max_samples_per_series: usize,
        aggregation: Option<AggregationOp>,
        quantile: Option<f64>,
    ) -> Result<Self> {
        Self::new_full(
            metric_name,
            matchers,
            start_ns,
            end_ns,
            step_ns,
            max_series,
            max_samples_per_series,
            aggregation,
            quantile,
            None,
            vec![],
            vec![],
            None,
            None,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_full(
        metric_name: String,
        matchers: Vec<LabelMatcher>,
        start_ns: i64,
        end_ns: i64,
        step_ns: Option<i64>,
        max_series: usize,
        max_samples_per_series: usize,
        aggregation: Option<AggregationOp>,
        quantile: Option<f64>,
        topk_n: Option<usize>,
        group_by: Vec<String>,
        group_without: Vec<String>,
        label_replace: Option<(String, String, String, String)>,
        scalar_param: Option<f64>,
        clamp: Option<(Option<f64>, Option<f64>)>,
    ) -> Result<Self> {
        if metric_name.is_empty() {
            return Err(Error::Validation("Metric name cannot be empty".into()));
        }
        if start_ns >= end_ns {
            return Err(Error::Validation(
                "Start time must be before end time".into(),
            ));
        }
        if let Some(step) = step_ns {
            if step <= 0 {
                return Err(Error::Validation(
                    "Step size must be greater than zero".into(),
                ));
            }
        }
        let thirty_days_ns = 30 * 24 * 3600 * 1_000_000_000i64;
        if end_ns - start_ns > thirty_days_ns {
            return Err(Error::Validation(
                "Time range cannot span more than 30 days".into(),
            ));
        }
        if let Some(q) = quantile {
            if q <= 0.0 || q >= 1.0 {
                return Err(Error::Validation(
                    "Quantile must be in the open interval (0, 1)".into(),
                ));
            }
        }
        Ok(Self {
            metric_name,
            matchers,
            start_ns,
            end_ns,
            step_ns,
            max_series,
            max_samples_per_series,
            aggregation,
            quantile,
            topk_n,
            group_by,
            group_without,
            label_replace,
            scalar_param,
            clamp,
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_plan_validation() {
        assert!(QueryPlan::new("".into(), vec![], 100, 200, None, 100, 100, None, None).is_err());
        assert!(
            QueryPlan::new("test".into(), vec![], 200, 100, None, 100, 100, None, None).is_err()
        );
        assert!(QueryPlan::new(
            "test".into(),
            vec![],
            100,
            200,
            Some(0),
            100,
            100,
            None,
            None
        )
        .is_err());
        assert!(QueryPlan::new(
            "test".into(),
            vec![],
            0,
            40 * 24 * 3600 * 1_000_000_000,
            None,
            100,
            100,
            None,
            None
        )
        .is_err());
        assert!(QueryPlan::new(
            "test".into(),
            vec![],
            0,
            100,
            None,
            100,
            100,
            None,
            Some(1.5)
        )
        .is_err());
    }

    #[test]
    fn test_plan_ok() {
        let p = QueryPlan::new(
            "cpu".into(),
            vec![],
            0,
            100,
            None,
            10,
            100,
            Some(AggregationOp::Avg),
            None,
        )
        .unwrap();
        assert_eq!(p.metric_name, "cpu");
        assert_eq!(p.aggregation, Some(AggregationOp::Avg));
        assert!(p.group_by.is_empty());
    }
}
