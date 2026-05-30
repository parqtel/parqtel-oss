use parqtel_core::{Error, Result};
use crate::matcher::LabelMatcher;

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregationOp {
    Avg,
    Sum,
    Min,
    Max,
    Count,
    Rate,
    HistogramQuantile,
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
        if metric_name.is_empty() {
            return Err(Error::Validation("Metric name cannot be empty".into()));
        }
        if start_ns >= end_ns {
            return Err(Error::Validation("Start time must be before end time".into()));
        }
        if let Some(step) = step_ns {
            if step <= 0 {
                return Err(Error::Validation("Step size must be greater than zero".into()));
            }
        }
        
        let thirty_days_ns = 30 * 24 * 3600 * 1_000_000_000i64;
        if end_ns - start_ns > thirty_days_ns {
            return Err(Error::Validation("Time range cannot span more than 30 days".into()));
        }

        if let Some(q) = quantile {
            if q <= 0.0 || q >= 1.0 {
                return Err(Error::Validation("Quantile must be in the open interval (0, 1)".into()));
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
        })
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_plan_validation() {
        let res = QueryPlan::new(
            "".into(), vec![], 100, 200, None, 100, 100, None, None
        );
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Metric name cannot be empty"));

        let res2 = QueryPlan::new(
            "test".into(), vec![], 200, 100, None, 100, 100, None, None
        );
        assert!(res2.is_err());
        assert!(res2.unwrap_err().to_string().contains("Start time must be before end time"));

        let res3 = QueryPlan::new(
            "test".into(), vec![], 100, 200, Some(0), 100, 100, None, None
        );
        assert!(res3.is_err());
        assert!(res3.unwrap_err().to_string().contains("Step size must be greater than zero"));
        
        let res4 = QueryPlan::new(
            "test".into(), vec![], 0, 40 * 24 * 3600 * 1_000_000_000, None, 100, 100, None, None
        );
        assert!(res4.is_err());
        assert!(res4.unwrap_err().to_string().contains("Time range cannot span more than 30 days"));

        let res5 = QueryPlan::new(
            "test".into(), vec![], 0, 100, None, 100, 100, None, Some(1.5)
        );
        assert!(res5.is_err());
        assert!(res5.unwrap_err().to_string().contains("Quantile must be in the open interval (0, 1)"));
    }
}
