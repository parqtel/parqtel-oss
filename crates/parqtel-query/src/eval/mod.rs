//! AST evaluator: per-step expression evaluation over series data.
//!
//! The executor loads all matching series once per query; the evaluator then
//! walks the AST for every step timestamp, producing an instant vector per
//! step. Range functions look back `range_ns` from each step (Prometheus
//! semantics, as in `downsample_windowed`).
//!
//! - `selectors` evaluates instant and range selectors.
//! - `calls` evaluates function calls, including the label and histogram families.
//! - `range_fn` evaluates the rate family and the `_over_time` functions.
//! - `aggregation` evaluates aggregation operators and grouping.
//! - `binary` evaluates binary operators and vector matching.
//! - `functions` holds the scalar math, quantile and date helpers.

use crate::ast::*;
use parqtel_core::{Error, LabelSet, Result};

mod aggregation;
mod binary;
mod calls;
mod functions;
mod range_fn;
mod selectors;

/// Evaluates an expression tree against pre-loaded series data.
pub struct Evaluator<'a> {
    data: &'a SeriesData,
    /// Native-histogram samples per metric/series (explicit buckets),
    /// for the histogram_* function family.
    hist_data: Option<&'a crate::ast::HistData>,
    /// Instant-selector lookback (Prometheus lookback-delta; 5m default).
    lookback_ns: i64,
    /// Group label sets for aggregations, keyed by the grouping fingerprint.
    ///
    /// The *values* of an aggregation group change every step but its *labels*
    /// never do, so building the output label set once per group and reusing it
    /// across steps removes one `LabelSet` construction per series per step.
    ///
    /// A `Mutex` rather than a `RefCell` to keep `Evaluator` `Sync`; the lock
    /// is uncontended and taken once per series per step, against the
    /// allocations it avoids.
    pub(super) group_cache:
        std::sync::Mutex<std::collections::HashMap<u64, crate::ast::SharedLabels>>,
}

impl<'a> Evaluator<'a> {
    pub fn new(data: &'a SeriesData) -> Self {
        Self {
            data,
            hist_data: None,
            group_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
            lookback_ns: 5 * 60 * 1_000_000_000,
        }
    }

    /// Evaluator with a custom instant-selector lookback window.
    pub fn with_lookback(data: &'a SeriesData, lookback_ns: i64) -> Self {
        Self {
            data,
            hist_data: None,
            group_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
            lookback_ns: lookback_ns.max(1),
        }
    }

    /// Attach native-histogram samples (explicit-bucket form) for the
    /// histogram_* function family.
    pub fn with_hist_data(mut self, hist: &'a crate::ast::HistData) -> Self {
        self.hist_data = Some(hist);
        self
    }

    /// Top-level: evaluate for each step, handing each step's instant vector to
    /// `sink` as soon as it is produced.
    ///
    /// The previous signature returned `Vec<(i64, InstantVector)>` — every step
    /// of the range held live until the last one finished. For a 4-hour panel at
    /// a 15 s step that is 960 vectors of (series, value) pairs, which for 1 000
    /// series is ~160 MB resident purely in per-step vectors, on top of the
    /// output. Peak memory is now one step's vector, so it is O(series) rather
    /// than O(steps x series).
    ///
    /// `sink` takes the vector by value so it is freed as soon as the consumer
    /// is done with it.
    pub fn eval_steps_into<F>(
        &self,
        expr: &Expr,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        mut sink: F,
    ) -> Result<()>
    where
        F: FnMut(i64, InstantVector) -> Result<()>,
    {
        let mut ts = start_ns;
        while ts < end_ns {
            let ctx = EvalContext {
                ts_ns: ts,
                range_ns: 0,
                offset_ns: 0,
                subquery_step_ns: None,
            };
            let v = self.eval(expr, ctx)?;
            sink(ts, v)?;
            ts += step_ns;
        }
        Ok(())
    }

    /// Top-level: evaluate for each step, collecting every step's instant
    /// vector.
    ///
    /// Retains every step, so prefer [`Self::eval_steps_into`] whenever the
    /// consumer can fold incrementally. Kept because it is what the operator
    /// tests assert against and because collecting is occasionally the
    /// convenient shape.
    pub fn eval_steps(
        &self,
        expr: &Expr,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
    ) -> Result<Vec<(i64, InstantVector)>> {
        let mut out = Vec::new();
        self.eval_steps_into(expr, start_ns, end_ns, step_ns, |ts, v| {
            out.push((ts, v));
            Ok(())
        })?;
        Ok(out)
    }

    /// Evaluate an expression at one timestamp.
    pub fn eval(&self, expr: &Expr, ctx: EvalContext) -> Result<InstantVector> {
        match expr {
            Expr::Number(n) => Ok(InstantVector {
                // Scalars are handled specially in binary ops; as a plain
                // instant vector a scalar has empty labels.
                series: vec![(crate::ast::shared_labels(LabelSet::default()), *n)],
            }),
            Expr::Paren(inner) => self.eval(inner, ctx),
            Expr::Str(_) => Err(Error::Validation(
                "string literal is only valid as a function argument".into(),
            )),
            Expr::Selector(sel) => self.eval_selector(sel, ctx),
            Expr::Call(call) => self.eval_call(call, ctx),
            Expr::Aggregation(agg) => self.eval_aggregation(agg, ctx),
            Expr::Range(r) => self.eval_range_top(r, ctx),
            Expr::Binary(b) => self.eval_binary(b, ctx),
        }
    }
}

#[cfg(test)]
mod tests;
