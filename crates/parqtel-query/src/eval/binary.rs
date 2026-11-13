//! Binary operators, vector matching and scalar/string operands.

use super::Evaluator;
use crate::ast::*;
use parqtel_core::{Error, LabelSet, Result};
use std::collections::BTreeMap;

trait IsScalar {
    fn lhs_is_scalar(&self) -> bool {
        false
    }
    fn rhs_is_scalar(&self) -> bool {
        false
    }
}

impl IsScalar for BinaryExpr {}

fn filter_cmp(b: &BinaryExpr, v: f64) -> Option<f64> {
    if b.return_bool {
        Some(v)
    } else if b.op.is_comparison() && v == 0.0 {
        None // filtered out
    } else {
        Some(v)
    }
}

/// Result labels for a binary op: the label projection.
///
/// PromQL semantics:
/// - default (no modifier): LHS labels minus `__name__`
/// - `on(x, ...)`: ONLY the on-labels
/// - `ignoring(x, ...)`: LHS labels minus `__name__` minus the ignored set
///
/// Prometheus drops `__name__` from binary-op results and applies the
/// `on`/`ignoring` projection when one is present. In the overwhelmingly common
/// case of `foo / bar` — no projection, and no metric name to drop — the result
/// labels are identical to the LHS labels, so the caller's `Arc` is reused
/// instead of rebuilding the set. Otherwise a fresh set is built in one pass
/// (`filtered`), not by merging one label at a time.
fn result_labels(
    b: &BinaryExpr,
    lhs: &crate::ast::SharedLabels,
    _rhs: &crate::ast::SharedLabels,
) -> crate::ast::SharedLabels {
    let matching = b.matching.as_ref().map(|(vm, _)| vm);
    let set = lhs.as_ref();

    let projects = match matching {
        Some(VectorMatch::On(list)) => set.keys().any(|k| !list.contains(k)),
        Some(VectorMatch::Ignoring(list)) => set.keys().any(|k| list.contains(k)),
        Some(VectorMatch::All) | None => false,
    };
    let drops_name = set.get("__name__").is_some();

    if !projects && !drops_name {
        return crate::ast::SharedLabels::clone(lhs);
    }
    crate::ast::shared_labels(set.filtered(|k, _| {
        if k == "__name__" {
            return false;
        }
        match matching {
            Some(VectorMatch::On(list)) => list.iter().any(|l| l == k),
            Some(VectorMatch::Ignoring(list)) => !list.iter().any(|l| l == k),
            Some(VectorMatch::All) | None => true,
        }
    }))
}

fn apply_binary_op(op: BinaryOp, a: f64, b: f64, return_bool: bool) -> Result<f64> {
    Ok(match op {
        BinaryOp::Add => a + b,
        BinaryOp::Sub => a - b,
        BinaryOp::Mul => a * b,
        BinaryOp::Div => a / b,
        BinaryOp::Mod => a % b,
        BinaryOp::Pow => a.powf(b),
        BinaryOp::Eq => bool_f(a == b, return_bool, a),
        BinaryOp::Ne => bool_f(a != b, return_bool, a),
        BinaryOp::Gt => bool_f(a > b, return_bool, a),
        BinaryOp::Lt => bool_f(a < b, return_bool, a),
        BinaryOp::Ge => bool_f(a >= b, return_bool, a),
        BinaryOp::Le => bool_f(a <= b, return_bool, a),
        BinaryOp::And | BinaryOp::Or | BinaryOp::Unless => {
            return Err(Error::Validation("set op in scalar position".into()))
        }
    })
}

fn bool_f(cond: bool, return_bool: bool, value: f64) -> f64 {
    if return_bool {
        if cond {
            1.0
        } else {
            0.0
        }
    } else if cond {
        value
    } else {
        0.0 // filtered by filter_cmp for comparisons
    }
}

impl<'a> Evaluator<'a> {
    pub(super) fn eval_binary(&self, b: &BinaryExpr, ctx: EvalContext) -> Result<InstantVector> {
        let lhs = self.eval(&b.lhs, ctx)?;
        let rhs = self.eval(&b.rhs, ctx)?;

        // Scalar op scalar / vector-scalar mixing
        let lhs_scalar = b.lhs_is_scalar();
        let rhs_scalar = b.rhs_is_scalar();
        let _ = (lhs_scalar, rhs_scalar);

        match (&*b.lhs, &*b.rhs) {
            (Expr::Number(a), Expr::Number(bb)) => {
                let v = apply_binary_op(b.op, *a, *bb, b.return_bool)?;
                Ok(InstantVector {
                    series: vec![(crate::ast::shared_labels(LabelSet::default()), v)],
                })
            }
            (Expr::Number(a), _) => {
                // scalar op vector
                let mut out = Vec::new();
                for (labels, rv) in rhs.series {
                    let v = apply_binary_op(b.op, *a, rv, b.return_bool)?;
                    if let Some(v) = filter_cmp(b, v) {
                        out.push((labels, v));
                    }
                }
                Ok(InstantVector { series: out })
            }
            (_, Expr::Number(bb)) => {
                // vector op scalar
                let mut out = Vec::new();
                for (labels, lv) in lhs.series {
                    let v = apply_binary_op(b.op, lv, *bb, b.return_bool)?;
                    if let Some(v) = filter_cmp(b, v) {
                        out.push((labels, v));
                    }
                }
                Ok(InstantVector { series: out })
            }
            _ => {
                // vector op vector with optional matching
                self.eval_vector_binary(b, lhs, rhs)
            }
        }
    }

    fn eval_vector_binary(
        &self,
        b: &BinaryExpr,
        lhs: InstantVector,
        rhs: InstantVector,
    ) -> Result<InstantVector> {
        // Set operators operate on label-set membership.
        match b.op {
            BinaryOp::And => {
                let mut out = Vec::new();
                for (l, v) in lhs.series {
                    if rhs.get(&l).is_some() {
                        out.push((l, v));
                    }
                }
                return Ok(InstantVector { series: out });
            }
            BinaryOp::Or => {
                let mut out = lhs.series.clone();
                for (l, v) in rhs.series {
                    if lhs.get(&l).is_none() {
                        out.push((l, v));
                    }
                }
                return Ok(InstantVector { series: out });
            }
            BinaryOp::Unless => {
                let mut out = Vec::new();
                for (l, v) in lhs.series {
                    if rhs.get(&l).is_none() {
                        out.push((l, v));
                    }
                }
                return Ok(InstantVector { series: out });
            }
            _ => {}
        }

        let (vm, card) = b
            .matching
            .clone()
            .unwrap_or((VectorMatch::All, MatchCardinality::OneToOne));
        let vm = &vm;
        let card = &card;

        let match_key = |labels: &LabelSet| -> Vec<(String, String)> {
            let mut k = Vec::new();
            for (lk, lv) in labels.iter() {
                let lk = lk.to_string();
                if lk == "__name__" {
                    continue;
                }
                match &vm {
                    VectorMatch::All => k.push((lk, lv.to_string())),
                    VectorMatch::On(list) => {
                        if list.contains(&lk) {
                            k.push((lk, lv.to_string()));
                        }
                    }
                    VectorMatch::Ignoring(list) => {
                        if !list.contains(&lk) {
                            k.push((lk, lv.to_string()));
                        }
                    }
                }
            }
            k.sort();
            k
        };

        // Build rhs index by match key
        type MatchKey = Vec<(String, String)>;
        type Matches = Vec<(crate::ast::SharedLabels, f64)>;
        let mut rhs_index: BTreeMap<MatchKey, Matches> = BTreeMap::new();
        for (labels, v) in rhs.series {
            rhs_index
                .entry(match_key(&labels))
                .or_default()
                .push((labels, v));
        }

        let mut out = Vec::new();
        for (llabels, lv) in lhs.series {
            let key = match_key(&llabels);
            let matches = rhs_index.get(&key);
            match card {
                MatchCardinality::OneToOne => {
                    if let Some(matches) = matches {
                        if matches.len() == 1 {
                            let (rlabels, rv) = &matches[0];
                            let v = apply_binary_op(b.op, lv, *rv, b.return_bool)?;
                            if let Some(v) = filter_cmp(b, v) {
                                out.push((result_labels(b, &llabels, rlabels), v));
                            }
                        }
                        // multiple matches with 1:1 -> drop (Prometheus: many-to-many not allowed)
                    }
                }
                MatchCardinality::ManyToOne(extra) => {
                    if let Some(matches) = matches {
                        for (rlabels, rv) in matches {
                            let v = apply_binary_op(b.op, lv, *rv, b.return_bool)?;
                            if let Some(v) = filter_cmp(b, v) {
                                // group_left: projected match labels + extras from RHS
                                // group_left: LHS labels plus extras from RHS.
                                let base = result_labels(b, &llabels, rlabels);
                                let mut labels = (*base).clone();
                                for e in extra.iter() {
                                    if let Some(ev) = rlabels.get(e) {
                                        // `with` is a single-pass insert;
                                        // `merge` with a one-entry set clones
                                        // the whole map.
                                        labels = labels.with(e, ev);
                                    }
                                }
                                out.push((crate::ast::shared_labels(labels), v));
                            }
                        }
                    }
                }
                MatchCardinality::OneToMany(_) => {
                    // group_right: RHS side may have many LHS matches; swap roles
                    if let Some(matches) = matches {
                        for (rlabels, rv) in matches {
                            let v = apply_binary_op(b.op, lv, *rv, b.return_bool)?;
                            if let Some(v) = filter_cmp(b, v) {
                                out.push((result_labels(b, rlabels, &llabels), v));
                            }
                        }
                    }
                }
            }
        }
        Ok(InstantVector { series: out })
    }

    pub(super) fn eval_scalar(&self, expr: &Expr, ctx: EvalContext) -> Result<f64> {
        match expr {
            Expr::Number(n) => Ok(*n),
            other => {
                let v = self.eval(other, ctx)?;
                if v.series.len() == 1 {
                    Ok(v.series[0].1)
                } else {
                    Ok(f64::NAN)
                }
            }
        }
    }

    pub(super) fn eval_string(&self, expr: &Expr, _ctx: EvalContext) -> Result<String> {
        match expr {
            Expr::Str(s) => Ok(s.clone()),
            other => Err(Error::Validation(format!(
                "expected string literal, got {other:?}"
            ))),
        }
    }
}
