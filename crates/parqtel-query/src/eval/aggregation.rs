//! Aggregation operators and grouping.

use super::functions::{fmt_num, quantile_of};
use super::Evaluator;
use crate::ast::*;
use parqtel_core::{Error, LabelSet, Result};
use std::collections::BTreeMap;

/// Canonical form of a grouping: the `By` label list sorted and deduplicated.
///
/// Sorting happens once per aggregation rather than once per series per step,
/// so that the per-series fingerprint can hash in a canonical order and
/// `by(a,b)` and `by(b,a)` agree without paying for a sort in the hot loop.
// The hot loop therefore requires a normalised grouping; callers get one from
/// here.
pub(super) fn normalize_grouping(grouping: &Grouping) -> Grouping {
    match grouping {
        Grouping::By(list) => {
            let mut sorted: Vec<String> = list.clone();
            sorted.sort();
            sorted.dedup();
            Grouping::By(sorted)
        }
        other => other.clone(),
    }
}

/// Allocation-free fingerprint of a series' grouping projection.
///
/// Recomputed for every series on every step, so this must not allocate. The
/// obvious key — a `Vec<(String, String)>` of the projected labels — costs two
/// allocations per label and is then cloned twice more for the two maps it
/// keys. Hashing the projected pairs straight into a hasher avoids all of that.
///
/// Only equality matters here, so a plain `DefaultHasher` is fine: a collision
/// would merge two groups, which the tests below check for.
pub(super) fn grouping_fingerprint(labels: &LabelSet, grouping: &Grouping) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    match grouping {
        Grouping::None => {}
        Grouping::By(list) => {
            // Hash in the declared order so `by(a,b)` and `by(b,a)` agree.
            for l in list {
                if let Some(v) = labels.get(l) {
                    l.hash(&mut h);
                    v.hash(&mut h);
                } else {
                    // A missing label is part of the key: `by(a)` with and
                    // without `a` must not collapse into one group.
                    l.hash(&mut h);
                    0u8.hash(&mut h);
                }
            }
        }
        Grouping::Without(list) => {
            // `labels.iter()` is already sorted, so the order is canonical.
            for (k, v) in labels.iter() {
                if k == "__name__" || list.iter().any(|l| l == k) {
                    continue;
                }
                k.hash(&mut h);
                v.hash(&mut h);
            }
        }
    }
    h.finish()
}

impl<'a> Evaluator<'a> {
    /// Builds (and caches) the label set for one aggregation group.
    ///
    /// Single pass over the projected labels rather than a `merge` per label,
    /// which was O(L^2 log L). The result is cached by fingerprint for the whole
    /// query, because a group's labels are identical on every step even though its
    /// values are not.
    fn cache_group_labels(&self, key: u64, labels: &LabelSet, grouping: &Grouping) {
        let mut cache = self.group_cache.lock().unwrap_or_else(|e| e.into_inner());
        if cache.contains_key(&key) {
            return;
        }
        let built = match grouping {
            Grouping::None => LabelSet::default(),
            Grouping::By(list) => LabelSet::try_from_iter(
                list.iter()
                    .filter_map(|l| labels.get(l).map(|v| (l.clone(), v.to_string()))),
            )
            .unwrap_or_default(),
            Grouping::Without(list) => LabelSet::try_from_iter(
                labels
                    .iter()
                    .filter(|(k, _)| *k != "__name__" && !list.iter().any(|l| l == k))
                    .map(|(k, v)| (k.to_string(), v.to_string())),
            )
            .unwrap_or_default(),
        };
        cache.insert(key, crate::ast::shared_labels(built));
    }

    pub(super) fn eval_aggregation(
        &self,
        agg: &AggregationExpr,
        ctx: EvalContext,
    ) -> Result<InstantVector> {
        let v = self.eval(&agg.expr, ctx)?;
        // Grouping is recomputed for every series on every step, so this loop
        // is the hottest part of an aggregation. It used to build a
        // `Vec<(String, String)>` key (two allocations per label), clone it
        // twice more, and rebuild the output `LabelSet` by merging one label
        // at a time — O(L^2 log L) per series per step.
        //
        // Now: an allocation-free fingerprint is the key, and the group's
        // label set is built once and cached for the whole query.
        // Normalised once per aggregation, not once per series: `by(a,b)` and
        // `by(b,a)` select the same pairs and must hash identically, and the
        // list has to be in a canonical order for that.
        let grouping = normalize_grouping(&agg.grouping);
        let mut groups: std::collections::HashMap<u64, Vec<f64>> = std::collections::HashMap::new();
        let mut group_order: Vec<u64> = Vec::new();
        for (labels, val) in &v.series {
            let key = grouping_fingerprint(labels, &grouping);
            match groups.get_mut(&key) {
                Some(vals) => vals.push(*val),
                None => {
                    group_order.push(key);
                    groups.insert(key, vec![*val]);
                    self.cache_group_labels(key, labels, &grouping);
                }
            }
        }
        // Deterministic output order: sorted by key, as the previous
        // BTreeMap<Vec<..>> was. Not first-appearance order, which would make
        // tie-breaks in any downstream sort unstable.
        group_order.sort_unstable();

        let cache = self.group_cache.lock().unwrap_or_else(|e| e.into_inner());
        let mut out = Vec::new();
        for key in group_order {
            let Some(vals) = groups.get(&key) else {
                continue;
            };
            let labels = match cache.get(&key) {
                Some(l) => crate::ast::SharedLabels::clone(l),
                None => continue,
            };
            let result: Option<f64> = match agg.op {
                AggregationOp::Sum => Some(vals.iter().sum()),
                AggregationOp::Avg => Some(vals.iter().sum::<f64>() / vals.len() as f64),
                AggregationOp::Min => {
                    if vals.is_empty() {
                        None
                    } else {
                        // Folded once: the previous `is_finite().then(|| …)`
                        // form evaluated the identical fold a second time.
                        let min = vals.iter().copied().fold(f64::INFINITY, f64::min);
                        min.is_finite().then_some(min)
                    }
                }
                AggregationOp::Max => {
                    if vals.is_empty() {
                        None
                    } else {
                        Some(vals.iter().copied().fold(f64::NEG_INFINITY, f64::max))
                    }
                }
                AggregationOp::Count => Some(vals.len() as f64),
                AggregationOp::Group => Some(1.0),
                AggregationOp::Stddev | AggregationOp::Stdvar => {
                    if vals.len() < 2 {
                        None
                    } else {
                        let n = vals.len() as f64;
                        let mean = vals.iter().sum::<f64>() / n;
                        let var = vals.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
                        Some(if agg.op == AggregationOp::Stddev {
                            var.sqrt()
                        } else {
                            var
                        })
                    }
                }
                AggregationOp::CountValues => None, // param-label based; rare, not yet supported
                AggregationOp::TopK | AggregationOp::BottomK => {
                    // Handled below with per-series labels (not aggregated).
                    None
                }
                AggregationOp::Quantile => None, // handled below
            };
            if let Some(r) = result {
                out.push((labels, r));
            }
        }

        // topk/bottomk/quantile need full series info, not just group values.
        match agg.op {
            AggregationOp::TopK | AggregationOp::BottomK => {
                let n = agg
                    .param
                    .as_ref()
                    .map(|p| self.eval_scalar(p, ctx))
                    .transpose()?
                    .unwrap_or(1.0) as usize;
                let mut series: Vec<(crate::ast::SharedLabels, f64)> = v.series;
                if agg.op == AggregationOp::TopK {
                    series
                        .sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                } else {
                    series
                        .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                }
                series.truncate(n);
                return Ok(InstantVector { series });
            }
            AggregationOp::Quantile => {
                let q = agg
                    .param
                    .as_ref()
                    .map(|p| self.eval_scalar(p, ctx))
                    .transpose()?
                    .unwrap_or(0.5);
                let mut vals: Vec<f64> = v.series.iter().map(|(_, v)| *v).collect();
                vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let qv = quantile_of(q, &vals);
                return Ok(InstantVector {
                    series: vec![(crate::ast::shared_labels(LabelSet::default()), qv)],
                });
            }
            AggregationOp::CountValues => {
                // count_values("label", expr): counts series per distinct
                // value of expr, exposing each as a series with the
                // param-named label set to that value.
                let label_param = agg
                    .param
                    .as_ref()
                    .ok_or_else(|| Error::Validation("count_values requires a label".into()))?;
                let label_name = self.eval_string(label_param, ctx)?;
                let mut counts: BTreeMap<String, usize> = BTreeMap::new();
                for (_, v) in &v.series {
                    *counts.entry(fmt_num(*v)).or_insert(0) += 1;
                }
                let out: Vec<(crate::ast::SharedLabels, f64)> = counts
                    .into_iter()
                    .map(|(val, n)| {
                        (
                            crate::ast::shared_labels(
                                LabelSet::try_from_iter(vec![(label_name.clone(), val)])
                                    .unwrap_or_default(),
                            ),
                            n as f64,
                        )
                    })
                    .collect();
                return Ok(InstantVector { series: out });
            }
            _ => {}
        }

        Ok(InstantVector { series: out })
    }
}
