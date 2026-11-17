//! Per-metric series admission.
//!
//! A label explosion in one metric must not degrade every other series. The
//! limiter remembers the stable fingerprint of each admitted label set and
//! refuses new ones past the configured cap; existing series keep ingesting.

use parqtel_core::{DataPoint, Metric};
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

#[derive(Default)]
pub struct SeriesLimiter {
    limit: usize,
    series: Mutex<HashMap<String, HashSet<u64>>>,
    rejected: Mutex<HashMap<String, u64>>,
}

impl SeriesLimiter {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            ..Self::default()
        }
    }

    /// Split a metric's points into admitted and rejected. The metric's
    /// identity stays intact; callers replace `data_points` with the result.
    pub fn admit(&self, metric: &Metric) -> (Vec<DataPoint>, u64) {
        if self.limit == 0 {
            return (metric.data_points.clone(), 0);
        }
        let Ok(mut series) = self.series.lock() else {
            return (metric.data_points.clone(), 0);
        };
        let known = series.entry(metric.name.clone()).or_default();
        let mut admitted = Vec::with_capacity(metric.data_points.len());
        let mut rejected = 0u64;
        for point in &metric.data_points {
            let fingerprint = point.labels.fingerprint();
            if known.contains(&fingerprint) {
                admitted.push(point.clone());
            } else if known.len() < self.limit {
                known.insert(fingerprint);
                admitted.push(point.clone());
            } else {
                rejected += 1;
            }
        }
        drop(series);
        if rejected > 0 {
            if let Ok(mut counts) = self.rejected.lock() {
                *counts.entry(metric.name.clone()).or_default() += rejected;
            }
        }
        (admitted, rejected)
    }

    /// Prometheus text for rejected points, labelled by metric name.
    pub fn render_metrics(&self) -> String {
        let Ok(rejected) = self.rejected.lock() else {
            return String::new();
        };
        if rejected.is_empty() {
            return String::new();
        }
        let mut out = String::from(
            "# HELP parqtel_series_rejected_total Points rejected by the per-metric series cap\n# TYPE parqtel_series_rejected_total counter\n",
        );
        let mut names: Vec<_> = rejected.iter().collect();
        names.sort_by_key(|(name, _)| *name);
        for (name, count) in names {
            out.push_str(&format!(
                "parqtel_series_rejected_total{{metric=\"{}\"}} {count}\n",
                name.replace('\\', "\\\\").replace('"', "\\\"")
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use parqtel_core::{LabelSet, MetricKind, MetricValue};

    fn metric(name: &str, labels: &[&str]) -> Metric {
        Metric {
            name: name.into(),
            description: String::new(),
            unit: String::new(),
            kind: MetricKind::Gauge,
            resource_attributes: LabelSet::default(),
            data_points: labels
                .iter()
                .map(|label| DataPoint {
                    timestamp_ns: 1,
                    value: MetricValue::Double(1.0),
                    labels: LabelSet::try_from_iter(vec![("user", *label)]).unwrap(),
                })
                .collect(),
        }
    }

    #[test]
    fn admits_existing_series_and_rejects_new_ones() {
        let limiter = SeriesLimiter::new(2);
        let (admitted, rejected) = limiter.admit(&metric("m", &["a", "b", "a"]));
        assert_eq!(admitted.len(), 3);
        assert_eq!(rejected, 0);
        let (admitted, rejected) = limiter.admit(&metric("m", &["c"]));
        assert!(admitted.is_empty());
        assert_eq!(rejected, 1);
        assert!(limiter.render_metrics().contains("metric=\"m\""));
    }

    #[test]
    fn zero_means_unlimited() {
        let limiter = SeriesLimiter::new(0);
        let (admitted, rejected) = limiter.admit(&metric("m", &["a", "b"]));
        assert_eq!(admitted.len(), 2);
        assert_eq!(rejected, 0);
    }
}
