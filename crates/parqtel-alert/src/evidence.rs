//! Bounded evidence and deterministic RCA attached to an alert.

use serde::{Deserialize, Serialize};

/// Maximum points retained in one alert evidence snapshot.
pub const MAX_EVIDENCE_POINTS: usize = 120;

/// One point in the bounded evidence graph.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct EvidencePoint {
    pub timestamp_ns: i64,
    pub value: f64,
}

/// Data needed to explain and redraw an alert without re-running the query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertEvidence {
    pub query: String,
    pub signal: String,
    pub window_start_ns: i64,
    pub window_end_ns: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operator: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    pub observed: f64,
    #[serde(default)]
    pub points: Vec<EvidencePoint>,
    #[serde(default)]
    pub series_count: usize,
}

impl AlertEvidence {
    /// Keep the first/last sample and the min/max of each bucket when a series
    /// is longer than [`MAX_EVIDENCE_POINTS`].
    pub fn bounded_points<I>(points: I, max: usize) -> Vec<EvidencePoint>
    where
        I: IntoIterator<Item = (i64, f64)>,
    {
        let all: Vec<EvidencePoint> = points
            .into_iter()
            .filter(|(_, value)| value.is_finite())
            .map(|(timestamp_ns, value)| EvidencePoint {
                timestamp_ns,
                value,
            })
            .collect();
        let max = max.clamp(2, 4_096);
        if all.len() <= max {
            return all;
        }

        let buckets = (max.saturating_sub(2) / 2).max(1);
        let mut out = Vec::with_capacity(max);
        out.push(all[0]);
        out.push(all[all.len() - 1]);
        for bucket in 0..buckets {
            let start = bucket * all.len() / buckets;
            let end = (bucket + 1) * all.len() / buckets;
            let slice = &all[start..end.max(start + 1)];
            let min = slice
                .iter()
                .min_by(|a, b| a.value.total_cmp(&b.value))
                .copied();
            let max = slice
                .iter()
                .max_by(|a, b| a.value.total_cmp(&b.value))
                .copied();
            if let Some(point) = min {
                out.push(point);
            }
            if let Some(point) = max {
                if min.map(|p| p.timestamp_ns) != Some(point.timestamp_ns) {
                    out.push(point);
                }
            }
        }
        out.sort_by_key(|point| point.timestamp_ns);
        out.dedup_by_key(|point| point.timestamp_ns);
        out.truncate(max);
        out
    }

    pub fn point_count(&self) -> usize {
        self.points.len()
    }

    /// Compact text graph for notification channels that cannot embed an
    /// image. The SVG endpoint remains the precise renderer.
    pub fn sparkline(&self, width: usize) -> String {
        const LEVELS: [char; 8] = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'];
        if self.points.len() < 2 {
            return String::new();
        }
        let width = width.clamp(8, 80);
        let stride = (self.points.len() as f64 / width as f64).ceil() as usize;
        let sampled: Vec<f64> = self
            .points
            .iter()
            .step_by(stride.max(1))
            .map(|point| point.value)
            .collect();
        let min = sampled.iter().copied().fold(f64::INFINITY, f64::min);
        let max = sampled.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        if !min.is_finite() || !max.is_finite() {
            return String::new();
        }
        let span = (max - min).max(1e-12);
        sampled
            .into_iter()
            .map(|value| {
                let level = ((value - min) / span * (LEVELS.len() - 1) as f64)
                    .round()
                    .clamp(0.0, (LEVELS.len() - 1) as f64) as usize;
                LEVELS[level]
            })
            .collect()
    }
}

/// Human-readable analysis derived from the evidence snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RcaSummary {
    /// Short identifier for the analysis method, e.g. `threshold`.
    pub method: String,
    /// Display name shown by the console.
    pub model: String,
    pub confidence: f32,
    pub primary_cause: String,
    pub evidence: Vec<String>,
    pub actions: Vec<String>,
}

impl RcaSummary {
    pub fn threshold(
        rule_name: &str,
        observed: f64,
        operator: &str,
        threshold: f64,
        sustained_secs: u64,
        points: &[EvidencePoint],
    ) -> Self {
        let (min, max, avg) = point_stats(points);
        let mut evidence = vec![
            format!("observed {observed:.6} vs threshold {operator} {threshold:.6}"),
            format!("{} evidence point(s) in the incident window", points.len()),
        ];
        if points.len() > 1 {
            evidence.push(format!("range {min:.6}..{max:.6}, mean {avg:.6}"));
        }
        if sustained_secs > 0 {
            evidence.push(format!("condition sustained for {sustained_secs}s"));
        }
        Self {
            method: "threshold".into(),
            model: "parqtel-threshold".into(),
            confidence: 1.0,
            primary_cause: format!(
                "{rule_name} crossed its threshold: {observed:.6} {operator} {threshold:.6}."
            ),
            evidence,
            actions: vec![
                "Inspect the metric graph and correlated logs in the alert evidence tab.".into(),
                "Check recent deployments and dependency health for this service.".into(),
            ],
        }
    }

    pub fn mad(
        rule_name: &str,
        observed: f64,
        median: f64,
        mad: f64,
        sensitivity: f64,
        points: &[EvidencePoint],
    ) -> Self {
        let deviation = if mad > 0.0 {
            (observed - median).abs() / mad
        } else {
            f64::INFINITY
        };
        let confidence = (deviation / (sensitivity.max(1.0) * 2.0)).clamp(0.5, 1.0) as f32;
        let (min, max, avg) = point_stats(points);
        Self {
            method: "mad".into(),
            model: "parqtel-mad".into(),
            confidence,
            primary_cause: format!(
                "{rule_name} deviated from its median baseline by {deviation:.2} MAD."
            ),
            evidence: vec![
                format!("observed {observed:.6}, baseline median {median:.6}, MAD {mad:.6}"),
                format!("sensitivity {sensitivity:.2} MAD"),
                format!("range {min:.6}..{max:.6}, mean {avg:.6}"),
            ],
            actions: vec![
                "Compare the current value with the baseline graph.".into(),
                "Check whether traffic, capacity, or a deploy changed near the deviation.".into(),
            ],
        }
    }
}

fn point_stats(points: &[EvidencePoint]) -> (f64, f64, f64) {
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;
    let mut sum = 0.0;
    for point in points {
        min = min.min(point.value);
        max = max.max(point.value);
        sum += point.value;
    }
    if points.is_empty() {
        (0.0, 0.0, 0.0)
    } else {
        (min, max, sum / points.len() as f64)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

    use super::*;

    #[test]
    fn downsample_is_bounded_and_keeps_extrema() {
        let points = (0..10_000).map(|i| (i, (i as f64).sin()));
        let bounded = AlertEvidence::bounded_points(points, MAX_EVIDENCE_POINTS);
        assert!(bounded.len() <= MAX_EVIDENCE_POINTS);
        assert!(bounded
            .windows(2)
            .all(|pair| pair[0].timestamp_ns <= pair[1].timestamp_ns));
        assert!((bounded[0].value - 0.0).abs() < 1e-9);
    }

    #[test]
    fn sparkline_uses_multiple_levels() {
        let evidence = AlertEvidence {
            query: "up".into(),
            signal: "metrics".into(),
            window_start_ns: 0,
            window_end_ns: 3,
            operator: None,
            threshold: None,
            observed: 3.0,
            points: vec![
                EvidencePoint {
                    timestamp_ns: 1,
                    value: 0.0,
                },
                EvidencePoint {
                    timestamp_ns: 2,
                    value: 0.5,
                },
                EvidencePoint {
                    timestamp_ns: 3,
                    value: 1.0,
                },
            ],
            series_count: 1,
        };
        let graph = evidence.sparkline(30);
        assert!(graph.chars().count() >= 3);
        assert!(graph.contains('▁'));
        assert!(graph.contains('█'));
    }

    #[test]
    fn threshold_analysis_mentions_values_and_actions() {
        let points = vec![
            EvidencePoint {
                timestamp_ns: 1,
                value: 2.0,
            },
            EvidencePoint {
                timestamp_ns: 2,
                value: 4.0,
            },
        ];
        let rca = RcaSummary::threshold("High errors", 4.0, ">", 1.0, 60, &points);
        assert_eq!(rca.method, "threshold");
        assert!(rca.primary_cause.contains("4.000000"));
        assert!(rca
            .evidence
            .iter()
            .any(|line| line.contains("range 2.000000..4.000000")));
        assert!(!rca.actions.is_empty());
    }
}
