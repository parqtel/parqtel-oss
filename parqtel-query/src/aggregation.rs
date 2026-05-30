use parqtel_core::MetricValue;
use crate::models::Sample;

/// Aggregates a window of data points into a single sample value.
pub fn aggregate(
    op: crate::plan::AggregationOp,
    points: &[(i64, MetricValue)],
    quantile: Option<f64>,
) -> Option<f64> {
    if points.is_empty() {
        return None;
    }

    match op {
        crate::plan::AggregationOp::Avg => {
            let sum: f64 = points.iter().map(|(_, v)| v_to_float(v)).sum();
            Some(sum / points.len() as f64)
        }
        crate::plan::AggregationOp::Sum => {
            Some(points.iter().map(|(_, v)| v_to_float(v)).sum())
        }
        crate::plan::AggregationOp::Min => {
            points.iter().map(|(_, v)| v_to_float(v)).min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        crate::plan::AggregationOp::Max => {
            points.iter().map(|(_, v)| v_to_float(v)).max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        crate::plan::AggregationOp::Count => {
            Some(points.len() as f64)
        }
        crate::plan::AggregationOp::Rate => {
            if points.len() < 2 { return None; }
            let first = &points[0];
            let last = &points[points.len() - 1];
            let dt = (last.0 - first.0) as f64 / 1_000_000_000.0;
            if dt <= 0.0 { return None; }
            let dv = v_to_float(&last.1) - v_to_float(&first.1);
            Some(dv / dt)
        }
        crate::plan::AggregationOp::HistogramQuantile => {
            let q = quantile?;
            // OTel histograms in one window usually come from one series.
            // We'll take the last one in the window (most recent cumulative)
            // or merge them if they are deltas. OTLP usually sends cumulative.
            if let (_, MetricValue::Histogram { boundaries, counts, .. }) = &points[points.len() - 1] {
                Some(estimate_quantile(boundaries, counts, q))
            } else {
                None
            }
        }
    }
}

fn v_to_float(v: &MetricValue) -> f64 {
    match v {
        MetricValue::Double(f) => *f,
        MetricValue::Int(i) => *i as f64,
        MetricValue::Histogram { sum, .. } => *sum,
        MetricValue::Summary { sum, .. } => *sum,
    }
}

fn estimate_quantile(boundaries: &[f64], counts: &[u64], q: f64) -> f64 {
    if counts.is_empty() { return 0.0; }
    let total: u64 = counts.iter().sum();
    if total == 0 { return 0.0; }
    
    let target = q * total as f64;
    let mut cumulative = 0.0;
    
    for (i, &count) in counts.iter().enumerate() {
        let prev_cumulative = cumulative;
        cumulative += count as f64;
        
        if cumulative >= target {
            let lower = if i == 0 { 0.0 } else { boundaries[i - 1] };
            let upper = if i < boundaries.len() { boundaries[i] } else { lower * 2.0 }; // Heuristic for last bucket
            
            // Linear interpolation within bucket
            let fraction = (target - prev_cumulative) / (cumulative - prev_cumulative);
            return lower + (upper - lower) * fraction;
        }
    }
    
    *boundaries.last().unwrap_or(&0.0)
}

/// Divides a time range into equal-width step windows and applies an aggregation.
pub fn downsample(
    points: Vec<(i64, MetricValue)>,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    op: crate::plan::AggregationOp,
    quantile: Option<f64>,
) -> Vec<Sample> {
    let mut samples = Vec::new();
    let mut current_window_start = start_ns;
    let mut points_idx = 0;

    while current_window_start < end_ns {
        let window_end = current_window_start + step_ns;
        let mut window_points = Vec::new();
        
        while points_idx < points.len() && points[points_idx].0 < window_end {
            if points[points_idx].0 >= current_window_start {
                window_points.push(points[points_idx].clone());
            }
            points_idx += 1;
        }

        if let Some(val) = aggregate(op, &window_points, quantile) {
            samples.push(Sample {
                timestamp_ns: current_window_start,
                value: val,
            });
        }
        
        current_window_start = window_end;
    }

    samples
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::plan::AggregationOp;

    #[test]
    fn test_average() {
        let points = vec![
            (100, MetricValue::Double(10.0)),
            (200, MetricValue::Double(20.0)),
            (300, MetricValue::Double(30.0)),
        ];
        let val = aggregate(AggregationOp::Avg, &points, None).unwrap();
        assert_eq!(val, 20.0);
    }

    #[test]
    fn test_min_max_sum_count() {
        let points = vec![
            (100, MetricValue::Double(10.0)),
            (200, MetricValue::Double(5.0)),
            (300, MetricValue::Double(15.0)),
        ];
        assert_eq!(aggregate(AggregationOp::Min, &points, None), Some(5.0));
        assert_eq!(aggregate(AggregationOp::Max, &points, None), Some(15.0));
        assert_eq!(aggregate(AggregationOp::Sum, &points, None), Some(30.0));
        assert_eq!(aggregate(AggregationOp::Count, &points, None), Some(3.0));
    }

    #[test]
    fn test_rate() {
        let points = vec![
            (1_000_000_000, MetricValue::Double(10.0)),
            (2_000_000_000, MetricValue::Double(20.0)),
        ];
        let val = aggregate(AggregationOp::Rate, &points, None).unwrap();
        assert_eq!(val, 10.0); // 10 units / 1 second
    }

    #[test]
    fn test_downsampling() {
        let points = vec![
            (100, MetricValue::Double(10.0)),
            (150, MetricValue::Double(20.0)),
            (250, MetricValue::Double(30.0)),
        ];
        // Windows: [100, 200), [200, 300)
        let res = downsample(points, 100, 300, 100, AggregationOp::Sum, None);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].value, 30.0);
        assert_eq!(res[1].value, 30.0);
    }

    #[test]
    fn test_downsampling_with_gap() {
        let points = vec![
            (100, MetricValue::Double(10.0)),
            (350, MetricValue::Double(20.0)),
        ];
        // Windows: [100, 200), [200, 300), [300, 400)
        let res = downsample(points, 100, 400, 100, AggregationOp::Sum, None);
        assert_eq!(res.len(), 2);
        assert_eq!(res[0].timestamp_ns, 100);
        assert_eq!(res[1].timestamp_ns, 300);
    }

    #[test]
    fn test_histogram_quantile() {
        let points = vec![
            (100, MetricValue::Histogram {
                count: 10,
                sum: 50.0,
                min: None,
                max: None,
                boundaries: vec![1.0, 5.0, 10.0],
                counts: vec![2, 3, 4, 1], // [0,1), [1,5), [5,10), [10,inf)
            }),
        ];
        let val = aggregate(AggregationOp::HistogramQuantile, &points, Some(0.5)).unwrap();
        // Target = 0.5 * 10 = 5th sample.
        // B1: 2 samples. B2: 3 samples. Total 5 samples.
        // 5th sample is at the end of B2 (upper bound 5.0).
        assert_eq!(val, 5.0);
        
        // Edge cases
        assert_eq!(estimate_quantile(&[], &[], 0.5), 0.0);
        assert_eq!(estimate_quantile(&[1.0], &[0, 0], 0.5), 0.0);
    }
}
