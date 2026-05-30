use serde::{Deserialize, Serialize};
use crate::models::labels::LabelSet;
use crate::error::{Error, Result};

/// Represents the kind of a metric as defined by OpenTelemetry.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Default)]
pub enum MetricKind {
    /// A gauge represents a single numerical value that can arbitrarily go up and down.
    #[default]
    Gauge,
    /// A sum represents the total of a property over time.
    Sum,
    /// A histogram represents a distribution of values.
    Histogram,
    /// A summary represents a distribution of values with quantiles.
    Summary,
}

/// The value of a single data point, varying by [MetricKind].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricValue {
    /// A single floating point value (Gauge or Sum).
    Double(f64),
    /// A single integer value (Sum).
    Int(i64),
    /// Histogram bucket data.
    Histogram {
        /// Number of samples.
        count: u64,
        /// Sum of all samples.
        sum: f64,
        /// Optional minimum value in the distribution.
        min: Option<f64>,
        /// Optional maximum value in the distribution.
        max: Option<f64>,
        /// Explicit bucket boundaries.
        boundaries: Vec<f64>,
        /// Counts for each bucket defined by boundaries.
        counts: Vec<u64>,
    },
    /// Summary quantile data.
    Summary {
        /// Number of samples.
        count: u64,
        /// Sum of all samples.
        sum: f64,
        /// Quantile values.
        quantiles: Vec<(f64, f64)>,
    },
}

/// A single data point for a metric.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    /// Nanosecond timestamp of the data point.
    pub timestamp_ns: i64,
    /// Value of the data point.
    pub value: MetricValue,
    /// Labels associated with this specific data point.
    pub labels: LabelSet,
}

impl DataPoint {
    /// Creates a new [DataPoint] and validates the timestamp.
    pub fn new(timestamp_ns: i64, value: MetricValue, labels: LabelSet) -> Result<Self> {
        if timestamp_ns <= 0 {
            return Err(Error::Validation("Data point timestamp must be greater than zero".into()));
        }
        Ok(Self {
            timestamp_ns,
            value,
            labels,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Metric {
    /// Unique name of the metric.
    pub name: String,
    /// Description of the metric.
    pub description: String,
    /// Unit of measure.
    pub unit: String,
    /// Kind of the metric.
    pub kind: MetricKind,
    /// Attributes of the resource that produced this metric (e.g. service.name).
    pub resource_attributes: LabelSet,
    /// List of data points for this metric.
    pub data_points: Vec<DataPoint>,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_data_point_valid() {
        let dp = DataPoint::new(100, MetricValue::Double(42.0), LabelSet::default());
        assert!(dp.is_ok());
    }

    #[test]
    fn test_data_point_zero_timestamp_invalid() {
        let dp = DataPoint::new(0, MetricValue::Double(1.0), LabelSet::default());
        assert!(dp.is_err());
    }

    #[test]
    fn test_data_point_negative_timestamp_invalid() {
        let dp = DataPoint::new(-1, MetricValue::Int(1), LabelSet::default());
        assert!(dp.is_err());
    }

    #[test]
    fn test_metric_kind_default() {
        assert_eq!(MetricKind::default(), MetricKind::Gauge);
    }

    #[test]
    fn test_metric_value_serialization() {
        let values = vec![
            MetricValue::Double(3.14),
            MetricValue::Int(42),
            MetricValue::Histogram { count: 10, sum: 100.0, min: Some(1.0), max: Some(20.0), boundaries: vec![5.0, 10.0], counts: vec![3, 5, 2] },
            MetricValue::Summary { count: 5, sum: 50.0, quantiles: vec![(0.5, 10.0), (0.99, 20.0)] },
        ];
        for v in values {
            let json = serde_json::to_string(&v).unwrap();
            let decoded: MetricValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, decoded);
        }
    }
}
