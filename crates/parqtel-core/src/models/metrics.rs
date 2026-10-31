use crate::error::{Error, Result};
use crate::models::labels::LabelSet;
use serde::{Deserialize, Serialize};

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

/// Histogram bucket data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HistogramValue {
    /// Number of samples.
    pub count: u64,
    /// Sum of all samples.
    pub sum: f64,
    /// Optional minimum value in the distribution.
    pub min: Option<f64>,
    /// Optional maximum value in the distribution.
    pub max: Option<f64>,
    /// Explicit bucket boundaries.
    pub boundaries: Vec<f64>,
    /// Counts for each bucket defined by boundaries.
    pub counts: Vec<u64>,
}

/// Summary quantile data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SummaryValue {
    /// Number of samples.
    pub count: u64,
    /// Sum of all samples.
    pub sum: f64,
    /// Quantile values.
    pub quantiles: Vec<(f64, f64)>,
}

/// The value of a single data point, varying by [MetricKind].
///
/// Histogram and summary payloads are boxed: they are an order of magnitude
/// larger than a number, and almost every point is a number. Inline, they made
/// every [`DataPoint`] 112 bytes; boxed, a point is 32. The serialised form is
/// unchanged - a boxed struct encodes exactly like the struct.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MetricValue {
    /// A single floating point value (Gauge or Sum).
    Double(f64),
    /// A single integer value (Sum).
    Int(i64),
    /// Histogram bucket data.
    Histogram(Box<HistogramValue>),
    /// Summary quantile data.
    Summary(Box<SummaryValue>),
}

impl MetricValue {
    /// Builds a histogram value.
    pub fn histogram(
        count: u64,
        sum: f64,
        min: Option<f64>,
        max: Option<f64>,
        boundaries: Vec<f64>,
        counts: Vec<u64>,
    ) -> Self {
        Self::Histogram(Box::new(HistogramValue {
            count,
            sum,
            min,
            max,
            boundaries,
            counts,
        }))
    }

    /// Builds a summary value.
    pub fn summary(count: u64, sum: f64, quantiles: Vec<(f64, f64)>) -> Self {
        Self::Summary(Box::new(SummaryValue {
            count,
            sum,
            quantiles,
        }))
    }
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
            return Err(Error::Validation(
                "Data point timestamp must be greater than zero".into(),
            ));
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
            MetricValue::Double(3.15),
            MetricValue::Int(42),
            MetricValue::histogram(
                10,
                100.0,
                Some(1.0),
                Some(20.0),
                vec![5.0, 10.0],
                vec![3, 5, 2],
            ),
            MetricValue::summary(5, 50.0, vec![(0.5, 10.0), (0.99, 20.0)]),
        ];
        for v in values {
            let json = serde_json::to_string(&v).unwrap();
            let decoded: MetricValue = serde_json::from_str(&json).unwrap();
            assert_eq!(v, decoded);
        }
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    /// Points are held by the million in buffers and writers; this is the size
    /// that keeps them cheap. A new inline variant would silently undo it.
    #[test]
    fn data_point_stays_small() {
        assert!(std::mem::size_of::<MetricValue>() <= 16);
        assert!(std::mem::size_of::<DataPoint>() <= 32);
    }

    /// WAL records written by earlier builds hold the struct-variant encoding.
    #[test]
    fn serialised_form_matches_the_struct_variant_encoding() {
        let v = MetricValue::histogram(3, 1.5, None, Some(2.0), vec![1.0], vec![1, 2]);
        let json = serde_json::to_string(&v).unwrap_or_default();
        assert_eq!(
            json,
            r#"{"Histogram":{"count":3,"sum":1.5,"min":null,"max":2.0,"boundaries":[1.0],"counts":[1,2]}}"#
        );
        let back: MetricValue = serde_json::from_str(&json).unwrap_or(MetricValue::Int(0));
        assert_eq!(back, v);
        let s = MetricValue::summary(2, 3.0, vec![(0.5, 1.0)]);
        assert_eq!(
            serde_json::to_string(&s).unwrap_or_default(),
            r#"{"Summary":{"count":2,"sum":3.0,"quantiles":[[0.5,1.0]]}}"#
        );
    }
}
