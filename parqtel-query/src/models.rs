use parqtel_core::LabelSet;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// A single data point in a query result.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Sample {
    /// Nanosecond timestamp.
    pub timestamp_ns: i64,
    /// Floating point value.
    pub value: f64,
}

impl Sample {
    /// Returns the timestamp in floating point Unix seconds.
    pub fn timestamp_seconds(&self) -> f64 {
        self.timestamp_ns as f64 / 1_000_000_000.0
    }
}

/// A time series containing labels and a list of samples.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TimeSeries {
    /// Set of labels that uniquely identify this series.
    pub labels: LabelSet,
    /// Samples sorted by timestamp.
    pub samples: Vec<Sample>,
}

/// The complete result of a query execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueryResult {
    /// List of series matching the query.
    pub series: Vec<TimeSeries>,
    /// Time taken to execute the query.
    pub execution_time: Duration,
    /// Number of raw data points scanned before filtering.
    pub points_scanned: u64,
    /// Total number of series matching the query (before truncation).
    pub total_series_count: usize,
    /// Bucketed ingestion volume for the query range (60 buckets).
    pub volume_summary: Vec<u64>,
}

/// The result of a log query execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LogQueryResult {
    /// List of log records matching the query.
    pub logs: Vec<parqtel_core::LogRecord>,
    /// Time taken to execute the query.
    pub execution_time: Duration,
    /// Total number of logs matching the query (before truncation).
    pub total_logs_count: usize,
    /// Bucketed ingestion volume for the query range (60 buckets).
    pub volume_summary: Vec<u64>,
}

/// A single event correlated with an anchor.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CorrelatedEvent {
    pub signal: String,
    pub record: serde_json::Value,
    pub score: f64,
    pub time_delta_ns: i64,
}

/// The result of a correlation query.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CorrelationResult {
    pub correlation_dimension_used: String,
    pub correlated: Vec<CorrelatedEvent>,
    pub execution_time: Duration,
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_sample_timestamp_seconds() {
        let s = Sample {
            timestamp_ns: 1_500_000_000_000_000_000,
            value: 42.0,
        };
        assert_eq!(s.timestamp_seconds(), 1_500_000_000.0);
    }

    #[test]
    fn test_sample_timestamp_seconds_zero() {
        let s = Sample {
            timestamp_ns: 0,
            value: 0.0,
        };
        assert_eq!(s.timestamp_seconds(), 0.0);
    }

    #[test]
    fn test_time_series_serialization() {
        let ts = TimeSeries {
            labels: LabelSet::try_from_iter(vec![("env", "prod")]).unwrap(),
            samples: vec![Sample {
                timestamp_ns: 100,
                value: 1.0,
            }],
        };
        let json = serde_json::to_string(&ts).unwrap();
        let decoded: TimeSeries = serde_json::from_str(&json).unwrap();
        assert_eq!(ts, decoded);
    }

    #[test]
    fn test_query_result_serialization() {
        let qr = QueryResult {
            series: vec![],
            execution_time: Duration::from_millis(50),
            points_scanned: 1000,
            total_series_count: 5,
            volume_summary: vec![1, 2, 3],
        };
        let json = serde_json::to_string(&qr).unwrap();
        let decoded: QueryResult = serde_json::from_str(&json).unwrap();
        assert_eq!(qr, decoded);
    }

    #[test]
    fn test_log_query_result_serialization() {
        let lqr = LogQueryResult {
            logs: vec![],
            execution_time: Duration::from_millis(10),
            total_logs_count: 0,
            volume_summary: vec![],
        };
        let json = serde_json::to_string(&lqr).unwrap();
        let decoded: LogQueryResult = serde_json::from_str(&json).unwrap();
        assert_eq!(lqr, decoded);
    }

    #[test]
    fn test_correlated_event_serialization() {
        let ce = CorrelatedEvent {
            signal: "metric".into(),
            record: serde_json::json!({"name": "cpu"}),
            score: 0.95,
            time_delta_ns: -5000,
        };
        let json = serde_json::to_string(&ce).unwrap();
        let decoded: CorrelatedEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(ce, decoded);
    }

    #[test]
    fn test_correlation_result_serialization() {
        let cr = CorrelationResult {
            correlation_dimension_used: "trace_id".into(),
            correlated: vec![],
            execution_time: Duration::from_millis(5),
        };
        let json = serde_json::to_string(&cr).unwrap();
        let decoded: CorrelationResult = serde_json::from_str(&json).unwrap();
        assert_eq!(cr, decoded);
    }
}
