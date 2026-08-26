use crate::models::labels::LabelSet;
use serde::{Deserialize, Serialize};

/// Represents a single log record, compatible with the OTLP log data model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LogRecord {
    /// Time when the event occurred in nanoseconds since UNIX epoch.
    pub timestamp_ns: i64,
    /// Time when the event was observed by the collection system.
    pub observed_timestamp_ns: i64,
    /// Numerical value of the severity.
    pub severity_number: i32,
    /// The severity text (also known as log level).
    pub severity_text: String,
    /// The log body (raw message).
    pub body: String,
    /// Attributes that describe the specific event occurrence.
    pub attributes: LabelSet,
    /// Attributes that describe the resource that emitted the log.
    pub resource_attributes: LabelSet,
    /// Unique identifier for a trace (16 bytes).
    pub trace_id: [u8; 16],
    /// Unique identifier for a span within a trace (8 bytes).
    pub span_id: [u8; 8],
    /// Flags, including trace flags.
    pub flags: u32,
    /// Name of the instrumentation scope.
    pub scope_name: String,
    /// Version of the instrumentation scope.
    pub scope_version: String,
}

impl LogRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        timestamp_ns: i64,
        observed_timestamp_ns: i64,
        severity_number: i32,
        severity_text: String,
        body: String,
        attributes: LabelSet,
        resource_attributes: LabelSet,
        trace_id: [u8; 16],
        span_id: [u8; 8],
        flags: u32,
        scope_name: String,
        scope_version: String,
    ) -> Self {
        Self {
            timestamp_ns,
            observed_timestamp_ns,
            severity_number,
            severity_text,
            body,
            attributes,
            resource_attributes,
            trace_id,
            span_id,
            flags,
            scope_name,
            scope_version,
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_log_record_creation() {
        let record = LogRecord::new(
            1000,
            1001,
            9,
            "INFO".into(),
            "test message".into(),
            LabelSet::default(),
            LabelSet::default(),
            [0; 16],
            [0; 8],
            0,
            "test".into(),
            "1.0".into(),
        );
        assert_eq!(record.timestamp_ns, 1000);
        assert_eq!(record.body, "test message");
        assert_eq!(record.severity_text, "INFO");
    }

    #[test]
    fn test_log_record_serialization() {
        let record = LogRecord::new(
            1000,
            1001,
            9,
            "INFO".into(),
            "test message".into(),
            LabelSet::default(),
            LabelSet::default(),
            [0; 16],
            [0; 8],
            0,
            "test".into(),
            "1.0".into(),
        );
        let json = serde_json::to_string(&record).unwrap();
        let decoded: LogRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(record, decoded);
    }
}
