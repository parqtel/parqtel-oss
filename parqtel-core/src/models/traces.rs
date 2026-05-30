use serde::{Deserialize, Serialize};
use crate::models::labels::LabelSet;

/// Represents a single span in a distributed trace, compatible with the OTLP span data model.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Span {
    /// Unique identifier for the trace (16 bytes).
    pub trace_id: [u8; 16],
    /// Unique identifier for the span within a trace (8 bytes).
    pub span_id: [u8; 8],
    /// Trace state for distributed tracing context.
    pub trace_state: String,
    /// Span name.
    pub name: String,
    /// Span kind (e.g., internal, server, client, producer, consumer).
    pub kind: i32,
    /// Start time in nanoseconds since UNIX epoch.
    pub start_time_ns: i64,
    /// End time in nanoseconds since UNIX epoch.
    pub end_time_ns: i64,
    /// Span attributes.
    pub attributes: LabelSet,
    /// Events within the span.
    pub events: Vec<SpanEvent>,
    /// Links to other spans.
    pub links: Vec<SpanLink>,
    /// Status of the span.
    pub status: SpanStatus,
    /// Parent span ID (8 bytes, zero if root span).
    pub parent_span_id: [u8; 8],
    /// Flags for the span.
    pub flags: u32,
}

/// Represents an event within a span.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanEvent {
    /// Event time in nanoseconds since UNIX epoch.
    pub time_ns: i64,
    /// Event name.
    pub name: String,
    /// Event attributes.
    pub attributes: LabelSet,
}

/// Represents a link to another span.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanLink {
    /// Trace ID of the linked span (16 bytes).
    pub trace_id: [u8; 16],
    /// Span ID of the linked span (8 bytes).
    pub span_id: [u8; 8],
    /// Link attributes.
    pub attributes: LabelSet,
}

/// Represents the status of a span.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanStatus {
    /// Status code (0=UNSET, 1=OK, 2=ERROR).
    pub code: i32,
    /// Status message.
    pub message: String,
}

impl Span {
    /// Creates a new span from OTLP proto data.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        trace_id: [u8; 16],
        span_id: [u8; 8],
        trace_state: String,
        name: String,
        kind: i32,
        start_time_ns: i64,
        end_time_ns: i64,
        attributes: LabelSet,
        events: Vec<SpanEvent>,
        links: Vec<SpanLink>,
        status: SpanStatus,
        parent_span_id: [u8; 8],
        flags: u32,
    ) -> Self {
        Self {
            trace_id,
            span_id,
            trace_state,
            name,
            kind,
            start_time_ns,
            end_time_ns,
            attributes,
            events,
            links,
            status,
            parent_span_id,
            flags,
        }
    }

    /// Returns the duration of the span in nanoseconds.
    pub fn duration_ns(&self) -> i64 {
        self.end_time_ns - self.start_time_ns
    }

    /// Returns the duration of the span in milliseconds.
    pub fn duration_ms(&self) -> f64 {
        self.duration_ns() as f64 / 1_000_000.0
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    #[test]
    fn test_span_creation() {
        let span = Span::new(
            [0; 16], [0; 8], "tracestate".into(), "test-span".into(),
            1, 1000, 2000, LabelSet::default(), vec![], vec![],
            SpanStatus { code: 0, message: "".into() },
            [0; 8], 0,
        );
        assert_eq!(span.name, "test-span");
        assert_eq!(span.duration_ns(), 1000);
    }

    #[test]
    fn test_span_duration_ms() {
        let span = Span::new(
            [0; 16], [0; 8], "tracestate".into(), "test-span".into(),
            1, 1_000_000_000, 2_500_000_000, LabelSet::default(), vec![], vec![],
            SpanStatus { code: 0, message: "".into() },
            [0; 8], 0,
        );
        assert_eq!(span.duration_ms(), 1500.0);
    }

    #[test]
    fn test_span_serialization() {
        let span = Span::new(
            [0; 16], [0; 8], "tracestate".into(), "test-span".into(),
            1, 1000, 2000, LabelSet::default(), vec![], vec![],
            SpanStatus { code: 0, message: "".into() },
            [0; 8], 0,
        );
        let json = serde_json::to_string(&span).unwrap();
        let decoded: Span = serde_json::from_str(&json).unwrap();
        assert_eq!(span, decoded);
    }
}
