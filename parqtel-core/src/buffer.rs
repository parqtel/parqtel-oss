//! In-memory queryable buffer for stream-queryable data before Parquet flush.
//!
//! Uses a HashMap indexed by metric name for O(1) lookup instead of O(n) scan.

use crate::models::logs::LogRecord;
use crate::models::metrics::DataPoint;
use crate::models::traces::Span;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe in-memory buffer indexed by metric name for fast lookups.
#[derive(Clone)]
pub struct MemoryBuffer {
    metrics: Arc<RwLock<HashMap<String, Vec<DataPoint>>>>,
    logs: Arc<RwLock<Vec<LogRecord>>>,
    spans: Arc<RwLock<Vec<Span>>>,
}

impl MemoryBuffer {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            logs: Arc::new(RwLock::new(Vec::with_capacity(5_000))),
            spans: Arc::new(RwLock::new(Vec::with_capacity(5_000))),
        }
    }

    /// Push metric data points into the indexed buffer.
    pub async fn push_metrics(&self, name: &str, data_points: &[DataPoint]) {
        let mut buf = self.metrics.write().await;
        buf.entry(name.to_string())
            .or_insert_with(|| Vec::with_capacity(128))
            .extend_from_slice(data_points);
    }

    /// Push log records into the buffer.
    pub async fn push_logs(&self, logs: &[LogRecord]) {
        self.logs.write().await.extend_from_slice(logs);
    }

    /// Push spans into the buffer (queryable immediately, pre-flush).
    pub async fn push_spans(&self, spans: &[Span]) {
        self.spans.write().await.extend_from_slice(spans);
    }

    /// Query spans from the buffer matching the given time range.
    /// A span matches if its interval [start, end] overlaps the query window.
    pub async fn scan_spans(&self, start_ns: i64, end_ns: i64) -> Vec<Span> {
        let buf = self.spans.read().await;
        buf.iter()
            .filter(|s| s.start_time_ns <= end_ns && s.end_time_ns >= start_ns)
            .cloned()
            .collect()
    }

    /// O(1) lookup by metric name + time range filter.
    pub async fn scan_metrics(
        &self,
        metric_name: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Vec<DataPoint> {
        let buf = self.metrics.read().await;
        match buf.get(metric_name) {
            Some(points) => points
                .iter()
                .filter(|dp| dp.timestamp_ns >= start_ns && dp.timestamp_ns <= end_ns)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// Query logs from the buffer matching the given time range.
    pub async fn scan_logs(&self, start_ns: i64, end_ns: i64) -> Vec<LogRecord> {
        let buf = self.logs.read().await;
        buf.iter()
            .filter(|l| l.timestamp_ns >= start_ns && l.timestamp_ns <= end_ns)
            .cloned()
            .collect()
    }

    /// O(1) — get metric names directly from HashMap keys.
    pub async fn metric_names(&self) -> Vec<String> {
        let buf = self.metrics.read().await;
        buf.keys().cloned().collect()
    }

    /// Get all unique label names from buffered metrics.
    pub async fn label_names(&self) -> Vec<String> {
        let buf = self.metrics.read().await;
        let mut labels: Vec<String> = buf
            .values()
            .flat_map(|points| points.iter().flat_map(|dp| dp.labels.keys().cloned()))
            .collect();
        labels.sort_unstable();
        labels.dedup();
        labels
    }

    /// Drain all metrics (called after flush).
    pub async fn drain_metrics(&self) -> Vec<(String, Vec<DataPoint>)> {
        let mut buf = self.metrics.write().await;
        std::mem::take(&mut *buf).into_iter().collect()
    }

    /// Drain all logs (called after flush).
    pub async fn drain_logs(&self) -> Vec<LogRecord> {
        let mut buf = self.logs.write().await;
        std::mem::take(&mut *buf)
    }

    /// Drain all spans (called after trace flush).
    pub async fn drain_spans(&self) -> Vec<Span> {
        let mut buf = self.spans.write().await;
        std::mem::take(&mut *buf)
    }

    /// Buffer stats for monitoring: (metrics, logs, spans).
    pub async fn stats(&self) -> (usize, usize, usize) {
        let m: usize = self.metrics.read().await.values().map(|v| v.len()).sum();
        let l = self.logs.read().await.len();
        let s = self.spans.read().await.len();
        (m, l, s)
    }
}

impl Default for MemoryBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::models::traces::SpanStatus;

    fn test_span(id: u8, start: i64, end: i64) -> Span {
        Span {
            trace_id: [id; 16],
            span_id: [id; 8],
            trace_state: String::new(),
            parent_span_id: [0; 8],
            name: format!("op-{id}"),
            kind: 2,
            start_time_ns: start,
            end_time_ns: end,
            attributes: Default::default(),
            events: Vec::new(),
            links: Vec::new(),
            status: SpanStatus {
                code: 0,
                message: String::new(),
            },
            flags: 0,
        }
    }

    #[tokio::test]
    async fn spans_queryable_immediately_after_push() {
        let buf = MemoryBuffer::new();
        buf.push_spans(&[test_span(1, 1_000, 2_000), test_span(2, 10_000, 12_000)])
            .await;
        let hits = buf.scan_spans(0, 5_000).await;
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "op-1");
    }

    #[tokio::test]
    async fn span_overlap_matches_interval_not_point() {
        // A span [100..200] overlaps a query [150..1000] even though its start
        // precedes the window — interval overlap, not point containment.
        let buf = MemoryBuffer::new();
        buf.push_spans(&[test_span(1, 100, 200)]).await;
        assert_eq!(buf.scan_spans(150, 1_000).await.len(), 1);
        assert_eq!(buf.scan_spans(201, 1_000).await.len(), 0);
    }

    #[tokio::test]
    async fn drain_spans_empties_buffer() {
        let buf = MemoryBuffer::new();
        buf.push_spans(&[test_span(1, 1_000, 2_000)]).await;
        let drained = buf.drain_spans().await;
        assert_eq!(drained.len(), 1);
        assert_eq!(buf.scan_spans(0, i64::MAX).await.len(), 0);
    }

    #[tokio::test]
    async fn stats_counts_all_signals() {
        let buf = MemoryBuffer::new();
        buf.push_spans(&[test_span(1, 1, 2)]).await;
        let (m, l, s) = buf.stats().await;
        assert_eq!((m, l, s), (0, 0, 1));
    }
}
