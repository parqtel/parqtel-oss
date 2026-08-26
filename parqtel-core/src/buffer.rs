//! In-memory queryable buffer for stream-queryable data before Parquet flush.
//!
//! Uses a HashMap indexed by metric name for O(1) lookup instead of O(n) scan.

use crate::models::logs::LogRecord;
use crate::models::metrics::DataPoint;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Thread-safe in-memory buffer indexed by metric name for fast lookups.
#[derive(Clone)]
pub struct MemoryBuffer {
    metrics: Arc<RwLock<HashMap<String, Vec<DataPoint>>>>,
    logs: Arc<RwLock<Vec<LogRecord>>>,
}

impl MemoryBuffer {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(RwLock::new(HashMap::with_capacity(64))),
            logs: Arc::new(RwLock::new(Vec::with_capacity(5_000))),
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

    /// Buffer stats for monitoring.
    pub async fn stats(&self) -> (usize, usize) {
        let m: usize = self.metrics.read().await.values().map(|v| v.len()).sum();
        let l = self.logs.read().await.len();
        (m, l)
    }
}

impl Default for MemoryBuffer {
    fn default() -> Self {
        Self::new()
    }
}
