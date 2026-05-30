use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use parqtel_core::BlockIndex;
use tokio::sync::RwLock;

/// Internal metrics for self-observability.
pub struct ServerMetrics {
    pub ingested_points: AtomicU64,
    pub ingest_errors: AtomicU64,
    pub batches_received: AtomicU64,
    pub queries_executed: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_duration_ms: Mutex<Histogram>,
}

impl Default for ServerMetrics {
    fn default() -> Self {
        Self {
            ingested_points: AtomicU64::new(0),
            ingest_errors: AtomicU64::new(0),
            batches_received: AtomicU64::new(0),
            queries_executed: AtomicU64::new(0),
            query_errors: AtomicU64::new(0),
            query_duration_ms: Mutex::new(Histogram::new(vec![
                1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0,
            ])),
        }
    }
}

pub struct Histogram {
    buckets: Vec<f64>,
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    pub fn new(buckets: Vec<f64>) -> Self {
        let n = buckets.len();
        Self {
            buckets,
            counts: vec![0; n + 1],
            sum: 0.0,
            count: 0,
        }
    }

    pub fn render(&self, name: &str) -> String {
        let mut out = String::new();
        let mut cumulative = 0;
        for (i, &b) in self.buckets.iter().enumerate() {
            cumulative += self.counts[i];
            out.push_str(&format!("{}_bucket{{le=\"{}\"}} {}\n", name, b, cumulative));
        }
        cumulative += self.counts.last().unwrap_or(&0);
        out.push_str(&format!("{}_bucket{{le=\"+Inf\"}} {}\n", name, cumulative));
        out.push_str(&format!("{}_sum {}\n", name, self.sum));
        out.push_str(&format!("{}_count {}\n", name, self.count));
        out
    }
}

impl ServerMetrics {
    pub async fn render(&self, index: &Arc<RwLock<BlockIndex>>) -> String {
        let mut out = String::new();
        
        let idx = index.read().await;
        
        out.push_str("# HELP parqtel_ingested_points_total Total data points ingested\n");
        out.push_str("# TYPE parqtel_ingested_points_total counter\n");
        out.push_str(&format!("parqtel_ingested_points_total {}\n", self.ingested_points.load(Ordering::Relaxed)));

        out.push_str("# HELP parqtel_ingest_errors_total Total ingestion errors\n");
        out.push_str("# TYPE parqtel_ingest_errors_total counter\n");
        out.push_str(&format!("parqtel_ingest_errors_total {}\n", self.ingest_errors.load(Ordering::Relaxed)));

        out.push_str("# HELP parqtel_batches_received_total Total OTLP batches received\n");
        out.push_str("# TYPE parqtel_batches_received_total counter\n");
        out.push_str(&format!("parqtel_batches_received_total {}\n", self.batches_received.load(Ordering::Relaxed)));

        out.push_str("# HELP parqtel_queries_executed_total Total queries executed\n");
        out.push_str("# TYPE parqtel_queries_executed_total counter\n");
        out.push_str(&format!("parqtel_queries_executed_total {}\n", self.queries_executed.load(Ordering::Relaxed)));

        out.push_str("# HELP parqtel_query_errors_total Total query errors\n");
        out.push_str("# TYPE parqtel_query_errors_total counter\n");
        out.push_str(&format!("parqtel_query_errors_total {}\n", self.query_errors.load(Ordering::Relaxed)));

        out.push_str("# HELP parqtel_query_duration_ms Query duration in milliseconds\n");
        out.push_str("# TYPE parqtel_query_duration_ms histogram\n");
        if let Ok(guard) = self.query_duration_ms.lock() {
            out.push_str(&guard.render("parqtel_query_duration_ms"));
        }

        out.push_str("# HELP parqtel_storage_blocks Total number of data blocks on disk\n");
        out.push_str("# TYPE parqtel_storage_blocks gauge\n");
        out.push_str(&format!("parqtel_storage_blocks {}\n", idx.total_blocks()));

        out.push_str("# HELP parqtel_storage_bytes Total size of all data blocks in bytes\n");
        out.push_str("# TYPE parqtel_storage_bytes gauge\n");
        out.push_str(&format!("parqtel_storage_bytes {}\n", idx.total_bytes()));

        out.push_str("# HELP parqtel_storage_rows Total number of data points stored\n");
        out.push_str("# TYPE parqtel_storage_rows gauge\n");
        out.push_str(&format!("parqtel_storage_rows {}\n", idx.total_rows()));

        out.push_str("# HELP parqtel_process_rss_bytes Process RSS memory in bytes\n");
        out.push_str("# TYPE parqtel_process_rss_bytes gauge\n");
        out.push_str(&format!("parqtel_process_rss_bytes {}\n", get_rss()));

        out
    }
}

fn get_rss() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
            for line in status.lines() {
                if line.starts_with("VmRSS:") {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 2 {
                        if let Ok(kb) = parts[1].parse::<u64>() {
                            return kb * 1024;
                        }
                    }
                }
            }
        }
    }
    0
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use std::sync::atomic::Ordering;
    use tempfile::tempdir;

    #[test]
    fn test_histogram_new() {
        let h = Histogram::new(vec![1.0, 5.0, 10.0]);
        let rendered = h.render("test");
        assert!(rendered.contains("test_bucket{le=\"1\"} 0"));
        assert!(rendered.contains("test_bucket{le=\"+Inf\"} 0"));
        assert!(rendered.contains("test_sum 0"));
        assert!(rendered.contains("test_count 0"));
    }

    #[test]
    fn test_server_metrics_default() {
        let m = ServerMetrics::default();
        assert_eq!(m.ingested_points.load(Ordering::Relaxed), 0);
        assert_eq!(m.queries_executed.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_render_metrics() {
        let dir = tempdir().unwrap();
        let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
        let metrics = ServerMetrics::default();
        metrics.ingested_points.store(100, Ordering::Relaxed);
        metrics.batches_received.store(5, Ordering::Relaxed);

        let output = metrics.render(&index).await;
        assert!(output.contains("parqtel_ingested_points_total 100"));
        assert!(output.contains("parqtel_batches_received_total 5"));
        assert!(output.contains("parqtel_storage_blocks 0"));
        assert!(output.contains("parqtel_process_rss_bytes"));
    }
}
