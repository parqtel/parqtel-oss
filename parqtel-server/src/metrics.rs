use parqtel_core::BlockIndex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

/// Internal metrics for self-observability.
pub struct ServerMetrics {
    pub ingested_points: AtomicU64,
    pub ingest_errors: AtomicU64,
    pub batches_received: AtomicU64,
    pub queries_executed: AtomicU64,
    pub query_errors: AtomicU64,
    pub query_duration_ms: Mutex<Histogram>,
    /// Live per-signal ingestion rates (metrics samples / log records /
    /// spans) — 1-second buckets over a 15-minute window.
    pub rates: RatesSnapshot,
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
            rates: RatesSnapshot::default(),
        }
    }
}

/// Number of 1-second rate buckets kept per signal (15 minutes).
pub const RATE_WINDOW_BUCKETS: usize = 900;

/// Rolling per-signal ingest-rate wheel: one slot per 1-second epoch
/// bucket. Each slot is a single packed atomic — high 32 bits hold the
/// epoch second, low 32 bits the count — so a slot overwrites itself when
/// its second comes around again and **no ticker task or background
/// rotation is needed**. Ingest threads only pay one CAS; readers get an
/// exact trailing-window sum with no locks.
struct RateWheel {
    slots: Vec<AtomicU64>,
}

/// Pack (epoch_sec, count) into one word. Count saturates at u32::MAX
/// items/sec per signal — far beyond any real ingest rate.
#[inline]
fn pack(sec: u64, count: u64) -> u64 {
    (sec << 32) | (count.min(u32::MAX as u64))
}

#[inline]
fn slot_index(sec: u64) -> usize {
    (sec % RATE_WINDOW_BUCKETS as u64) as usize
}

impl Default for RateWheel {
    fn default() -> Self {
        Self::new()
    }
}

impl RateWheel {
    fn new() -> Self {
        Self {
            slots: (0..RATE_WINDOW_BUCKETS)
                .map(|_| AtomicU64::new(0))
                .collect(),
        }
    }

    /// Add `n` items ingested during epoch second `sec`.
    fn record(&self, n: u64, sec: u64) {
        if n == 0 {
            return;
        }
        let slot = &self.slots[slot_index(sec)];
        let mut cur = slot.load(Ordering::Relaxed);
        loop {
            let (stamp, count) = (cur >> 32, cur & 0xFFFF_FFFF);
            let next = if stamp == sec {
                // Same second — bump the count in place.
                pack(sec, count.saturating_add(n))
            } else {
                // New second claims the slot (any old second's data is by
                // construction ≥ RATE_WINDOW_SECS old and already expired).
                pack(sec, n)
            };
            match slot.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Sum counts for the inclusive second range [start, end] (end <
    /// start → 0). Slots whose stamp falls outside the range are skipped,
    /// which is how old data expires.
    fn sum_range(&self, start: u64, end: u64) -> u64 {
        if end < start {
            return 0;
        }
        // Span can be up to the window size; walk by epoch second so each
        // slot is visited at most once.
        let mut sum = 0u64;
        let mut t = start;
        while t <= end {
            let v = self.slots[slot_index(t)].load(Ordering::Relaxed);
            if v >> 32 == t {
                sum += v & 0xFFFF_FFFF;
            }
            t += 1;
        }
        sum
    }

    /// Seconds since the newest non-zero bucket, searching back at most
    /// the whole window. 0 = ingesting right now (or in this second).
    fn gap_secs(&self, now: u64) -> u64 {
        for back in 0..=RATE_WINDOW_BUCKETS as u64 {
            let t = now.saturating_sub(back);
            let v = self.slots[slot_index(t)].load(Ordering::Relaxed);
            if v >> 32 == t && v & 0xFFFF_FFFF > 0 {
                return back;
            }
            if t == 0 {
                break;
            }
        }
        // Nothing in the entire window: report full-window silence. Use
        // RATE_WINDOW_BUCKETS so "gap == window" reads as "no data for
        // 15m", never an unbounded value.
        RATE_WINDOW_BUCKETS as u64
    }
}

/// Live ingestion rates for one signal, as rendered for API/UI.
#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct SignalRates {
    /// Items ingested in the last complete 1s bucket.
    pub current_per_sec: u64,
    /// Items/sec averaged over the trailing 60 complete seconds.
    pub avg_60s: f64,
    /// Items/sec averaged over the trailing 5 minutes.
    pub avg_5m: f64,
    /// Items/sec averaged over the trailing 15 minutes.
    pub avg_15m: f64,
    /// Wire payload bytes/sec averaged over the trailing 60 complete
    /// seconds. Shows protocol-level ingest pressure (big batches
    /// vs many small ones).
    pub bytes_per_sec: f64,
    /// Seconds since the last non-zero ingest bucket (capped at the
    /// 900s window; 0 = active right now).
    pub gap_secs: u64,
    /// Total items recorded since process start.
    pub total: u64,
}

/// Per-signal rate wheels + totals, shared between ingest handlers, the
/// gRPC service, and the API handlers. Lock-free; hot path is one CAS.
pub struct RatesSnapshot {
    metrics: RateWheel,
    logs: RateWheel,
    spans: RateWheel,
    bytes_metrics: RateWheel,
    bytes_logs: RateWheel,
    bytes_spans: RateWheel,
    total_metrics: AtomicU64,
    total_logs: AtomicU64,
    total_spans: AtomicU64,
    total_bytes_metrics: AtomicU64,
    total_bytes_logs: AtomicU64,
    total_bytes_spans: AtomicU64,
}

impl Default for RatesSnapshot {
    fn default() -> Self {
        Self {
            metrics: RateWheel::new(),
            logs: RateWheel::new(),
            spans: RateWheel::new(),
            bytes_metrics: RateWheel::new(),
            bytes_logs: RateWheel::new(),
            bytes_spans: RateWheel::new(),
            total_metrics: AtomicU64::new(0),
            total_logs: AtomicU64::new(0),
            total_spans: AtomicU64::new(0),
            total_bytes_metrics: AtomicU64::new(0),
            total_bytes_logs: AtomicU64::new(0),
            total_bytes_spans: AtomicU64::new(0),
        }
    }
}

impl RatesSnapshot {
    /// Record `n` metric samples ingested now from a wire payload of
    /// `bytes` bytes.
    pub fn record_metrics(&self, n: u64, bytes: u64) {
        self.total_metrics.fetch_add(n, Ordering::Relaxed);
        self.total_bytes_metrics.fetch_add(bytes, Ordering::Relaxed);
        let now = now_secs();
        self.metrics.record(n, now);
        self.bytes_metrics.record(bytes, now);
    }

    /// Record `n` log records ingested now from a wire payload of `bytes`
    /// bytes.
    pub fn record_logs(&self, n: u64, bytes: u64) {
        self.total_logs.fetch_add(n, Ordering::Relaxed);
        self.total_bytes_logs.fetch_add(bytes, Ordering::Relaxed);
        let now = now_secs();
        self.logs.record(n, now);
        self.bytes_logs.record(bytes, now);
    }

    /// Record `n` trace spans ingested now from a wire payload of `bytes`
    /// bytes.
    pub fn record_spans(&self, n: u64, bytes: u64) {
        self.total_spans.fetch_add(n, Ordering::Relaxed);
        self.total_bytes_spans.fetch_add(bytes, Ordering::Relaxed);
        let now = now_secs();
        self.spans.record(n, now);
        self.bytes_spans.record(bytes, now);
    }

    /// Render the live rates for all three signals, as of `now`.
    pub fn snapshot(&self, now: u64) -> [SignalRates; 3] {
        [
            self.one(&self.metrics, &self.bytes_metrics, &self.total_metrics, now),
            self.one(&self.logs, &self.bytes_logs, &self.total_logs, now),
            self.one(&self.spans, &self.bytes_spans, &self.total_spans, now),
        ]
    }

    /// Per-second counts for the `secs` most recent seconds (including the
    /// current, still-filling one), oldest first (zeros where nothing
    /// arrived). Clamped to the window size. Powers the live-rate
    /// sparklines in the UI overview.
    pub fn history(&self, secs: usize, now: u64) -> [Vec<u64>; 3] {
        let secs = secs.clamp(1, RATE_WINDOW_BUCKETS) as u64;
        let end = now;
        let start = end.saturating_sub(secs - 1);
        let series = |w: &RateWheel| -> Vec<u64> {
            (start..=end)
                .map(|t| {
                    let v = w.slots[slot_index(t)].load(Ordering::Relaxed);
                    if v >> 32 == t {
                        v & 0xFFFF_FFFF
                    } else {
                        0
                    }
                })
                .collect()
        };
        [
            series(&self.metrics),
            series(&self.logs),
            series(&self.spans),
        ]
    }

    fn one(
        &self,
        wheel: &RateWheel,
        bytes_wheel: &RateWheel,
        total: &AtomicU64,
        now: u64,
    ) -> SignalRates {
        let last_complete = now.saturating_sub(1);
        let avg = |secs: u64| -> f64 {
            // Average over complete seconds only; early in the process's
            // life the window is clamped to what exists.
            let start = last_complete.saturating_sub(secs - 1);
            let span = last_complete.saturating_sub(start) + 1;
            wheel.sum_range(start, last_complete) as f64 / span as f64
        };
        let bytes_start = last_complete.saturating_sub(59);
        let bytes_span = last_complete.saturating_sub(bytes_start) + 1;
        SignalRates {
            current_per_sec: wheel.sum_range(last_complete, last_complete),
            avg_60s: avg(60),
            avg_5m: avg(300),
            avg_15m: avg(900),
            bytes_per_sec: bytes_wheel.sum_range(bytes_start, last_complete) as f64
                / bytes_span as f64,
            gap_secs: wheel.gap_secs(now),
            total: total.load(Ordering::Relaxed),
        }
    }
}

pub fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
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

    /// Records one observation into the histogram.
    pub fn record(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;
        for (i, &bound) in self.buckets.iter().enumerate() {
            if value <= bound {
                self.counts[i] += 1;
                return;
            }
        }
        // Above every bucket bound -> the +Inf overflow slot.
        if let Some(last) = self.counts.last_mut() {
            *last += 1;
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
        out.push_str(&format!(
            "parqtel_ingested_points_total {}\n",
            self.ingested_points.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP parqtel_ingest_errors_total Total ingestion errors\n");
        out.push_str("# TYPE parqtel_ingest_errors_total counter\n");
        out.push_str(&format!(
            "parqtel_ingest_errors_total {}\n",
            self.ingest_errors.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP parqtel_batches_received_total Total OTLP batches received\n");
        out.push_str("# TYPE parqtel_batches_received_total counter\n");
        out.push_str(&format!(
            "parqtel_batches_received_total {}\n",
            self.batches_received.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP parqtel_queries_executed_total Total queries executed\n");
        out.push_str("# TYPE parqtel_queries_executed_total counter\n");
        out.push_str(&format!(
            "parqtel_queries_executed_total {}\n",
            self.queries_executed.load(Ordering::Relaxed)
        ));

        out.push_str("# HELP parqtel_query_errors_total Total query errors\n");
        out.push_str("# TYPE parqtel_query_errors_total counter\n");
        out.push_str(&format!(
            "parqtel_query_errors_total {}\n",
            self.query_errors.load(Ordering::Relaxed)
        ));

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

        let now = now_secs();
        let [metrics, logs, spans] = self.rates.snapshot(now);
        out.push_str("# HELP parqtel_ingest_rate_per_sec Items ingested per second (last complete 1s bucket)\n");
        out.push_str("# TYPE parqtel_ingest_rate_per_sec gauge\n");
        out.push_str(&format!(
            "parqtel_ingest_rate_per_sec{{signal=\"metrics\"}} {}\n",
            metrics.current_per_sec
        ));
        out.push_str(&format!(
            "parqtel_ingest_rate_per_sec{{signal=\"logs\"}} {}\n",
            logs.current_per_sec
        ));
        out.push_str(&format!(
            "parqtel_ingest_rate_per_sec{{signal=\"traces\"}} {}\n",
            spans.current_per_sec
        ));

        out.push_str("# HELP parqtel_ingest_rate_avg_60s Items/sec averaged over the last 60s\n");
        out.push_str("# TYPE parqtel_ingest_rate_avg_60s gauge\n");
        for (name, r) in [("metrics", metrics), ("logs", logs), ("traces", spans)] {
            out.push_str(&format!(
                "parqtel_ingest_rate_avg_60s{{signal=\"{name}\"}} {}\n",
                r.avg_60s
            ));
        }

        out.push_str(
            "# HELP parqtel_ingest_gap_secs Seconds since the last ingested item (capped at 900)\n",
        );
        out.push_str("# TYPE parqtel_ingest_gap_secs gauge\n");
        for (name, r) in [("metrics", metrics), ("logs", logs), ("traces", spans)] {
            out.push_str(&format!(
                "parqtel_ingest_gap_secs{{signal=\"{name}\"}} {}\n",
                r.gap_secs
            ));
        }

        out.push_str(
            "# HELP parqtel_ingest_bytes_per_sec Wire payload bytes/sec ingested over the last 60s\n",
        );
        out.push_str("# TYPE parqtel_ingest_bytes_per_sec gauge\n");
        for (name, r) in [("metrics", metrics), ("logs", logs), ("traces", spans)] {
            out.push_str(&format!(
                "parqtel_ingest_bytes_per_sec{{signal=\"{name}\"}} {}\n",
                r.bytes_per_sec
            ));
        }

        out.push_str(
            "# HELP parqtel_ingested_items_total Items ingested per signal since process start\n",
        );
        out.push_str("# TYPE parqtel_ingested_items_total counter\n");
        for (name, r) in [("metrics", metrics), ("logs", logs), ("traces", spans)] {
            out.push_str(&format!(
                "parqtel_ingested_items_total{{signal=\"{name}\"}} {}\n",
                r.total
            ));
        }

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
        assert!(output.contains("parqtel_ingest_rate_per_sec{signal=\"metrics\"}"));
        assert!(output.contains("parqtel_ingest_gap_secs{signal=\"traces\"}"));
        assert!(output.contains("parqtel_ingested_items_total{signal=\"logs\"}"));
    }

    #[test]
    fn test_rate_wheel_record_and_sum() {
        let w = RateWheel::new();
        // Empty wheel: everything zero, gap = full window.
        assert_eq!(w.sum_range(100, 200), 0);
        assert_eq!(w.gap_secs(1000), RATE_WINDOW_BUCKETS as u64);

        // Record 3 items in second 1000, 4 items in second 1001.
        w.record(3, 1000);
        w.record(1, 1001);
        w.record(3, 1001);
        assert_eq!(w.sum_range(1000, 1001), 7);
        assert_eq!(w.sum_range(1001, 1001), 4);
        assert_eq!(w.gap_secs(1001), 0);
        assert_eq!(w.gap_secs(1005), 4);

        // A second ≥ RATE_WINDOW_BUCKETS later reclaims the same slot: the
        // old second's data must expire (stamp mismatch → skipped). 1900
        // maps to the same slot as 1000, 1901 to the same as 1001.
        w.record(9, 1900);
        assert_eq!(w.sum_range(1000, 1000), 0);
        assert_eq!(w.sum_range(1001, 1001), 4); // not yet reclaimed
        w.record(1, 1901);
        assert_eq!(w.sum_range(1001, 1001), 0);
        assert_eq!(w.sum_range(1900, 1901), 10);
    }

    #[test]
    fn test_rate_wheel_concurrent_record() {
        // Hammer the same second from many threads: the packed CAS loop
        // must not lose counts.
        let w = Arc::new(RateWheel::new());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let w = w.clone();
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        w.record(1, 42);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(w.sum_range(42, 42), 8000);
    }

    #[test]
    fn test_rates_snapshot_signals() {
        let r = RatesSnapshot::default();
        r.record_metrics(10, 1000);
        r.record_logs(5, 500);
        r.record_spans(2, 200);
        let now = now_secs();
        let [m, l, s] = r.snapshot(now);
        assert_eq!(m.total, 10);
        assert_eq!(l.total, 5);
        assert_eq!(s.total, 2);
        assert_eq!(m.gap_secs, 0);
        assert!(m.avg_60s >= 0.0);
        // Byte wheels track alongside item wheels; the current-second
        // bytes may not be in avg yet, but the byte wheel holds the data.
        assert!(m.bytes_per_sec >= 0.0);
        assert!(l.bytes_per_sec >= 0.0);
        assert!(s.bytes_per_sec >= 0.0);
        // current_per_sec reads the last COMPLETE second; recorded data is
        // in "now" so it may be 0 until the second rolls over — but totals
        // are always exact.
        assert!(m.total >= m.current_per_sec);
    }
}
