//! Cross-crate self-instrumentation primitives.
//!
//! These live in `parqtel-core` rather than in the server crate because the
//! code that needs to measure them is spread across the workspace:
//! `parqtel-ingest` holds the per-signal ingest mutex, `parqtel-core::storage`
//! holds the block index lock, and `parqtel-server` renders the Prometheus
//! text. Putting the counters here keeps the hot paths free of any dependency
//! on the HTTP layer and gives one histogram implementation instead of one
//! per crate.
//!
//! What is measured (and why):
//!
//! * **Ingest lock wait** — every ingest request serializes on one mutex per
//!   signal, and a block flush happens while that mutex is held. Lock wait is
//!   therefore the single best predictor of ingest p99, and until now it was
//!   only derivable from a profiler.
//! * **Flush duration / in-flight** — `parqtel-server`'s 5-second tick records
//!   flush latency, but capacity-triggered flushes inside a request were not
//!   observed at all, so the expensive case was invisible.
//! * **Index lock wait** — `BlockIndex` is read by every query handler and
//!   written by the index task, which also performs synchronous file I/O.
//!
//! All counters are lock-free apart from short critical sections around the
//! histogram vectors, and the recording path allocates nothing.

use crate::models::storage::SignalType;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Number of signals tracked by [`ContentionMetrics`].
///
/// Must match the length of [`SignalType::ALL`]; asserted by a unit test.
const SIGNAL_COUNT: usize = SignalType::ALL.len();

/// Bucket upper bounds (seconds) for the latency histograms. Chosen so the
/// interesting region for both lock waits and flushes — sub-millisecond when
/// healthy, seconds when blocked — is well resolved.
const SECONDS_BUCKETS: [f64; 12] = [
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Fixed-bucket cumulative histogram with a running sum and count.
///
/// Deliberately simple: no allocation on the recording path, and a linear
/// scan over ~12 bucket bounds, which is cheaper than any concurrent
/// structure at these observation rates.
#[derive(Debug, Clone)]
pub struct Histogram {
    /// Upper bounds, ascending.
    buckets: Vec<f64>,
    /// `counts[i]` holds observations `<= buckets[i]`, plus a trailing
    /// `+Inf` overflow slot.
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    /// Builds a histogram with the given ascending bucket upper bounds.
    pub fn new(buckets: Vec<f64>) -> Self {
        let n = buckets.len();
        Self {
            buckets,
            counts: vec![0; n + 1],
            sum: 0.0,
            count: 0,
        }
    }

    /// A histogram with the shared [`SECONDS_BUCKETS`] layout.
    pub fn seconds() -> Self {
        Self::new(SECONDS_BUCKETS.to_vec())
    }

    /// Records one observation.
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

    /// Records one observation expressed as a [`Duration`], in seconds.
    pub fn record_duration(&mut self, elapsed: Duration) {
        self.record(elapsed.as_secs_f64());
    }

    /// Total observations recorded.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Sum of all observations.
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Renders the histogram in Prometheus text format. Bucket counts are
    /// cumulative, as the format requires.
    ///
    /// `series_prefix` carries the label set for the metric family, e.g.
    /// `parqtel_flush_duration_seconds{signal="metrics"}`, so the emitted
    /// bucket/sum/count lines are correctly labelled series rather than a
    /// single unlabelled family.
    pub fn render(&self, series_prefix: &str) -> String {
        let mut out = String::new();
        let mut cumulative = 0;
        for (i, &b) in self.buckets.iter().enumerate() {
            cumulative += self.counts[i];
            out.push_str(&format!(
                "{series_prefix}_bucket{{le=\"{b}\"}} {cumulative}\n"
            ));
        }
        cumulative += self.counts.last().copied().unwrap_or(0);
        out.push_str(&format!(
            "{series_prefix}_bucket{{le=\"+Inf\"}} {cumulative}\n"
        ));
        out.push_str(&format!("{series_prefix}_sum {}\n", self.sum));
        out.push_str(&format!("{series_prefix}_count {}\n", self.count));
        out
    }
}

/// Series prefix for a metric family carrying a `signal` label.
fn series(metric: &str, signal: SignalType) -> String {
    format!("{metric}{{signal=\"{}\"}}", signal.as_str())
}

/// Emits the `# HELP` / `# TYPE` header for a metric family.
///
/// Prometheus declares a family once; the per-signal series follow.
fn render_header(out: &mut String, metric: &str, help: &str, kind: &str) {
    out.push_str(&format!("# HELP {metric} {help}\n"));
    out.push_str(&format!("# TYPE {metric} {kind}\n"));
}

/// Renders a histogram family with one labelled series per signal.
///
/// Signals are always emitted (zero-valued when idle) so that a dashboard
/// query does not have to cope with series appearing and disappearing.
fn render_histogram_family<'h, F>(out: &mut String, metric: &str, help: &str, f: F)
where
    F: Fn(SignalType) -> &'h Histogram,
{
    render_header(out, metric, help, "histogram");
    for signal in SignalType::ALL {
        out.push_str(&f(signal).render(&series(metric, signal)));
    }
}

/// Renders a gauge or counter family with one labelled sample per signal.
fn render_sample_family<F>(out: &mut String, metric: &str, help: &str, kind: &str, f: F)
where
    F: Fn(SignalType) -> String,
{
    render_header(out, metric, help, kind);
    for signal in SignalType::ALL {
        out.push_str(&format!("{} {}\n", series(metric, signal), f(signal)));
    }
}

/// Lock-wait and flush-shape counters for the ingest path and the block index.
///
/// Cheap to share (`Arc<ContentionMetrics>`) and safe to call from any thread
/// or task. A default instance is created by the server and handed to the
/// three ingestion services, the block-index tasks and the `/metrics`
/// renderer.
#[derive(Debug)]
pub struct ContentionMetrics {
    /// Time spent waiting for the per-signal ingest mutex.
    ingest_lock_wait: [Mutex<Histogram>; SIGNAL_COUNT],
    /// Wall time of each block flush that actually wrote rows.
    flush_duration: [Mutex<Histogram>; SIGNAL_COUNT],
    /// Flushes currently encoding/writing for a signal.
    flush_inflight: [AtomicU64; SIGNAL_COUNT],
    /// Rows written by flushes for a signal, since process start.
    flush_rows: [AtomicU64; SIGNAL_COUNT],
    /// Time spent waiting for the block index write lock.
    index_lock_wait: Mutex<Histogram>,
    /// Writer shards the metrics ingest rotator is using, or 0 when unknown.
    /// Zero is the unambiguous "not reported" value, distinct from a
    /// legitimately configured single shard.
    ingest_rotator_shards: AtomicU64,
}

impl Default for ContentionMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentionMetrics {
    /// A fresh set of counters with empty histograms.
    pub fn new() -> Self {
        Self {
            ingest_lock_wait: std::array::from_fn(|_| Mutex::new(Histogram::seconds())),
            flush_duration: std::array::from_fn(|_| Mutex::new(Histogram::seconds())),
            flush_inflight: std::array::from_fn(|_| AtomicU64::new(0)),
            flush_rows: std::array::from_fn(|_| AtomicU64::new(0)),
            index_lock_wait: Mutex::new(Histogram::seconds()),
            ingest_rotator_shards: AtomicU64::new(0),
        }
    }

    /// Records how many writer shards the metrics ingest rotator uses.
    ///
    /// Reported once at startup. Zero means the caller did not report it, so
    /// a dashboard never mistakes "unknown" for "single-writer".
    pub fn set_ingest_rotator_shards(&self, shards: usize) {
        self.ingest_rotator_shards
            .store(shards as u64, Ordering::Relaxed);
    }

    /// Writer shards in use, or 0 when not reported.
    pub fn ingest_rotator_shards(&self) -> u64 {
        self.ingest_rotator_shards.load(Ordering::Relaxed)
    }

    /// Records how long a request waited for the ingest mutex of `signal`.
    ///
    /// Call this *after* the guard is acquired, so the contended mutex is not
    /// held while recording.
    pub fn record_ingest_lock_wait(&self, signal: SignalType, waited: Duration) {
        let mut h = self.ingest_lock_wait[signal.index()]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        h.record_duration(waited);
    }

    /// Records how long a block-index write lock acquisition took.
    pub fn record_index_lock_wait(&self, waited: Duration) {
        let mut h = self
            .index_lock_wait
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        h.record_duration(waited);
    }

    /// Marks the start of a flush and returns a guard that records the
    /// duration and the row count when dropped.
    ///
    /// The guard must be created only for a flush that really writes rows —
    /// an idle no-op flush would otherwise dilute the histogram, the same
    /// rule the SLI layer already applies.
    pub fn flush_started(&self, signal: SignalType) -> FlushGuard<'_> {
        self.flush_inflight[signal.index()].fetch_add(1, Ordering::Relaxed);
        FlushGuard {
            metrics: self,
            signal,
            started: Instant::now(),
        }
    }

    /// Flushes currently in progress for `signal`.
    pub fn flush_inflight(&self, signal: SignalType) -> u64 {
        self.flush_inflight[signal.index()].load(Ordering::Relaxed)
    }

    /// Rows written to Parquet blocks for `signal` since process start.
    pub fn flush_rows(&self, signal: SignalType) -> u64 {
        self.flush_rows[signal.index()].load(Ordering::Relaxed)
    }

    /// Observations recorded for the ingest lock wait of `signal`.
    pub fn ingest_lock_wait_count(&self, signal: SignalType) -> u64 {
        self.ingest_lock_wait[signal.index()]
            .lock()
            .map(|h| h.count())
            .unwrap_or(0)
    }

    /// Observations recorded for flushes of `signal`.
    pub fn flush_duration_count(&self, signal: SignalType) -> u64 {
        self.flush_duration[signal.index()]
            .lock()
            .map(|h| h.count())
            .unwrap_or(0)
    }

    /// Renders every counter in Prometheus text format.
    pub fn render(&self) -> String {
        let mut out = String::new();

        // The histogram locks are held briefly and never across an await, so
        // borrowing them for the render is safe and keeps the output
        // internally consistent.
        let ingest_lock_wait = self.lock_all(&self.ingest_lock_wait);
        render_histogram_family(
            &mut out,
            "parqtel_ingest_lock_wait_seconds",
            "Seconds an ingest request waited for the per-signal ingest mutex",
            |s| &ingest_lock_wait[s.index()],
        );
        drop(ingest_lock_wait);

        let flush_duration = self.lock_all(&self.flush_duration);
        render_histogram_family(
            &mut out,
            "parqtel_flush_duration_seconds",
            "Wall time of each block flush that wrote rows (encode, compress, fsync)",
            |s| &flush_duration[s.index()],
        );
        drop(flush_duration);

        render_sample_family(
            &mut out,
            "parqtel_flush_inflight",
            "Block flushes currently in progress",
            "gauge",
            |s| self.flush_inflight(s).to_string(),
        );

        render_sample_family(
            &mut out,
            "parqtel_flush_rows_total",
            "Rows written to Parquet blocks since process start",
            "counter",
            |s| self.flush_rows(s).to_string(),
        );

        // Process-wide, not per signal: only the metrics rotator is sharded.
        render_header(
            &mut out,
            "parqtel_ingest_rotator_shards",
            "Writer shards in the metrics ingest rotator (0 = not reported)",
            "gauge",
        );
        out.push_str(&format!(
            "parqtel_ingest_rotator_shards {}\n",
            self.ingest_rotator_shards()
        ));

        render_header(
            &mut out,
            "parqtel_index_lock_wait_seconds",
            "Seconds the block-index writer waited for the write lock",
            "histogram",
        );
        if let Ok(h) = self.index_lock_wait.lock() {
            out.push_str(&h.render("parqtel_index_lock_wait_seconds"));
        }

        out
    }

    /// Locks every histogram in `slot` once, up front.
    ///
    /// A poisoned lock (only reachable if a recording thread panicked) yields
    /// an empty histogram rather than losing the whole metric family, so a
    /// panic cannot make `/metrics` fail to render.
    fn lock_all<'a>(
        &'a self,
        slot: &'a [Mutex<Histogram>; SIGNAL_COUNT],
    ) -> Vec<std::sync::MutexGuard<'a, Histogram>> {
        slot.iter()
            .map(|m| m.lock().unwrap_or_else(|e| e.into_inner()))
            .collect()
    }
}

/// RAII guard returned by [`ContentionMetrics::flush_started`].
///
/// Records wall time and row count on drop, so a flush cannot be silently
/// forgotten on an early-return or error path. Because the row count is only
/// known after the writer has written, pass it to [`FlushGuard::count_rows`]
/// before the guard goes out of scope.
#[derive(Debug)]
pub struct FlushGuard<'a> {
    metrics: &'a ContentionMetrics,
    signal: SignalType,
    started: Instant,
}

impl FlushGuard<'_> {
    /// Records how many rows this flush wrote.
    pub fn count_rows(&self, rows: u64) {
        self.metrics.flush_rows[self.signal.index()].fetch_add(rows, Ordering::Relaxed);
    }
}

impl Drop for FlushGuard<'_> {
    fn drop(&mut self) {
        self.metrics.flush_inflight[self.signal.index()].fetch_sub(1, Ordering::Relaxed);
        let mut h = self.metrics.flush_duration[self.signal.index()]
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        h.record_duration(self.started.elapsed());
    }
}

#[cfg(test)]
mod tests;
