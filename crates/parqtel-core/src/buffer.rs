//! In-memory queryable buffer for stream-queryable data before Parquet flush.
//!
//! Uses a HashMap indexed by metric name for O(1) lookup instead of O(n) scan.

use crate::models::labels::LabelSet;
use crate::models::logs::LogRecord;
use crate::models::metrics::DataPoint;
use crate::models::storage::SignalType;
use crate::models::traces::Span;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Number of independent maps the metric buffer is split into.
///
/// Pushes arrive from every ingest shard at once, and queries read concurrently;
/// one lock over one map made all of them queue behind each other.
const METRIC_MAP_SHARDS: usize = 16;

/// Marks the writer shard of points that were buffered without epoch tracking.
const UNTRACKED: u16 = u16::MAX;

/// A run of points that entered the buffer in the same writer epoch.
#[derive(Debug, Clone, Copy)]
struct EpochMark {
    /// Index in the slot's points of the run's first point.
    start: usize,
    shard: u16,
    epoch: u64,
}

/// One metric's buffered points, oldest first, with the epoch runs they form.
#[derive(Debug, Default)]
struct Slot {
    points: Vec<DataPoint>,
    marks: Vec<EpochMark>,
    /// The distinct label-set handles among `points`, found as they were pushed.
    ///
    /// Enumerating label names or values then costs one visit per series rather
    /// than one per point. Handles that are equal but not shared (a source that
    /// does not intern) simply appear more than once, which only repeats work.
    /// It can also name a series whose buffered points have since been dropped;
    /// the metric's flushed blocks index the same names and values.
    series: Vec<LabelSet>,
    series_seen: std::collections::HashSet<usize, ahash::RandomState>,
}

type MetricMaps = Vec<RwLock<HashMap<String, Slot>>>;

/// Thread-safe in-memory buffer indexed by metric name for fast lookups.
///
/// # Epochs
///
/// A block flush takes the points of every writer shard and, some time later,
/// the block becomes durable. In between, new points keep arriving, and they
/// belong to the *next* block. To remove exactly the points a block holds, each
/// writer shard counts epochs: a flush seals the shard's current epoch when it
/// swaps the writer out. Points are buffered together with the epoch of the
/// writer they went into, so [`Self::drop_sealed_metrics`] can remove "everything
/// up to the sealed epoch" and leave the newer points queryable.
#[derive(Clone)]
pub struct MemoryBuffer {
    metrics: Arc<MetricMaps>,
    logs: Arc<RwLock<Vec<LogRecord>>>,
    spans: Arc<RwLock<Vec<Span>>>,
}

fn map_shard(name: &str) -> usize {
    static STATE: std::sync::OnceLock<ahash::RandomState> = std::sync::OnceLock::new();
    let state = STATE.get_or_init(|| ahash::RandomState::with_seeds(1, 2, 3, 4));
    (state.hash_one(name) as usize) % METRIC_MAP_SHARDS
}

impl MemoryBuffer {
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(
                (0..METRIC_MAP_SHARDS)
                    .map(|_| RwLock::new(HashMap::with_capacity(16)))
                    .collect(),
            ),
            logs: Arc::new(RwLock::new(Vec::with_capacity(5_000))),
            spans: Arc::new(RwLock::new(Vec::with_capacity(5_000))),
        }
    }

    /// Push metric data points into the indexed buffer.
    ///
    /// The points are not tied to a writer epoch, so only [`Self::take_metrics`]
    /// and [`Self::drain_offloaded`] remove them.
    pub async fn push_metrics(&self, name: &str, data_points: &[DataPoint]) {
        self.push_metrics_tagged(name, data_points, UNTRACKED, 0)
            .await;
    }

    /// Push points that went into writer `shard` while it was in `epoch`.
    ///
    /// Must be called with the writer shard's lock held, so that buffer order
    /// and writer order agree for every metric.
    pub async fn push_metrics_tagged(
        &self,
        name: &str,
        data_points: &[DataPoint],
        shard: u16,
        epoch: u64,
    ) {
        if data_points.is_empty() {
            return;
        }
        tracing::debug!(metric = %name, points = data_points.len(), "buffered metrics");
        let mut map = self.metrics[map_shard(name)].write().await;
        let slot = map.entry(name.to_string()).or_default();
        if slot.points.capacity() == 0 {
            slot.points.reserve(128);
        }
        let new_run = match slot.marks.last() {
            Some(last) => last.shard != shard || last.epoch != epoch,
            None => true,
        };
        if new_run {
            slot.marks.push(EpochMark {
                start: slot.points.len(),
                shard,
                epoch,
            });
        }
        slot.points.extend_from_slice(data_points);
        for dp in data_points {
            if slot.series_seen.insert(dp.labels.addr()) {
                slot.series.push(dp.labels.clone());
            }
        }
    }

    /// Removes the points of every writer epoch that a durable block holds.
    ///
    /// `sealed[shard]` is the highest epoch of that shard to remove (`0` for
    /// none). Points of later epochs - rows that arrived while the block was
    /// being encoded - stay.
    pub async fn drop_sealed_metrics(&self, sealed: &[u64]) {
        if sealed.iter().all(|e| *e == 0) {
            return;
        }
        let mut freed: Vec<Vec<DataPoint>> = Vec::new();
        for map in self.metrics.iter() {
            let mut map = map.write().await;
            map.retain(|_, slot| {
                let droppable = slot
                    .marks
                    .iter()
                    .take_while(|m| {
                        (m.shard as usize) < sealed.len()
                            && m.shard != UNTRACKED
                            && m.epoch <= sealed[m.shard as usize]
                    })
                    .count();
                if droppable == 0 {
                    return true;
                }
                let cut = slot
                    .marks
                    .get(droppable)
                    .map_or(slot.points.len(), |m| m.start);
                freed.push(slot.points.drain(..cut).collect());
                slot.marks.drain(..droppable);
                for m in slot.marks.iter_mut() {
                    m.start -= cut;
                }
                // Shed series whose points are all gone, once they outnumber the
                // points that remain; otherwise churning label sets would grow
                // the registry without bound.
                if slot.series.len() > slot.points.len().max(16) {
                    slot.series.clear();
                    slot.series_seen.clear();
                    for dp in &slot.points {
                        if slot.series_seen.insert(dp.labels.addr()) {
                            slot.series.push(dp.labels.clone());
                        }
                    }
                }
                !slot.points.is_empty()
            });
        }
        // Freed on the blocking pool, as `drain_offloaded` does.
        Self::offload_drop(freed).await;
    }

    /// Push log records into the buffer.
    pub async fn push_logs(&self, logs: &[LogRecord]) {
        if !logs.is_empty() {
            tracing::debug!(logs = logs.len(), "buffered logs");
        }
        self.logs.write().await.extend_from_slice(logs);
    }

    /// Push spans into the buffer (queryable immediately, pre-flush).
    pub async fn push_spans(&self, spans: &[Span]) {
        if !spans.is_empty() {
            tracing::debug!(spans = spans.len(), "buffered spans");
        }
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
        let map = self.metrics[map_shard(metric_name)].read().await;
        match map.get(metric_name) {
            Some(slot) => slot
                .points
                .iter()
                .filter(|dp| dp.timestamp_ns >= start_ns && dp.timestamp_ns <= end_ns)
                .cloned()
                .collect(),
            None => Vec::new(),
        }
    }

    /// As [`Self::scan_metrics`], dropping series `filter` rejects before their
    /// points are copied. Returns the points and the number of in-window rows
    /// considered (what an unfiltered scan would have returned).
    ///
    /// The filter runs once per distinct label set, not once per point.
    pub async fn scan_metrics_filtered(
        &self,
        metric_name: &str,
        start_ns: i64,
        end_ns: i64,
        filter: Option<&crate::storage::scanner::SeriesFilter>,
    ) -> (Vec<DataPoint>, u64) {
        let map = self.metrics[map_shard(metric_name)].read().await;
        let Some(slot) = map.get(metric_name) else {
            return (Vec::new(), 0);
        };
        // The filter is judged once per distinct series handle, from the slot's
        // series list, before any point is looked at. A selector that no series
        // satisfies then costs nothing, and for the rest each point needs only its
        // handle's address looked up (and consecutive points of one series, the
        // usual layout, not even that).
        let accepted: Option<std::collections::HashSet<usize, ahash::RandomState>> =
            filter.map(|f| {
                slot.series
                    .iter()
                    .filter(|labels| f(labels))
                    .map(|labels| labels.addr())
                    .collect()
            });
        if accepted.as_ref().is_some_and(|a| a.is_empty()) {
            return (Vec::new(), 0);
        }
        let mut out = Vec::new();
        let mut considered = 0u64;
        // Address of the previous point's handle and the verdict for it.
        let mut last: Option<(usize, bool)> = None;
        for dp in &slot.points {
            if dp.timestamp_ns < start_ns || dp.timestamp_ns > end_ns {
                continue;
            }
            considered += 1;
            let keep = match &accepted {
                None => true,
                Some(set) => {
                    let addr = dp.labels.addr();
                    match last {
                        Some((a, v)) if a == addr => v,
                        _ => {
                            let v = set.contains(&addr);
                            last = Some((addr, v));
                            v
                        }
                    }
                }
            };
            if keep {
                out.push(dp.clone());
            }
        }
        (out, considered)
    }

    /// Query logs from the buffer matching the given time range.
    pub async fn scan_logs(&self, start_ns: i64, end_ns: i64) -> Vec<LogRecord> {
        let buf = self.logs.read().await;
        buf.iter()
            .filter(|l| l.timestamp_ns >= start_ns && l.timestamp_ns <= end_ns)
            .cloned()
            .collect()
    }

    /// As [`Self::scan_logs`], but only rows meeting `keep` are cloned out, so a
    /// selective query does not copy the whole buffer to discard most of it.
    pub async fn scan_logs_filtered<F>(&self, start_ns: i64, end_ns: i64, keep: F) -> Vec<LogRecord>
    where
        F: Fn(&LogRecord) -> bool,
    {
        let buf = self.logs.read().await;
        buf.iter()
            .filter(|l| l.timestamp_ns >= start_ns && l.timestamp_ns <= end_ns && keep(l))
            .cloned()
            .collect()
    }

    /// O(1) — get metric names directly from HashMap keys.
    pub async fn metric_names(&self) -> Vec<String> {
        let mut names = Vec::new();
        for map in self.metrics.iter() {
            names.extend(map.read().await.keys().cloned());
        }
        names
    }

    /// Get all unique label names from buffered metrics.
    pub async fn label_names(&self) -> Vec<String> {
        let mut labels: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
        for map in self.metrics.iter() {
            let map = map.read().await;
            for slot in map.values() {
                for set in &slot.series {
                    labels.extend(set.keys().cloned());
                }
            }
        }
        labels.into_iter().collect()
    }

    /// Empties the metric buffer and hands the value back for the caller to
    /// hand to [`Self::offload_drop`].
    ///
    /// Dropping the value under the lock is safe: the lock is released
    /// immediately afterwards, and a concurrent `mem::take` would win the same
    /// race anyway.
    pub async fn take_metrics(&self) -> Vec<(String, Vec<DataPoint>)> {
        let mut drained: Vec<(String, Vec<DataPoint>)> = Vec::new();
        for map in self.metrics.iter() {
            let mut map = map.write().await;
            drained.extend(
                std::mem::take(&mut *map)
                    .into_iter()
                    .map(|(name, slot)| (name, slot.points)),
            );
        }
        let total: usize = drained.iter().map(|(_, v)| v.len()).sum();
        tracing::debug!(
            signal = "metrics",
            drained = total,
            "buffer drained after flush"
        );
        drained
    }

    /// Empties the log buffer and hands the value back for the caller to hand
    /// to [`Self::offload_drop`].
    pub async fn take_logs(&self) -> Vec<LogRecord> {
        let drained = {
            let mut buf = self.logs.write().await;
            std::mem::take(&mut *buf)
        };
        tracing::debug!(
            signal = "logs",
            drained = drained.len(),
            "buffer drained after flush"
        );
        drained
    }

    /// Empties the span buffer and hands the value back for the caller to hand
    /// to [`Self::offload_drop`].
    pub async fn take_spans(&self) -> Vec<Span> {
        let drained = {
            let mut buf = self.spans.write().await;
            std::mem::take(&mut *buf)
        };
        tracing::debug!(
            signal = "traces",
            drained = drained.len(),
            "buffer drained after flush"
        );
        drained
    }

    /// Removes the oldest `n` buffered log records: the ones a flushed block
    /// holds, when buffer order and writer order are the same.
    pub async fn drop_flushed_logs(&self, n: usize) {
        let dropped: Vec<LogRecord> = {
            let mut buf = self.logs.write().await;
            let n = n.min(buf.len());
            buf.drain(..n).collect()
        };
        Self::offload_drop(dropped).await;
    }

    /// Removes the oldest `n` buffered spans (see [`Self::drop_flushed_logs`]).
    pub async fn drop_flushed_spans(&self, n: usize) {
        let dropped: Vec<Span> = {
            let mut buf = self.spans.write().await;
            let n = n.min(buf.len());
            buf.drain(..n).collect()
        };
        Self::offload_drop(dropped).await;
    }

    /// Drains one signal and frees the drained records on the blocking pool
    /// rather than on the caller's runtime thread.
    ///
    /// Freeing up to `max_rows_per_block` records is millions of `free()`
    /// calls in one uninterruptible burst, and it lands immediately after the
    /// flush that already blocked the runtime — a latency spike on whichever
    /// worker happened to trigger it. Spawning one task per drain is cheap
    /// relative to the flush that preceded it.
    ///
    /// `signal` selects which buffer to drain.
    pub async fn drain_offloaded(&self, signal: SignalType) {
        match signal {
            SignalType::Metrics => {
                let drained = self.take_metrics().await;
                Self::offload_drop(drained).await;
            }
            SignalType::Logs => {
                let drained = self.take_logs().await;
                Self::offload_drop(drained).await;
            }
            SignalType::Traces => {
                let drained = self.take_spans().await;
                Self::offload_drop(drained).await;
            }
        }
    }

    /// Frees a drained value on the blocking pool instead of the caller's
    /// runtime thread.
    pub async fn offload_drop<T: Send + 'static>(value: T) {
        tokio::task::spawn_blocking(move || {
            // Explicitly dropped on the blocking thread.
            drop(value);
        })
        .await
        .map_err(|e| tracing::warn!(error = %e, "buffer teardown task panicked"))
        .ok();
    }

    /// Buffer stats for monitoring: (metrics, logs, spans).
    pub async fn stats(&self) -> (usize, usize, usize) {
        let mut m = 0usize;
        for map in self.metrics.iter() {
            m += map
                .read()
                .await
                .values()
                .map(|s| s.points.len())
                .sum::<usize>();
        }
        let l = self.logs.read().await.len();
        let s = self.spans.read().await.len();
        (m, l, s)
    }

    /// Freshest label values for a label name, bounded and prefix-filtered.
    ///
    /// Builder autocomplete path: points append in ingest order (≈ time
    /// order), so walking each series' points newest-first yields the most
    /// recent values. Stops after `limit` distinct matches — never scans the
    /// whole buffer when the answer is already complete.
    pub async fn recent_label_values(
        &self,
        label: &str,
        prefix: Option<&str>,
        limit: usize,
    ) -> Vec<String> {
        let mut out: Vec<String> = Vec::with_capacity(limit.min(16));
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        // A label set repeats on every point of its series; look at each once.
        let mut visited: std::collections::HashSet<usize, ahash::RandomState> = Default::default();
        for map in self.metrics.iter() {
            let map = map.read().await;
            for slot in map.values() {
                for dp in slot.points.iter().rev() {
                    if !visited.insert(dp.labels.addr()) {
                        continue;
                    }
                    if let Some(v) = dp.labels.get(label) {
                        if prefix.is_none_or(|p| v.starts_with(p)) && seen.insert(v.to_string()) {
                            out.push(v.to_string());
                            if out.len() >= limit {
                                return out;
                            }
                        }
                    }
                }
            }
        }
        out
    }

    /// All distinct values of one label across every buffered metric series,
    /// newest-first per series. Unbounded (mirror of [`Self::label_names`])
    /// so full-enumeration paths (`list_label_values`) see the freshest
    /// data the block index may not cover yet.
    pub async fn label_values(&self, label: &str) -> std::collections::HashSet<String> {
        let mut out = std::collections::HashSet::new();
        for map in self.metrics.iter() {
            let map = map.read().await;
            for slot in map.values() {
                for set in &slot.series {
                    if let Some(v) = set.get(label) {
                        if !out.contains(v) {
                            out.insert(v.to_string());
                        }
                    }
                }
            }
        }
        out
    }
}

impl Default for MemoryBuffer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests;
