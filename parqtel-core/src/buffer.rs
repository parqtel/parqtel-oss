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
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;
    use crate::models::traces::SpanStatus;

    /// A selector is judged per series, not per point, and must return exactly the
    /// points of the series it accepts - interleaved with others, split across
    /// pushes, and with equal-but-unshared label sets.
    #[tokio::test]
    async fn filtered_metric_scan_returns_only_accepted_series() {
        use crate::models::labels::LabelSet;
        use crate::models::metrics::{DataPoint, MetricValue};
        let buf = MemoryBuffer::new();
        let host = |h: &str| LabelSet::try_from_iter(vec![("host", h)]).unwrap();
        let pt = |ts: i64, h: &LabelSet| {
            DataPoint::new(ts, MetricValue::Double(ts as f64), h.clone()).unwrap()
        };
        let (a, b) = (host("a"), host("b"));
        buf.push_metrics("m", &[pt(1, &a), pt(2, &b), pt(3, &a), pt(4, &b)])
            .await;
        // The same series again as a separate allocation.
        buf.push_metrics("m", &[pt(5, &host("a")), pt(6, &host("c"))])
            .await;

        let only_a: crate::storage::scanner::SeriesFilter =
            std::sync::Arc::new(|l: &LabelSet| l.get("host") == Some("a"));
        let (got, considered) = buf.scan_metrics_filtered("m", 0, 100, Some(&only_a)).await;
        let ts: Vec<i64> = got.iter().map(|p| p.timestamp_ns).collect();
        assert_eq!(ts, vec![1, 3, 5]);
        assert_eq!(considered, 6);

        // Window applies on top.
        let (got, _) = buf.scan_metrics_filtered("m", 2, 4, Some(&only_a)).await;
        assert_eq!(got.len(), 1);

        // No series accepted: nothing is returned or read.
        let none: crate::storage::scanner::SeriesFilter = std::sync::Arc::new(|_: &LabelSet| false);
        assert_eq!(
            buf.scan_metrics_filtered("m", 0, 100, Some(&none)).await,
            (Vec::new(), 0)
        );
        // No filter returns everything in the window.
        assert_eq!(
            buf.scan_metrics_filtered("m", 0, 100, None).await.0.len(),
            6
        );
        assert!(buf
            .scan_metrics_filtered("absent", 0, 100, None)
            .await
            .0
            .is_empty());
    }

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
    async fn take_spans_empties_buffer() {
        let buf = MemoryBuffer::new();
        buf.push_spans(&[test_span(1, 1_000, 2_000)]).await;
        let drained = buf.take_spans().await;
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

    fn test_dp(ts: i64, user: &str) -> DataPoint {
        DataPoint::new(
            ts + 1,
            crate::models::metrics::MetricValue::Double(1.0),
            crate::models::labels::LabelSet::try_from_iter(vec![("user_id", user)]).unwrap(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn recent_label_values_is_bounded_and_fresh() {
        // High-cardinality: 30 distinct users, buffer must stop at limit.
        let buf = MemoryBuffer::new();
        let dps: Vec<_> = (0..30)
            .map(|i| test_dp(i * 1000, &format!("user-{i:03}")))
            .collect();
        buf.push_metrics("user_sessions_active_0", &dps).await;

        let top = buf.recent_label_values("user_id", None, 10).await;
        assert_eq!(top.len(), 10);
        // Points append chronologically → newest (user-029) comes first.
        assert_eq!(top[0], "user-029");
        // All distinct.
        let set: std::collections::HashSet<_> = top.iter().cloned().collect();
        assert_eq!(set.len(), top.len());
    }

    #[tokio::test]
    async fn recent_label_values_prefix_filter() {
        let buf = MemoryBuffer::new();
        let dps: Vec<_> = (0..30)
            .map(|i| test_dp(i * 1000, &format!("user-{i:03}")))
            .collect();
        buf.push_metrics("m", &dps).await;
        // Only values starting with the typed prefix come back — the
        // builder narrows high-cardinality labels as the user types.
        let hits = buf
            .recent_label_values("user_id", Some("user-02"), 10)
            .await;
        assert!(!hits.is_empty());
        assert!(hits.iter().all(|v| v.starts_with("user-02")));
        // Zero-length result for a prefix that matches nothing.
        assert!(buf
            .recent_label_values("user_id", Some("zzz"), 10)
            .await
            .is_empty());
        // Missing label → empty, not an error.
        assert!(buf.recent_label_values("nope", None, 10).await.is_empty());
    }

    fn pt(ts: i64) -> DataPoint {
        DataPoint::new(
            ts,
            crate::models::metrics::MetricValue::Double(ts as f64),
            crate::models::labels::LabelSet::default(),
        )
        .unwrap()
    }

    async fn stamps(buf: &MemoryBuffer, name: &str) -> Vec<i64> {
        buf.scan_metrics(name, 0, i64::MAX)
            .await
            .iter()
            .map(|p| p.timestamp_ns)
            .collect()
    }

    /// Dropping a sealed epoch removes exactly the points that were in the
    /// flushed writer; points buffered afterwards (a later epoch) survive.
    #[tokio::test]
    async fn drop_sealed_keeps_points_from_later_epochs() {
        let buf = MemoryBuffer::new();
        buf.push_metrics_tagged("m", &[pt(1), pt(2)], 0, 1).await;
        buf.push_metrics_tagged("m", &[pt(3)], 0, 1).await;
        // The flush sealed epoch 1; these arrive during its encode.
        buf.push_metrics_tagged("m", &[pt(4), pt(5)], 0, 2).await;

        buf.drop_sealed_metrics(&[1]).await;
        assert_eq!(stamps(&buf, "m").await, vec![4, 5]);

        // The next flush seals epoch 2 and empties the metric.
        buf.drop_sealed_metrics(&[2]).await;
        assert!(stamps(&buf, "m").await.is_empty());
        assert!(buf.metric_names().await.is_empty());
        assert_eq!(buf.stats().await.0, 0);
    }

    /// Shards are independent: sealing one shard's epoch leaves another's alone,
    /// and a shard that was not sealed (0) is never touched.
    #[tokio::test]
    async fn drop_sealed_is_per_shard() {
        let buf = MemoryBuffer::new();
        buf.push_metrics_tagged("a", &[pt(1)], 0, 1).await;
        buf.push_metrics_tagged("b", &[pt(2)], 1, 1).await;
        buf.push_metrics_tagged("b", &[pt(3)], 1, 2).await;

        buf.drop_sealed_metrics(&[1, 0]).await;
        assert!(stamps(&buf, "a").await.is_empty());
        assert_eq!(stamps(&buf, "b").await, vec![2, 3]);

        buf.drop_sealed_metrics(&[0, 1]).await;
        assert_eq!(stamps(&buf, "b").await, vec![3]);
    }

    /// Points pushed without epoch tracking are only removed by a full drain.
    #[tokio::test]
    async fn untracked_points_ignore_sealed_epochs() {
        let buf = MemoryBuffer::new();
        buf.push_metrics("m", &[pt(1)]).await;
        buf.drop_sealed_metrics(&[u64::MAX, u64::MAX]).await;
        assert_eq!(stamps(&buf, "m").await, vec![1]);
        buf.drain_offloaded(SignalType::Metrics).await;
        assert!(stamps(&buf, "m").await.is_empty());
    }

    /// The sharded map keeps every whole-buffer view consistent.
    #[tokio::test]
    async fn whole_buffer_views_span_all_map_shards() {
        let buf = MemoryBuffer::new();
        for i in 0..64 {
            buf.push_metrics(&format!("metric_{i}"), &[test_dp(i + 1, &format!("u{i}"))])
                .await;
        }
        let mut names = buf.metric_names().await;
        names.sort();
        assert_eq!(names.len(), 64);
        assert_eq!(buf.stats().await.0, 64);
        assert_eq!(buf.label_names().await, vec!["user_id".to_string()]);
        assert_eq!(buf.label_values("user_id").await.len(), 64);
        assert_eq!(buf.take_metrics().await.len(), 64);
        assert_eq!(buf.stats().await.0, 0);
    }
}
