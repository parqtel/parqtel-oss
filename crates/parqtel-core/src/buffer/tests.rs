//! Tests for the `buffer` module.

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
