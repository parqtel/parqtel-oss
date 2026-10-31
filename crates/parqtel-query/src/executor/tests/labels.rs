//! Metric and label discovery.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn test_list_metrics() {
    let (exec, _dir) = setup_with_data().await;
    let metrics = exec.list_metrics().await;
    assert!(metrics.contains("cpu"));
    assert!(metrics.contains("mem"));
}

#[tokio::test]
async fn test_list_labels() {
    let (exec, _dir) = setup_with_data().await;
    let labels = exec.list_labels(None).await;
    assert!(labels.contains("host"));
}

#[tokio::test]
async fn test_list_label_values() {
    let (exec, _dir) = setup_with_data().await;
    let values = exec.list_label_values("host").await;
    assert!(values.contains("h1"));
    assert!(values.contains("h2"));
}

#[tokio::test]
async fn test_list_label_values_merges_buffer() {
    // Values that exist only in the in-memory buffer (never flushed)
    // must still surface: on a busy cluster the last-5-blocks window
    // can otherwise hide whole label dimensions.
    let (exec, _dir) = setup_with_data().await;
    let dps: Vec<_> = (0..3)
        .map(|i| {
            parqtel_core::DataPoint::new(
                50_000 + i,
                parqtel_core::MetricValue::Double(i as f64),
                LabelSet::try_from_iter(vec![("buffer_only_dim", format!("bval-{i}"))]).unwrap(),
            )
            .unwrap()
        })
        .collect();
    exec.buffer()
        .push_metrics("buffer_only_metric_0", &dps)
        .await;

    let values = exec.list_label_values("buffer_only_dim").await;
    assert!(values.contains("bval-0"));
    assert!(values.contains("bval-2"));
}

#[tokio::test]
async fn test_recent_label_values_bounded() {
    // High-cardinality scenario: a label with far more distinct values
    // than the builder's top-N — the bounded path must return exactly
    // `limit` values, newest-first from the buffer.
    let (exec, _dir) = setup_with_data().await;
    // Seed the in-memory buffer with 50 distinct users.
    let dps: Vec<_> = (0..50)
        .map(|i| {
            parqtel_core::DataPoint::new(
                10_000 + i,
                parqtel_core::MetricValue::Double(i as f64),
                LabelSet::try_from_iter(vec![("user_id", format!("user-{i:04}"))]).unwrap(),
            )
            .unwrap()
        })
        .collect();
    exec.buffer()
        .push_metrics("user_sessions_active_0", &dps)
        .await;

    let top = exec.recent_label_values("user_id", None, 10, 5).await;
    assert_eq!(top.len(), 10, "bounded to limit");
    // Newest-first: the buffer's last point carries user-0049.
    assert_eq!(top[0], "user-0049", "freshest value first");
    assert!(
        top.iter().all(|v| v.starts_with("user-")),
        "all match label"
    );

    // Prefix filter narrows server-side.
    let pref = exec
        .recent_label_values("user_id", Some("user-004"), 10, 5)
        .await;
    assert!(
        pref.iter().all(|v| v.starts_with("user-004")),
        "prefix applied"
    );
    assert!(!pref.is_empty(), "prefix found matches");

    // No matches for an absent prefix.
    let none = exec
        .recent_label_values("user_id", Some("zzz"), 10, 5)
        .await;
    assert!(none.is_empty(), "no spurious matches");
}

#[tokio::test]
async fn test_recent_label_values_flushed_blocks() {
    // Blocks written by the plain storage engine carry no flush-time
    // label_values index (only the ingest writer builds one); the
    // bounded path must degrade gracefully — no panic, empty-from-blocks,
    // buffer values still visible.
    let (exec, _dir) = setup_with_data().await;
    let dps: Vec<_> = (0..5)
        .map(|i| {
            parqtel_core::DataPoint::new(
                10_000 + i,
                parqtel_core::MetricValue::Double(i as f64),
                LabelSet::try_from_iter(vec![("host", format!("h{i}"))]).unwrap(),
            )
            .unwrap()
        })
        .collect();
    exec.buffer().push_metrics("m", &dps).await;
    let top = exec.recent_label_values("host", None, 10, 5).await;
    assert!(!top.is_empty(), "buffer values visible");
    assert_eq!(top[0], "h4", "freshest first");
}
