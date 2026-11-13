//! Sharded block rotator: routing, capacity and accounting.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn tx2() -> mpsc::UnboundedSender<PendingIndex> {
    mpsc::unbounded_channel().0
}

#[tokio::test]
async fn test_block_rotator_flush() {
    let dir = tempdir().unwrap();
    let config = tiny_metrics_config(dir.path());
    let (tx, mut rx) = mpsc::unbounded_channel();
    let rotator = BlockRotator::new(config, tx);

    rotator
        .push(one_point_metric("m1", 100), None)
        .await
        .unwrap();

    rotator.flush(None).await.unwrap();
    let meta = rx.recv().await.unwrap().meta;
    assert_eq!(meta.row_count, 1);
    assert!(meta.path.exists());
}

/// An idle flush must not produce an observation: it dilutes the flush
/// latency histogram and would show up as a p50 far below any real flush.
#[tokio::test]
async fn test_idle_flush_is_not_recorded() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

    rotator.flush(Some(Arc::clone(&contention))).await.unwrap();
    assert!(rx.try_recv().is_err(), "no block should be written");
    assert_eq!(contention.flush_duration_count(SignalType::Metrics), 0);
    assert_eq!(contention.flush_rows(SignalType::Metrics), 0);
    assert_eq!(contention.flush_inflight(SignalType::Metrics), 0);
}

/// A real flush records wall time, rows written, and leaves the in-flight
/// gauge at zero — the guard must not leak on the error path either.
#[tokio::test]
async fn test_flush_records_duration_rows_and_clears_inflight() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

    rotator
        .push(one_point_metric("m1", 100), Some(Arc::clone(&contention)))
        .await
        .unwrap();
    rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

    let meta = rx.recv().await.unwrap().meta;
    assert_eq!(contention.flush_duration_count(SignalType::Metrics), 1);
    assert_eq!(contention.flush_rows(SignalType::Metrics), 1);
    assert_eq!(contention.flush_inflight(SignalType::Metrics), 0);
    assert_eq!(
        contention.flush_rows(SignalType::Metrics),
        meta.row_count as u64,
        "rows recorded must match the block that was written"
    );
}

/// Crossing the row cap triggers a flush from inside `push`, so the
/// capacity-triggered path — the expensive case that the 5s tick never
/// sees — must be observable too.
#[tokio::test]
async fn test_capacity_triggered_flush_is_recorded() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

    // Ten points exactly fill the cap: no flush, because the check is
    // "would this push overflow it" — so a block is never larger than the
    // cap plus one request.
    for i in 0..10 {
        let flushed = rotator
            .push(
                one_point_metric("m1", 100 + i),
                Some(Arc::clone(&contention)),
            )
            .await
            .unwrap();
        assert!(!flushed, "no flush while the batch still fits");
    }
    assert_eq!(contention.flush_duration_count(SignalType::Metrics), 0);
    assert_eq!(rotator.buffered_rows(), 10, "the counter tracks the rows");

    // The 11th point would overflow, so the block is closed first. That
    // push must report the flush so the caller drains the memory buffer and
    // the flushed rows are not read twice.
    let flushed = rotator
        .push(one_point_metric("m1", 200), Some(Arc::clone(&contention)))
        .await
        .unwrap();
    assert!(flushed, "push must report the flush so the buffer drains");

    let meta = rx.recv().await.unwrap().meta;
    assert_eq!(meta.row_count, 10);
    assert_eq!(contention.flush_duration_count(SignalType::Metrics), 1);
    assert_eq!(contention.flush_rows(SignalType::Metrics), 10);
    assert_eq!(contention.flush_inflight(SignalType::Metrics), 0);
}

/// The shards exist to remove cross-metric contention, so a metric must
/// always route to the same shard — otherwise one metric's rows scatter
/// across buffers and the merge becomes order-dependent.
#[tokio::test]
async fn test_shard_routing_is_stable_and_bounded() {
    for count in [1usize, 2, 4, 8] {
        for name in ["cpu", "memory_used", "http_requests_total", "a", "zzz"] {
            let a = shard_for(name, count);
            assert!(a < count, "shard {a} out of range for {count} shards");
            assert_eq!(
                shard_for(name, count),
                a,
                "routing for {name} must be stable across calls"
            );
        }
    }
    // Distinct metrics must be able to land on different shards, or the
    // sharding buys nothing.
    let spread: std::collections::HashSet<usize> = (0..64)
        .map(|i| shard_for(&format!("metric_{i}"), 8))
        .collect();
    assert!(spread.len() > 1, "shards must actually spread load");
}

/// Zero or absurd shard counts must degrade gracefully: 1 restores the
/// previous single-writer behaviour and the upper bound keeps a bad config
/// from allocating hundreds of writer buffers.
#[tokio::test]
async fn test_shard_count_is_clamped() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, _rx) = mpsc::unbounded_channel();
    for (configured, expected) in [(0usize, 1usize), (1, 1), (4, 4), (10_000, 256)] {
        let rotator =
            BlockRotator::with_shards(tiny_metrics_config(dir.path()), tx.clone(), configured);
        assert_eq!(
            rotator.shard_count(),
            expected,
            "configured {configured} should clamp to {expected}"
        );
    }
}

/// The whole point of sharding the locks without sharding the blocks: a
/// flush must still write **one** file, no matter how many shards the rows
/// came from. If this regresses to one block per shard, query fan-out
/// multiplies silently.
#[tokio::test]
async fn test_sharded_flush_writes_one_block() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    // A cap comfortably above the row count, so no rotation is triggered
    // by the cap check and the explicit flush below is the only writer.
    let rotator = BlockRotator::with_shards(
        BlockConfig {
            max_rows_per_block: 100,
            row_group_size: 50,
            block_duration_secs: 3600,
            ..tiny_metrics_config(dir.path())
        },
        tx,
        4,
    );
    assert_eq!(rotator.shard_count(), 4);

    // 16 distinct metric names spread over 4 shards.
    for i in 0..16 {
        rotator
            .push(
                one_point_metric(&format!("m{i}"), 100 + i),
                Some(Arc::clone(&contention)),
            )
            .await
            .unwrap();
    }
    rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

    let mut metas = Vec::new();
    while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
        metas.push(meta);
    }
    assert_eq!(
        metas.len(),
        1,
        "shards must merge into a single block, got {} blocks",
        metas.len()
    );
    assert_eq!(
        metas[0].row_count, 16,
        "every shard's rows must be in the merged block"
    );
    assert_eq!(contention.flush_rows(SignalType::Metrics), 16);
}

/// A shard must not rotate its own block just because *its* slice of the
/// cap is full — the cap is a whole-block budget. Rotating per shard would
/// put back the block-count multiplication the merge exists to avoid.
#[tokio::test]
async fn test_shards_do_not_rotate_early_on_their_own_slice() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    // Cap 16, 4 shards: each shard's share is 4 rows. Filling all four to
    // exactly the share must not produce four blocks.
    let rotator = BlockRotator::with_shards(
        BlockConfig {
            max_rows_per_block: 16,
            row_group_size: 8,
            block_duration_secs: 3600,
            ..tiny_metrics_config(dir.path())
        },
        tx,
        4,
    );

    // 15 of 16 rows: below the cap, so nothing may be written even though
    // each shard already holds more than a quarter of it.
    for i in 0..15 {
        rotator
            .push(one_point_metric(&format!("m{i}"), 100 + i), None)
            .await
            .unwrap();
    }
    assert!(
        rx.try_recv().is_err(),
        "no block may be written before the whole-block cap is reached"
    );

    // The 16th row reaches the cap: exactly one merged block.
    rotator
        .push(one_point_metric("m15", 115), None)
        .await
        .unwrap();
    rotator.flush(None).await.unwrap();
    let mut metas = Vec::new();
    while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
        metas.push(meta);
    }
    assert_eq!(metas.len(), 1, "the four shards must merge into one block");
    assert_eq!(metas[0].row_count, 16);
}

/// The buffered-row counter is what gates rotation, so it must track the
/// writers exactly. A drift upward would mean the cap never fires again and
/// blocks grow without bound; a drift downward would mean constant
/// pointless flushes.
#[tokio::test]
async fn test_buffered_counter_tracks_the_writers() {
    let dir = tempdir().unwrap();
    let (tx, _rx) = mpsc::unbounded_channel();
    let rotator = BlockRotator::with_shards(
        BlockConfig {
            max_rows_per_block: 1000,
            row_group_size: 500,
            block_duration_secs: 3600,
            ..tiny_metrics_config(dir.path())
        },
        tx,
        4,
    );

    assert_eq!(rotator.buffered_rows(), 0);
    for i in 0..37 {
        rotator
            .push(one_point_metric(&format!("m{}", i % 8), 100 + i), None)
            .await
            .unwrap();
    }
    assert_eq!(
        rotator.buffered_rows(),
        37,
        "counter must equal pushed rows"
    );

    rotator.flush(None).await.unwrap();
    assert_eq!(rotator.buffered_rows(), 0, "a flush must zero the counter");

    // A mid-metric split inside the writer must not leave the counter high:
    // those rows left through the writer, not through `flush_locked`.
    let split = BlockRotator::with_shards(
        BlockConfig {
            max_rows_per_block: 5,
            row_group_size: 5,
            block_duration_secs: 3600,
            ..tiny_metrics_config(dir.path())
        },
        tx2(),
        1,
    );
    let points: Vec<_> = (0..13)
        .map(|i| {
            parqtel_core::DataPoint::new(
                100 + i,
                parqtel_core::MetricValue::Double(1.0),
                parqtel_core::LabelSet::default(),
            )
            .unwrap()
        })
        .collect();
    split
        .push(
            Metric {
                name: "split".into(),
                kind: parqtel_core::MetricKind::Gauge,
                data_points: points,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        split.buffered_rows(),
        13 - 10,
        "the 10 rows the writer closed mid-metric must not stay counted"
    );
}

/// Regression test for a counter bug that only appeared under contention.
///
/// The buffered-row counter drives rotation, so a drift in either
/// direction is serious: upward pins it above the cap and turns every push
/// into a flush (measured: 2263 blocks of ~20 rows instead of 24 of 2000);
/// downward rotates constantly for no reason. An earlier implementation
/// folded "rows added" and "rows the writer flushed internally" into one
/// `points - closed`, which saturated to zero whenever a push landed in a
/// full writer and silently lost both adjustments.
///
/// Compares the counter against the shard writers' real contents at the cap
/// boundary, for one shard and several.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_buffered_counter_matches_writers_under_contention() {
    for shards in [1usize, 4] {
        let dir = tempfile::tempdir().unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let rotator = Arc::new(BlockRotator::with_shards(
            BlockConfig {
                max_rows_per_block: 2000,
                row_group_size: 1000,
                block_duration_secs: 3600,
                ..tiny_metrics_config(dir.path())
            },
            tx,
            shards,
        ));

        let mut handles = Vec::new();
        for w in 0..40 {
            let rotator = rotator.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..60 {
                    let dps: Vec<_> = (0..20)
                        .map(|k| {
                            parqtel_core::DataPoint::new(
                                1_000_000 * (i as i64 + 1) + k,
                                parqtel_core::MetricValue::Double(1.0),
                                parqtel_core::LabelSet::default(),
                            )
                            .unwrap()
                        })
                        .collect();
                    rotator
                        .push(
                            Metric {
                                name: format!("metric_{}", (w * 60 + i) % 60),
                                kind: parqtel_core::MetricKind::Gauge,
                                data_points: dps,
                                ..Default::default()
                            },
                            None,
                        )
                        .await
                        .unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(
            rotator.buffered_rows(),
            rotator.actual_rows().await,
            "shards={shards}: counter must equal the writers' real contents"
        );

        rotator.flush(None).await.unwrap();
        assert_eq!(
            rotator.buffered_rows(),
            0,
            "shards={shards}: a flush must zero the counter"
        );
        assert_eq!(rotator.actual_rows().await, 0);

        // Blocks must respect the cap rather than degenerating into one
        // per push, and no block may exceed it by more than one request.
        const TOTAL: usize = 40 * 60 * 20;
        const REQUEST: usize = 60 * 20;
        let mut blocks = Vec::new();
        while let Ok(p) = rx.try_recv() {
            blocks.push(p.meta.row_count);
        }
        let written: usize = blocks.iter().sum();
        assert_eq!(written, TOTAL, "shards={shards}: no rows may be lost");
        assert!(
            blocks.len() <= TOTAL / 1000 + 40,
            "shards={shards}: {} blocks for {TOTAL} rows indicates a flush per push",
            blocks.len()
        );
        assert!(
            blocks.iter().all(|&b| b <= 2000 + REQUEST),
            "shards={shards}: a block may exceed the cap by at most one request"
        );
    }
}

/// Concurrent pushes across shards must not lose or duplicate rows, and a
/// concurrent flush must not swallow a push that arrives mid-encode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_sharded_pushes_lose_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let rotator = Arc::new(BlockRotator::with_shards(
        BlockConfig {
            max_rows_per_block: 50,
            row_group_size: 50,
            block_duration_secs: 3600,
            ..tiny_metrics_config(dir.path())
        },
        tx,
        4,
    ));

    const WORKERS: usize = 8;
    const PER_WORKER: usize = 25;
    let mut handles = Vec::new();
    for w in 0..WORKERS {
        let rotator = rotator.clone();
        let contention = contention.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..PER_WORKER {
                // Distinct names route to distinct shards.
                let name = format!("m{}_{i}", w % 4);
                rotator
                    .push(
                        one_point_metric(&name, 1000 * (i as i64 + 1)),
                        Some(Arc::clone(&contention)),
                    )
                    .await
                    .unwrap();
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }
    rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

    let total: usize = std::iter::from_fn(|| rx.try_recv().ok())
        .map(|p: PendingIndex| p.meta.row_count)
        .sum();
    assert_eq!(
        total,
        WORKERS * PER_WORKER,
        "every concurrently pushed point must be written exactly once"
    );
}

/// A single metric larger than a whole block must be split across blocks
/// and ingested in full — not partially accepted and then reported as an
/// error, which made clients retry a batch that was already half-ingested.
#[tokio::test]
async fn test_oversized_metric_is_split_across_blocks() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

    // 25 points into a block that holds 10: three blocks, no error.
    let points: Vec<_> = (0..25)
        .map(|i| {
            parqtel_core::DataPoint::new(
                1000 + i,
                parqtel_core::MetricValue::Double(i as f64),
                parqtel_core::LabelSet::default(),
            )
            .unwrap()
        })
        .collect();
    let flushed = rotator
        .push(
            Metric {
                name: "big".into(),
                kind: parqtel_core::MetricKind::Gauge,
                data_points: points,
                ..Default::default()
            },
            Some(Arc::clone(&contention)),
        )
        .await
        .unwrap();
    assert!(flushed, "splitting a metric must report the flush");

    rotator.flush(Some(Arc::clone(&contention))).await.unwrap();

    let mut metas = Vec::new();
    while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
        metas.push(meta);
    }
    let total: usize = metas.iter().map(|m| m.row_count).sum();
    assert_eq!(
        total, 25,
        "every point must land exactly once across the split blocks"
    );
    assert!(
        metas.len() >= 3,
        "25 points at 10 per block needs at least 3 blocks, got {}",
        metas.len()
    );
    // Split blocks must be published, not orphaned: the index only learns
    // about blocks whose metadata reaches the channel.
    assert_eq!(
        contention.flush_rows(SignalType::Metrics),
        25,
        "rows from the split blocks must be accounted for"
    );
}

/// Every block written — including the ones the writer closes mid-push —
/// must publish its metadata, or the data becomes unqueryable even though
/// the bytes are on disk.
#[tokio::test]
async fn test_split_blocks_are_published_to_the_index() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let rotator = BlockRotator::new(tiny_metrics_config(dir.path()), tx);

    let points: Vec<_> = (0..25)
        .map(|i| {
            parqtel_core::DataPoint::new(
                1000 + i,
                parqtel_core::MetricValue::Double(i as f64),
                parqtel_core::LabelSet::default(),
            )
            .unwrap()
        })
        .collect();
    rotator
        .push(
            Metric {
                name: "big".into(),
                kind: parqtel_core::MetricKind::Gauge,
                data_points: points,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // The two closed blocks must already be in the channel before any
    // explicit flush; the tail is still buffered.
    let mut published = 0usize;
    while let Ok(meta) = rx.try_recv().map(|p| p.meta) {
        assert!(meta.path.exists(), "published block must exist on disk");
        published += meta.row_count;
    }
    assert_eq!(
        published, 20,
        "the two blocks closed by the split must be published"
    );
}

/// A log/trace record arriving at a full buffer closes the block and starts
/// the next one, rather than failing the request.
#[tokio::test]
async fn test_full_log_buffer_rolls_over_instead_of_failing() {
    let dir = tempdir().unwrap();
    let (tx, mut rx) = mpsc::unbounded_channel();
    let contention = Arc::new(ContentionMetrics::new());
    let mut rotator = LogRotator::new(
        LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 3,
            block_duration_secs: 3600,
            ..Default::default()
        },
        tx,
    );

    let make = |ts: i64| {
        parqtel_core::LogRecord::new(
            ts,
            ts,
            9,
            "INFO".into(),
            "msg".into(),
            parqtel_core::LabelSet::default(),
            parqtel_core::LabelSet::default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        )
    };

    for i in 0..3 {
        assert!(
            !rotator
                .push(make(100 + i), Some(&contention))
                .await
                .unwrap(),
            "no flush expected while the buffer has room"
        );
    }
    // Fourth record crosses the cap: must roll over, not error.
    assert!(rotator.push(make(103), Some(&contention)).await.unwrap());

    let meta = rx.recv().await.unwrap().meta;
    assert_eq!(meta.row_count, 3, "the full block is written");
    assert_eq!(
        contention.flush_rows(SignalType::Logs),
        3,
        "the rolled-over block must be accounted for"
    );
}

#[tokio::test]
async fn test_check_and_flush_within_duration() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 3600,
        ..Default::default()
    };
    let (tx, _rx) = mpsc::unbounded_channel();
    let rotator = BlockRotator::new(config, tx);

    rotator
        .push(
            parqtel_core::Metric {
                name: "m".into(),
                kind: parqtel_core::MetricKind::Gauge,
                data_points: vec![parqtel_core::DataPoint::new(
                    100,
                    parqtel_core::MetricValue::Double(1.0),
                    parqtel_core::LabelSet::default(),
                )
                .unwrap()],
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();

    // Should not flush since duration hasn't elapsed
    rotator.check_and_flush(None).await.unwrap();
}
