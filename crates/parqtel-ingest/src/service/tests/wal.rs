//! Write-ahead log: recovery and commit ordering.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// Number of WAL-written Parquet blocks in a data dir.
fn wal_files(data_dir: &std::path::Path) -> usize {
    std::fs::read_dir(data_dir)
        .map(|d| {
            d.flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                .count()
        })
        .unwrap_or(0)
}

/// The WAL must not be committed until the sidecar holding the block is
/// durable.
///
/// A flush that commits the WAL right after publishing the metadata, while
/// the sidecar is written up to two seconds later by a debounce, opens a
/// crash window that leaves a block on disk, absent from the sidecar and
/// already marked recovered — so nothing replays it and no query finds it.
///
/// A deliberately slow index task stands in for a slow sidecar write: the
/// commit must not have happened while it was still blocked.
#[tokio::test]
async fn test_wal_commit_waits_for_the_sidecar_to_be_durable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let config = BlockConfig {
        data_dir: data_dir.clone(),
        max_rows_per_block: 10_000,
        block_duration_secs: 3600,
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::unbounded_channel::<PendingIndex>();

    // Read the commit point at the moment the index task is released, before
    // the flush can possibly commit: it must still be START.
    let commit_when_released = Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
    let index_task = tokio::spawn({
        let commit_when_released = commit_when_released.clone();
        let data_dir = data_dir.clone();
        async move {
            while let Some(pending) = rx.recv().await {
                // Stand in for a slow sidecar write.
                tokio::time::sleep(Duration::from_millis(300)).await;
                commit_when_released.store(
                    parqtel_core::wal::read_commit(&data_dir, "metrics").0,
                    std::sync::atomic::Ordering::SeqCst,
                );
                if let Some(ack) = pending.durable {
                    let _ = ack.send(());
                }
            }
        }
    });

    let svc = IngestionService::with_shards(
        config,
        tx,
        1,
        Some(
            WalWriter::open_with_segment_limit(
                &data_dir,
                "metrics",
                WalSyncMode::Always,
                Duration::from_millis(0),
                64 * 1024 * 1024,
            )
            .unwrap(),
        ),
    );
    svc.ingest_metrics(vec![metric_named("ord", 1)])
        .await
        .unwrap();
    svc.shutdown().await.unwrap();
    index_task.abort();

    assert_eq!(
        commit_when_released.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the WAL was committed before the sidecar was durable"
    );
    assert_ne!(
        parqtel_core::wal::read_commit(&data_dir, "metrics").0,
        0,
        "once the index releases the flush, the WAL must be committed"
    );
}

/// A block may only commit once every earlier block is durable, whatever
/// order the concurrent encodes happen to finish in.
#[test]
fn test_wal_commit_never_passes_an_unfinished_flush() {
    let mut f = FlushOrder::default();
    let (a, b, c) = (f.begin(), f.begin(), f.begin());
    // The latest block finishes first: nothing may be committed.
    assert_eq!(f.wal_complete(c, WalPosition(30)), None);
    // The middle one next: still blocked on the first.
    assert_eq!(f.wal_complete(b, WalPosition(20)), None);
    // The first completing releases all three, up to the highest position.
    assert_eq!(f.wal_complete(a, WalPosition(10)), Some(WalPosition(30)));
    // And the frontier keeps going for later flushes.
    let d = f.begin();
    assert_eq!(f.wal_complete(d, WalPosition(40)), Some(WalPosition(40)));
}

/// Logs: one WAL write per request still replays record by record, and the
/// commit point leaves nothing to replay after a clean flush.
#[tokio::test]
async fn test_log_batch_wal_append_replays_each_record() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let svc = LogIngestionService::with_wal(
        LogBlockConfig {
            data_dir: data_dir.clone(),
            max_rows_per_block: 100_000,
            block_duration_secs: 3600,
            ..Default::default()
        },
        tx,
        Some(
            WalWriter::open(
                &data_dir,
                "logs",
                WalSyncMode::Always,
                Duration::from_millis(0),
            )
            .unwrap(),
        ),
    );
    svc.ingest_logs((0..7).map(|i| log_at(i + 1, &format!("m{i}"))).collect())
        .await
        .unwrap();
    svc.ingest_logs(vec![log_at(50, "tail")]).await.unwrap();

    // Nothing flushed yet: a crash here replays all eight, in order.
    let mut bodies = Vec::new();
    let stats = parqtel_core::wal::replay::<parqtel_core::LogRecord, _>(
        &data_dir,
        "logs",
        parqtel_core::wal::read_commit(&data_dir, "logs"),
        |l| bodies.push(l.body),
    )
    .unwrap();
    assert_eq!(stats.records, 8);
    assert_eq!(bodies[0], "m0");
    assert_eq!(bodies[6], "m6");
    assert_eq!(bodies[7], "tail");

    // After a clean shutdown the block covers them, so nothing replays.
    svc.shutdown().await.unwrap();
    let mut replayed = 0;
    parqtel_core::wal::replay::<parqtel_core::LogRecord, _>(
        &data_dir,
        "logs",
        parqtel_core::wal::read_commit(&data_dir, "logs"),
        |_| replayed += 1,
    )
    .unwrap();
    assert_eq!(replayed, 0, "a durable block must commit its WAL range");
}

/// A crash between a block becoming durable and its WAL commit leaves the
/// rows in both. The block says which WAL span it covers (in its footer, so
/// even an un-indexed block does), and startup advances the commit over it.
#[tokio::test]
async fn test_block_on_disk_without_a_wal_commit_is_not_replayed() {
    for signal in ["metrics", "logs", "traces"] {
        let dir = tempdir().unwrap();
        let data_dir = if signal == "logs" {
            dir.path().join("logs")
        } else {
            dir.path().to_path_buf()
        };
        std::fs::create_dir_all(&data_dir).unwrap();
        let wal = || {
            WalWriter::open(
                &data_dir,
                signal,
                WalSyncMode::Always,
                Duration::from_millis(0),
            )
            .unwrap()
        };
        let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
        let _index = spawn_index_task(rx);
        let sig = match signal {
            "metrics" => SignalType::Metrics,
            "logs" => SignalType::Logs,
            _ => SignalType::Traces,
        };
        match signal {
            "metrics" => {
                let cfg = BlockConfig {
                    data_dir: data_dir.clone(),
                    max_rows_per_block: 10_000,
                    block_duration_secs: 3600,
                    ..Default::default()
                };
                let svc = IngestionService::with_shards(cfg, tx, 2, Some(wal()));
                for i in 0..4 {
                    svc.ingest_metrics(vec![metric_named(&format!("m{i}"), i + 1)])
                        .await
                        .unwrap();
                }
                svc.shutdown().await.unwrap();
            }
            "logs" => {
                let cfg = LogBlockConfig {
                    data_dir: data_dir.clone(),
                    max_rows_per_block: 10_000,
                    block_duration_secs: 3600,
                    ..Default::default()
                };
                let svc = LogIngestionService::with_wal(cfg, tx, Some(wal()));
                svc.ingest_logs((0..4).map(|i| log_at(i + 1, "x")).collect())
                    .await
                    .unwrap();
                svc.shutdown().await.unwrap();
            }
            _ => {
                let cfg = BlockConfig {
                    data_dir: data_dir.clone(),
                    max_rows_per_block: 10_000,
                    block_duration_secs: 3600,
                    ..Default::default()
                };
                let svc = TraceIngestionService::with_wal(cfg, tx, Some(wal()));
                svc.ingest_spans((1..=4).map(test_span).collect())
                    .await
                    .unwrap();
                svc.shutdown().await.unwrap();
            }
        }

        // Die before the commit: rewind it, as if the process never wrote it.
        parqtel_core::wal::commit_position(&data_dir, signal, WalPosition::START).unwrap();
        let replay_count = || {
            let mut n = 0;
            parqtel_core::wal::replay::<serde_json::Value, _>(
                &data_dir,
                signal,
                parqtel_core::wal::read_commit(&data_dir, signal),
                |_| n += 1,
            )
            .unwrap();
            n
        };
        assert!(replay_count() > 0, "{signal}: the rewound WAL must replay");

        // A restart: the index has never heard of the block, so reconcile
        // adopts it from the directory - footer and all.
        let mut index = parqtel_core::storage::BlockIndex::new(&data_dir);
        parqtel_core::storage::reconcile(&mut index, sig).unwrap();
        assert_eq!(index.blocks.len(), 1, "{signal}: one block adopted");
        assert!(
            index.blocks[0].wal_range.is_some(),
            "{signal}: the adopted block must report its WAL span"
        );
        parqtel_core::storage::advance_wal_commit(&data_dir, signal, &index);
        assert_eq!(
            replay_count(),
            0,
            "{signal}: rows already in a block must not be replayed"
        );
    }
}

/// A flush that never completes (it failed) holds back every later commit,
/// so replay still recovers its rows.
#[test]
fn test_wal_commit_holds_back_behind_a_failed_flush() {
    let mut f = FlushOrder::default();
    let (_failed, ok1, ok2) = (f.begin(), f.begin(), f.begin());
    assert_eq!(f.wal_complete(ok1, WalPosition(20)), None);
    assert_eq!(f.wal_complete(ok2, WalPosition(30)), None);
}

/// The service must still work with no contention sink attached, so an
/// embedder that does not build telemetry pays nothing.
/// The property the WAL exists for: a crash must lose nothing that was
/// acknowledged, and must not duplicate anything that was already
/// flushed.
///
/// Simulated by dropping the service without flushing (a crash) and then
/// replaying into a fresh one, which is exactly what startup does.
#[tokio::test]
async fn test_wal_recovers_acknowledged_but_unflushed_points() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let config = BlockConfig {
        data_dir: data_dir.clone(),
        max_rows_per_block: 10_000, // nothing flushes on the cap
        block_duration_secs: 3600,  // nor on the timer
        ..Default::default()
    };

    // First "process": accept points, acknowledge, then crash.
    {
        let (tx, rx) = mpsc::unbounded_channel();
        let _index = spawn_index_task(rx);
        let svc = IngestionService::with_shards(
            config.clone(),
            tx,
            2,
            Some(
                WalWriter::open_with_segment_limit(
                    &data_dir,
                    "metrics",
                    WalSyncMode::Always,
                    Duration::from_millis(0),
                    64 * 1024 * 1024,
                )
                .unwrap(),
            ),
        );
        for i in 0..5i64 {
            let n = svc
                .ingest_metrics(vec![metric_named(&format!("m{i}"), i)])
                .await
                .unwrap();
            assert_eq!(n, 1, "the request must be acknowledged");
        }
        // Dropped without shutdown(): no flush, no commit.
    }

    // The WAL must hold the acknowledged points.
    let commit = parqtel_core::wal::read_commit(&data_dir, "metrics");
    let mut recovered = Vec::new();
    // One record per metric: the rotator appends while it holds the shard
    // lock so WAL order and writer order are the same total order.
    let stats =
        parqtel_core::wal::replay::<Metric, _>(&data_dir, "metrics", commit, |m| recovered.push(m))
            .unwrap();
    for e in std::fs::read_dir(dir.path().join("wal").join("metrics"))
        .unwrap()
        .flatten()
    {
        eprintln!(
            "  file {:?} size {:?}",
            e.path(),
            e.metadata().map(|m| m.len())
        );
    }
    assert_eq!(stats.records, 5, "every acknowledged batch must replay");
    assert_eq!(stats.skipped, 0);
    let names: Vec<String> = recovered.iter().map(|m| m.name.clone()).collect();
    assert_eq!(names, vec!["m0", "m1", "m2", "m3", "m4"]);
}

/// After a successful flush the WAL must be committed, so a replay finds
/// nothing to redo. Without this every restart would re-ingest every block
/// ever written.
#[tokio::test]
async fn test_wal_is_committed_once_the_block_is_durable() {
    let dir = tempdir().unwrap();
    let data_dir = dir.path().to_path_buf();
    let config = BlockConfig {
        data_dir: data_dir.clone(),
        max_rows_per_block: 10_000,
        block_duration_secs: 3600,
        ..Default::default()
    };
    let (tx, rx) = mpsc::unbounded_channel();
    let _index = spawn_index_task(rx);
    let svc = IngestionService::with_shards(
        config.clone(),
        tx,
        2,
        Some(
            WalWriter::open_with_segment_limit(
                &data_dir,
                "metrics",
                WalSyncMode::Always,
                Duration::from_millis(0),
                64 * 1024 * 1024,
            )
            .unwrap(),
        ),
    );

    for i in 0..4i64 {
        svc.ingest_metrics(vec![metric_named(&format!("m{i}"), i)])
            .await
            .unwrap();
    }
    svc.shutdown().await.unwrap();
    // The index task consumed the metadata, so the durable proof is that
    // the WAL is committed: the block is on disk and covered.
    assert!(
        wal_files(&data_dir) >= 1,
        "the flush must have written a block"
    );

    let commit = parqtel_core::wal::read_commit(&data_dir, "metrics");
    let mut seen = 0u64;
    let stats = parqtel_core::wal::replay::<Metric, _>(&data_dir, "metrics", commit, |_| seen += 1)
        .unwrap();
    assert_eq!(seen, 0, "flushed rows must not be replayed");
    assert!(stats.records == 0);
    assert!(stats.skipped > 0, "they must be counted as skipped");
}

/// Logs and traces get the same crash-recovery contract as metrics: a
/// process death must lose nothing acknowledged, and a graceful flush must
/// commit the WAL so a restart does not re-ingest it.
#[tokio::test]
async fn test_log_and_trace_wal_recover_and_commit() {
    for signal in ["logs", "traces"] {
        let dir = tempdir().unwrap();
        // Logs live in their own data dir; trace blocks share the metrics
        // one, because TraceWriter is built from config.storage.
        let data_dir = if signal == "logs" {
            dir.path().join("logs")
        } else {
            dir.path().to_path_buf()
        };
        std::fs::create_dir_all(&data_dir).unwrap();
        // Each service owns the WAL of its own signal.
        let open = || {
            WalWriter::open_with_segment_limit(
                &data_dir,
                signal,
                WalSyncMode::Always,
                Duration::from_millis(0),
                64 * 1024 * 1024,
            )
            .unwrap()
        };
        let log_config = LogBlockConfig {
            data_dir: data_dir.clone(),
            max_rows_per_block: 10_000,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let block_config = BlockConfig {
            data_dir: data_dir.clone(),
            max_rows_per_block: 10_000,
            block_duration_secs: 3600,
            ..Default::default()
        };
        let log_at = |ts: i64, body: &str| {
            parqtel_core::LogRecord::new(
                ts,
                ts,
                9,
                "INFO".into(),
                body.into(),
                parqtel_core::LabelSet::default(),
                parqtel_core::LabelSet::default(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        };
        let span_at = |i: i64| {
            let mut sp = test_span(i);
            sp.start_time_ns = i;
            sp.end_time_ns = i + 99;
            sp
        };

        // Crash: accept, acknowledge, drop without flushing.
        {
            let (tx, rx) = mpsc::unbounded_channel();
            let _index = spawn_index_task(rx);
            if signal == "logs" {
                let svc = LogIngestionService::with_wal(log_config.clone(), tx, Some(open()));
                for i in 0..3i64 {
                    let l = log_at(i + 1, &format!("log-{i}"));
                    assert_eq!(svc.ingest_logs(vec![l]).await.unwrap(), 1);
                }
            } else {
                let svc = TraceIngestionService::with_wal(block_config.clone(), tx, Some(open()));
                for i in 0..3i64 {
                    assert_eq!(svc.ingest_spans(vec![span_at(i + 1)]).await.unwrap(), 1);
                }
            }
        }

        let commit = parqtel_core::wal::read_commit(&data_dir, signal);
        if signal == "logs" {
            let mut recovered = Vec::new();
            let stats = parqtel_core::wal::replay::<parqtel_core::LogRecord, _>(
                &data_dir,
                signal,
                commit,
                |r| recovered.push(r),
            )
            .unwrap();
            assert_eq!(stats.records, 3, "logs: every acknowledged log must replay");
            assert_eq!(
                recovered.iter().map(|l| l.body.clone()).collect::<Vec<_>>(),
                vec!["log-0", "log-1", "log-2"]
            );
        } else {
            let mut recovered = Vec::new();
            let stats = parqtel_core::wal::replay::<parqtel_core::Span, _>(
                &data_dir,
                signal,
                commit,
                |r| recovered.push(r),
            )
            .unwrap();
            assert_eq!(
                stats.records, 3,
                "traces: every acknowledged span must replay"
            );
            assert_eq!(recovered.len(), 3);
        }

        // Graceful flush commits, so a second replay finds nothing.
        let (tx, rx) = mpsc::unbounded_channel();
        let _index = spawn_index_task(rx);
        // Ingest again so the writer is non-empty and `shutdown` really
        // flushes - an empty writer short-circuits before the commit, which
        // would leave the earlier records uncommitted and make this phase
        // assert the wrong thing.
        if signal == "logs" {
            let svc = LogIngestionService::with_wal(log_config.clone(), tx, Some(open()));
            svc.ingest_logs(vec![log_at(99, "log-flushed")])
                .await
                .unwrap();
            svc.shutdown().await.unwrap();
        } else {
            let svc = TraceIngestionService::with_wal(block_config.clone(), tx, Some(open()));
            svc.ingest_spans(vec![span_at(99)]).await.unwrap();
            svc.shutdown().await.unwrap();
        }

        let commit = parqtel_core::wal::read_commit(&data_dir, signal);
        let after =
            parqtel_core::wal::replay::<serde_json::Value, _>(&data_dir, signal, commit, |_| {})
                .unwrap();
        assert_eq!(
            after.records, 0,
            "{signal}: flushed rows must not be replayed again"
        );
    }
}
