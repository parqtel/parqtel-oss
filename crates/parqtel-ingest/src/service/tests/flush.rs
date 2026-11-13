//! Background flushes: visibility, ordering and the flush worker.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// Number of Parquet block files a data dir holds.
fn block_file_count(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .map(|d| {
            d.flatten()
                .filter(|e| e.path().extension().is_some_and(|x| x == "parquet"))
                .count()
        })
        .unwrap_or(0)
}

/// The hazard the background worker introduces: between a flush being
/// *scheduled* and the block being *durable*, the rows live only in the
/// WAL. Draining the memory buffer during that window would leave them in
/// neither the buffer nor a block, and they would be invisible to queries.
///
/// Driven through the service, which is the path that populates both.
#[tokio::test]
async fn test_background_flush_keeps_rows_visible_throughout() {
    let dir = tempdir().unwrap();
    let buffer = parqtel_core::MemoryBuffer::new();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10_000,
        block_duration_secs: 3600,
        ..Default::default()
    };
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let svc = IngestionService::with_shards(config, tx, 2, None).with_memory_buffer(buffer.clone());
    svc.start_flush_worker(2);

    for i in 0..8i64 {
        svc.ingest_metrics(vec![metric_named(&format!("vis{i}"), i + 1)])
            .await
            .unwrap();
    }
    // Queryable before any flush: the rows are in the buffer.
    assert_eq!(buffer.stats().await.0, 8, "rows must be in the buffer");

    // Schedule a flush. It returns without waiting for the encode.
    svc.schedule_flush_for_test().await.unwrap();

    // Eventually the block is written, and only then is the buffer drained.
    // At no point may the two overlap, or rows would be read twice.
    let mut blocks = 0usize;
    for _ in 0..200 {
        blocks = block_file_count(dir.path()).max(blocks);
        if buffer.stats().await.0 == 0 && blocks > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(blocks > 0, "the worker must write the block");
    assert_eq!(
        buffer.stats().await.0,
        0,
        "the buffer must be drained once the block is durable"
    );
    assert_eq!(blocks, 1, "all 8 rows belong to a single block");
    svc.shutdown().await.unwrap();
}

/// Rows that arrive while a block is being encoded belong to the *next*
/// block. When the encode finishes, only the rows that went into it may leave
/// the memory buffer; draining everything would hide the newer rows from
/// queries until a later block is written.
#[tokio::test]
async fn test_background_flush_keeps_rows_that_arrived_during_the_encode() {
    let dir = tempdir().unwrap();
    let buffer = parqtel_core::MemoryBuffer::new();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 100_000,
        block_duration_secs: 3600,
        ..Default::default()
    };
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let wal = WalWriter::open(
        dir.path(),
        "metrics",
        WalSyncMode::None,
        Duration::from_secs(1),
    )
    .unwrap();
    let svc =
        IngestionService::with_shards(config, tx, 2, Some(wal)).with_memory_buffer(buffer.clone());
    svc.start_flush_worker(2);

    for i in 0..4i64 {
        svc.ingest_metrics(vec![metric_named(&format!("old{i}"), i + 1)])
            .await
            .unwrap();
    }
    // The swap happens before this returns; the encode finishes later.
    svc.schedule_flush_for_test().await.unwrap();
    for i in 0..3i64 {
        svc.ingest_metrics(vec![metric_named(&format!("new{i}"), 100 + i)])
            .await
            .unwrap();
    }

    for _ in 0..300 {
        if block_file_count(dir.path()) > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    // Give the post-encode drain time to run.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        block_file_count(dir.path()),
        1,
        "the first block was written"
    );
    let mut names = buffer.metric_names().await;
    names.sort();
    assert_eq!(
        names,
        vec!["new0", "new1", "new2"],
        "only the rows that were flushed may leave the buffer"
    );
    svc.shutdown().await.unwrap();
}

/// Logs: a block is written without holding the ingest path, and rows that
/// arrive during the encode stay queryable once it lands; only the block's own
/// rows leave the buffer.
#[tokio::test]
async fn test_log_flush_keeps_rows_that_arrived_during_the_encode() {
    let dir = tempdir().unwrap();
    let buffer = parqtel_core::MemoryBuffer::new();
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let svc = LogIngestionService::with_wal(
        LogBlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 100_000,
            block_duration_secs: 3600,
            ..Default::default()
        },
        tx,
        Some(
            WalWriter::open(
                dir.path(),
                "logs",
                WalSyncMode::None,
                Duration::from_secs(1),
            )
            .unwrap(),
        ),
    )
    .with_memory_buffer(buffer.clone());

    svc.ingest_logs((0..5).map(|i| log_at(i + 1, "old")).collect())
        .await
        .unwrap();
    // Starts the flush and returns; the encode runs in the background.
    {
        let mut rotator = svc.rotator.lock().await;
        rotator.start_flush(None).await;
    }
    svc.ingest_logs((0..3).map(|i| log_at(100 + i, "new")).collect())
        .await
        .unwrap();

    // Wait for the block in flight, not for the open one.
    svc.rotator.lock().await.wait_idle().await;

    let left = buffer.scan_logs(0, i64::MAX).await;
    let bodies: Vec<_> = left.iter().map(|l| l.body.clone()).collect();
    assert_eq!(
        bodies,
        vec!["new", "new", "new"],
        "only the flushed rows may leave the buffer"
    );
    assert!(
        block_file_count(dir.path()) >= 1,
        "the first block must be written"
    );
}

/// Spans take the same path as logs.
#[tokio::test]
async fn test_trace_flush_keeps_spans_that_arrived_during_the_encode() {
    let dir = tempdir().unwrap();
    let buffer = parqtel_core::MemoryBuffer::new();
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let svc = TraceIngestionService::with_wal(
        BlockConfig {
            data_dir: dir.path().to_path_buf(),
            max_rows_per_block: 100_000,
            block_duration_secs: 3600,
            ..Default::default()
        },
        tx,
        Some(
            WalWriter::open(
                dir.path(),
                "traces",
                WalSyncMode::None,
                Duration::from_secs(1),
            )
            .unwrap(),
        ),
    )
    .with_memory_buffer(buffer.clone());

    svc.ingest_spans((1..=4).map(test_span).collect())
        .await
        .unwrap();
    {
        let mut rotator = svc.rotator.lock().await;
        rotator.start_flush(None).await;
    }
    svc.ingest_spans((100..103).map(test_span).collect())
        .await
        .unwrap();
    svc.rotator.lock().await.wait_idle().await;

    let left = buffer.scan_spans(0, i64::MAX).await;
    assert_eq!(left.len(), 3, "only the newer spans remain buffered");
    assert!(left.iter().all(|s| s.start_time_ns >= 100));
    assert!(block_file_count(dir.path()) >= 1);
}

/// Buffer drops follow the same order: a later flush finishing first must
/// not remove rows before the earlier block holding the older ones landed.
#[test]
fn test_buffer_drops_are_released_in_flush_order() {
    let mut f = FlushOrder::default();
    let (a, b) = (f.begin(), f.begin());
    assert_eq!(f.drop_complete(b, vec![0, 4]), None);
    // Releasing the earlier flush frees both, merged element-wise.
    assert_eq!(f.drop_complete(a, vec![3, 2]), Some(vec![3, 4]));
    // A failed flush completes with nothing to drop, unblocking the rest.
    let (c, d) = (f.begin(), f.begin());
    assert_eq!(f.drop_complete(d, vec![5, 5]), None);
    assert_eq!(f.drop_complete(c, vec![0, 0]), Some(vec![5, 5]));
}

/// Without a WAL the worker must refuse to start.
///
/// Its contract is "the rows are already logged, so nothing is lost if this
/// process dies before the encode lands". With no WAL that is void, and
/// running it would silently acknowledge unrecoverable data. So the channel
/// stays unset and `flush_locked` takes the synchronous path - still
/// acknowledged only once the block is on disk.
#[tokio::test]
async fn test_flush_worker_refuses_to_start_without_a_wal() {
    let dir = tempdir().unwrap();
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let rotator = BlockRotator::with_shards(tiny_metrics_config(dir.path()), tx, 2);
    rotator.start_flush_worker(4);

    assert!(
        rotator.flush_tx.lock().unwrap().is_none(),
        "no worker may be started without a WAL"
    );

    // And a flush is therefore synchronous: the block exists by the time it
    // returns, with no waiting for a background task.
    rotator.push(metric_named("nowal", 1), None).await.unwrap();
    rotator.flush(None).await.unwrap();
    assert!(
        block_file_count(dir.path()) >= 1,
        "the flush must have written its block before returning"
    );
}

/// With a WAL the worker does start, and a flush is scheduled rather than
/// encoded inline.
#[tokio::test]
async fn test_flush_worker_starts_with_a_wal() {
    let dir = tempdir().unwrap();
    let (tx, rx) = mpsc::unbounded_channel::<PendingIndex>();
    let _index = spawn_index_task(rx);
    let wal = WalWriter::open_with_segment_limit(
        dir.path(),
        "metrics",
        WalSyncMode::Always,
        Duration::from_millis(0),
        64 * 1024 * 1024,
    )
    .unwrap();
    let _ = &wal;
    let rotator = BlockRotator::with_shards(tiny_metrics_config(dir.path()), tx, 2).with_wal(wal);
    rotator.start_flush_worker(4);
    assert!(
        rotator.flush_tx.lock().unwrap().is_some(),
        "the worker must start when a WAL is attached"
    );
    rotator.stop_flush_worker().await;
}

#[tokio::test]
async fn test_log_rotator_flush() {
    let dir = tempdir().unwrap();
    let config = LogBlockConfig {
        data_dir: dir.path().to_path_buf(),
        max_rows_per_block: 10,
        block_duration_secs: 1,
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::unbounded_channel();
    let mut rotator = LogRotator::new(config, tx);

    let log = parqtel_core::LogRecord::new(
        100,
        100,
        9,
        "INFO".into(),
        "test".into(),
        parqtel_core::LabelSet::default(),
        parqtel_core::LabelSet::default(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    );
    rotator.push(log, None).await.unwrap();

    rotator.flush(None).await.unwrap();
    let meta = rx.recv().await.unwrap().meta;
    assert_eq!(meta.row_count, 1);
}
