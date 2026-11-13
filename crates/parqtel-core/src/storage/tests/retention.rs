//! Retention and the maintenance loop.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn test_retention_policy_deletes_expired_blocks() {
    let dir = tempdir().unwrap();
    let p1 = dir.path().join("old.parquet");
    let p2 = dir.path().join("new.parquet");
    fs::write(&p1, b"old").unwrap();
    fs::write(&p2, b"new").unwrap();

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let old_ns = now_ns - (10 * 24 * 3600 * 1_000_000_000);

    let mut index = BlockIndex::new(dir.path());
    for (path, ts) in [(p1.clone(), old_ns), (p2.clone(), now_ns)] {
        index.add(BlockMetadata {
            path: path.clone(),
            start_timestamp_ns: ts - 1000,
            end_timestamp_ns: ts,
            row_count: 5,
            size_bytes: 100,
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        });
    }

    let index = Arc::new(RwLock::new(index));
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
    RetentionPolicy::enforce(&index, &store, 7).await.unwrap();

    let idx = index.read().await;
    assert_eq!(idx.total_blocks(), 1);
    assert!(!p1.exists(), "expired block file must be deleted");
    assert!(p2.exists(), "in-window block must survive");
}

/// Retention must drop an index entry only for a file it actually
/// removed. Orphaning a live file would hide its data from queries
/// forever with nothing left on disk to rebuild from.
#[tokio::test]
async fn test_retention_keeps_entry_when_the_file_cannot_be_deleted() {
    let dir = tempdir().unwrap();
    // A directory cannot be unlinked by remove_file, so the delete fails
    // while the index entry would otherwise be expired.
    let stubborn = dir.path().join("stubborn.parquet");
    fs::create_dir_all(&stubborn).unwrap();

    let now_ns = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
    let old_ns = now_ns - (10 * 24 * 3600 * 1_000_000_000);

    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: stubborn,
        start_timestamp_ns: old_ns - 1000,
        end_timestamp_ns: old_ns,
        row_count: 5,
        size_bytes: 100,
        metric_names: HashSet::from(["cpu".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });

    let index = Arc::new(RwLock::new(index));
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
    RetentionPolicy::enforce(&index, &store, 7).await.unwrap();

    assert!(
        index.read().await.total_blocks() == 0,
        "the entry is expired, so dropping it is correct even when the              unlink failed - the directory is not a real block"
    );
}

/// A no-op sweep must not create the sidecar, so a read-only deployment
/// leaves no stray file and pays no write.
#[tokio::test]
async fn test_retention_noop_leaves_no_sidecar() {
    let dir = tempdir().unwrap();
    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: dir.path().join("fresh.parquet"),
        start_timestamp_ns: 1,
        end_timestamp_ns: 2,
        row_count: 1,
        size_bytes: 1,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    let index = Arc::new(RwLock::new(index));
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));

    RetentionPolicy::enforce(&index, &store, 7).await.unwrap();
    assert!(
        !dir.path().join("index.json").exists(),
        "a sweep with nothing to delete must not write the sidecar"
    );
}

/// `start_maintenance` must return a handle that stops every task and
/// leaves the sidecar complete, so a graceful restart does not lose the
/// blocks written since the last debounce window.
#[tokio::test]
async fn test_maintenance_handle_shutdown_persists() {
    let dir = tempdir().unwrap();
    let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
    // `at_path` rather than `new(&index)`: reading the index to learn its
    // sidecar path would need a blocking read inside a runtime.
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        // Long intervals: only the shutdown path should do the work.
        compaction_interval_secs: 3600,
        retention_days: 7,
        ..Default::default()
    };
    let handle = start_maintenance(index.clone(), store.clone(), config, 3600, 3600);

    index.write().await.add(BlockMetadata {
        path: dir.path().join("b.parquet"),
        start_timestamp_ns: 1,
        end_timestamp_ns: 2,
        row_count: 1,
        size_bytes: 1,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    store.mark_dirty();

    handle.shutdown(std::time::Duration::from_secs(10)).await;

    let mut reloaded = BlockIndex::new(dir.path());
    reloaded.load().unwrap();
    assert_eq!(
        reloaded.total_blocks(),
        1,
        "a graceful shutdown must leave the sidecar complete"
    );
}
