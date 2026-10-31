//! Tests for the `index` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::models::storage::SignalType;
use std::path::PathBuf;

fn meta(path: PathBuf, start: i64, end: i64, signal: SignalType) -> BlockMetadata {
    BlockMetadata {
        path,
        start_timestamp_ns: start,
        end_timestamp_ns: end,
        row_count: 1,
        size_bytes: 10,
        metric_names: HashSet::from(["m".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: signal,
        wal_range: None,
    }
}

/// `add` must not touch the filesystem: that is the whole point of the
/// split. Persistence is the store's job.
#[test]
fn add_does_not_write_the_sidecar() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    idx.add(meta(
        dir.path().join("a.parquet"),
        100,
        200,
        SignalType::Metrics,
    ));
    assert_eq!(idx.total_blocks(), 1);
    assert!(
        !dir.path().join("index.json").exists(),
        "add must not perform filesystem I/O"
    );
}

#[test]
fn serialize_load_round_trip_preserves_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    // Inserted out of order on purpose: `query` relies on the sorted
    // invariant, so the round trip must re-establish it.
    idx.add(meta(
        dir.path().join("c.parquet"),
        300,
        400,
        SignalType::Logs,
    ));
    idx.add(meta(
        dir.path().join("a.parquet"),
        100,
        200,
        SignalType::Metrics,
    ));
    idx.add(meta(
        dir.path().join("b.parquet"),
        200,
        300,
        SignalType::Traces,
    ));

    let payload = idx.serialize().unwrap();
    let mut restored = BlockIndex::new(dir.path());
    restored.load_serialized(&payload).unwrap();
    assert_eq!(restored.total_blocks(), 3);
    assert_eq!(restored.query(0, 1000, None).len(), 3);
    assert_eq!(
        restored.blocks[0].start_timestamp_ns, 100,
        "blocks must be sorted by start timestamp after a load"
    );
}

#[test]
fn blocks_by_signal_counts_each_signal() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    idx.add(meta(
        dir.path().join("a.parquet"),
        1,
        2,
        SignalType::Metrics,
    ));
    idx.add(meta(
        dir.path().join("b.parquet"),
        3,
        4,
        SignalType::Metrics,
    ));
    idx.add(meta(dir.path().join("c.parquet"), 5, 6, SignalType::Logs));
    let [metrics, logs, traces] = idx.blocks_by_signal();
    assert_eq!((metrics, logs, traces), (2, 1, 0));
}

/// `replace` must publish a compaction/retention change in one swap.
#[test]
fn replace_swaps_a_set_of_blocks_atomically() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    let p1 = dir.path().join("1.parquet");
    let p2 = dir.path().join("2.parquet");
    idx.add(meta(p1.clone(), 100, 200, SignalType::Metrics));
    idx.add(meta(p2.clone(), 300, 400, SignalType::Metrics));

    let merged = meta(dir.path().join("m.parquet"), 100, 400, SignalType::Metrics);
    idx.replace(&[p1.clone(), p2.clone()], Some(merged));

    assert_eq!(idx.total_blocks(), 1);
    assert!(!idx.blocks[0].path.exists(), "source paths must be gone");
    assert!(idx.blocks[0].path.to_string_lossy().contains("m.parquet"));
}

#[test]
fn replace_with_no_new_block_only_removes() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    let p1 = dir.path().join("1.parquet");
    idx.add(meta(p1.clone(), 100, 200, SignalType::Metrics));
    idx.add(meta(
        dir.path().join("2.parquet"),
        300,
        400,
        SignalType::Metrics,
    ));

    // Retention path: drop without inserting.
    idx.replace(&[p1], None);
    assert_eq!(idx.total_blocks(), 1);
    assert!(!idx.blocks[0].path.to_string_lossy().contains("1.parquet"));
}

#[test]
fn store_dirty_flag_lifecycle() {
    let dir = tempfile::tempdir().unwrap();
    let idx = BlockIndex::new(dir.path());
    let store = BlockIndexStore::new(&idx);

    assert!(!store.is_dirty());
    assert!(!store.begin(), "nothing to do on a clean store");

    store.mark_dirty();
    assert!(store.is_dirty());
    assert!(store.begin(), "a dirty store has work");
    assert!(
        !store.is_dirty(),
        "begin() must take the flag so a pass is not repeated"
    );

    store.complete("[]".to_string());
    assert!(store.has_persisted());
}

/// A mutation that lands *while* a payload is in flight must not be lost:
/// `complete` must not clear the flag that `mark_dirty` set after `begin`.
#[test]
fn mutation_during_inflight_pass_is_not_lost() {
    let dir = tempfile::tempdir().unwrap();
    let idx = BlockIndex::new(dir.path());
    let store = BlockIndexStore::new(&idx);

    store.mark_dirty();
    assert!(store.begin());
    // Simulates a flush arriving between begin() and complete().
    store.mark_dirty();
    store.complete("[]".to_string());
    assert!(
        store.is_dirty(),
        "a change made during the pass must schedule another one"
    );
}

#[test]
fn write_payload_creates_parent_and_is_readable() {
    let dir = tempfile::tempdir().unwrap();
    let nested = dir.path().join("a/b");
    let mut idx = BlockIndex::new(&nested);
    idx.add(meta(nested.join("1.parquet"), 1, 2, SignalType::Metrics));
    let payload = idx.serialize().unwrap();

    let store = BlockIndexStore::new(&idx);
    assert_eq!(store.size_bytes(), 0, "absent sidecar reports zero");
    store.write_payload(&payload).unwrap();
    assert!(nested.join("index.json").exists());
    assert_eq!(store.size_bytes() as usize, payload.len());

    let mut reloaded = BlockIndex::new(&nested);
    reloaded.load().unwrap();
    assert_eq!(reloaded.total_blocks(), 1, "payload must be loadable");
    assert!(
        !nested.join("index.tmp").exists(),
        "the tmp file must be renamed away, not left behind"
    );
}
