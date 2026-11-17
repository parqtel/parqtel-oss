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

    store.complete(2);
    assert!(store.has_persisted());
    assert_eq!(store.payload_capacity_hint(), 2);
}

/// Serialising into a buffer sized from the last payload must not reallocate,
/// and must produce exactly the same bytes. Growing a 611 MB string by doubling
/// briefly held ~2x its size and pushed uat-oman over its 4 GiB limit.
#[test]
fn presized_serialization_matches_and_does_not_grow() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    for i in 0..50 {
        idx.add(meta(
            dir.path().join(format!("{i}.parquet")),
            i,
            i + 1,
            SignalType::Metrics,
        ));
    }
    let store = BlockIndexStore::new(&idx);
    let plain = idx.serialize().unwrap();
    store.complete(plain.len());

    let hint = store.payload_capacity_hint();
    let presized = idx.serialize_with_capacity(hint).unwrap();
    assert_eq!(presized, plain);
    assert_eq!(presized.capacity(), hint, "buffer must not have grown");
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
    store.complete(2);
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

fn with_values(mut m: BlockMetadata, field: &str, values: &[&str]) -> BlockMetadata {
    m.label_values.insert(
        field.to_string(),
        values.iter().map(|v| v.to_string()).collect(),
    );
    m
}

/// A legacy sidecar (bare array, values per block) loads into the shared
/// dictionary and is rewritten with each value stored once. Per-block copies
/// made uat-oman's sidecar 611 MB and every persist pass re-serialised them.
#[test]
fn legacy_sidecar_migrates_to_one_value_dictionary() {
    let dir = tempfile::tempdir().unwrap();
    let blocks: Vec<BlockMetadata> = (0..20)
        .map(|i| {
            with_values(
                meta(
                    dir.path().join(format!("{i}.parquet")),
                    i,
                    i + 1,
                    SignalType::Metrics,
                ),
                "pod",
                &["api-0", "api-1"],
            )
        })
        .collect();
    let legacy = serde_json::to_string(&blocks).unwrap();
    assert_eq!(legacy.matches("api-0").count(), 20);

    let mut idx = BlockIndex::new(dir.path());
    idx.load_serialized(&legacy).unwrap();
    assert_eq!(idx.blocks.len(), 20);
    assert!(idx.blocks.iter().all(|b| b.label_values.is_empty()));
    assert_eq!(idx.label_values("pod").unwrap().len(), 2);

    let current = idx.serialize().unwrap();
    assert_eq!(current.matches("api-0").count(), 0, "values moved out");
    let labels = idx.serialize_label_values().unwrap();
    assert_eq!(labels.matches("api-0").count(), 1, "value stored once");
    std::fs::write(idx.sidecar_path(), &current).unwrap();
    std::fs::write(idx.label_values_path(), &labels).unwrap();

    let mut reloaded = BlockIndex::new(dir.path());
    reloaded.load().unwrap();
    assert_eq!(reloaded.blocks.len(), 20);
    assert_eq!(reloaded.label_values("pod"), idx.label_values("pod"));
}

/// A full field evicts an old value so values from the newest block are
/// always offered; dropping new values would freeze autocomplete on churny
/// fields such as container IDs.
#[test]
fn full_field_keeps_newest_values() {
    let dir = tempfile::tempdir().unwrap();
    let mut idx = BlockIndex::new(dir.path());
    let old: Vec<String> = (0..10_000).map(|i| format!("a{i:05}")).collect();
    let old: Vec<&str> = old.iter().map(String::as_str).collect();
    idx.add(with_values(
        meta(dir.path().join("old.parquet"), 1, 2, SignalType::Metrics),
        "container_id",
        &old,
    ));
    idx.add(with_values(
        meta(dir.path().join("new.parquet"), 3, 4, SignalType::Metrics),
        "container_id",
        &["z-fresh"],
    ));
    let values = idx.label_values("container_id").unwrap();
    assert_eq!(values.len(), 10_000, "bounded");
    assert!(values.contains("z-fresh"), "newest value present");
}
