//! Block index: persistence, queries and reconciliation with the data directory.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// Writes one real block per signal, for the reconcile tests.
///
/// Uses the **production** filenames (`{start}_{end}_{uuid}.parquet` for
/// metrics, `logs_`/`traces_` prefixed otherwise), because reconcile uses
/// that prefix as a cheap signal hint before reading a footer.
fn write_one_of_each(dir: &std::path::Path) {
    let ts = 1000i64;
    write_metrics_parquet(
        &dir.join("1000_1001_metrics.parquet"),
        &[Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(ts, MetricValue::Double(1.0), LabelSet::default()).unwrap(),
                DataPoint::new(ts + 1, MetricValue::Double(2.0), LabelSet::default()).unwrap(),
            ],
            ..Default::default()
        }],
    );
    write_logs_parquet(
        &dir.join("logs_1000_1001_logs.parquet"),
        &[LogRecord::new(
            ts,
            ts,
            9,
            "INFO".into(),
            "hello".into(),
            LabelSet::default(),
            LabelSet::default(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        )],
    );
    write_traces_parquet(
        &dir.join("traces_1000_1050_traces.parquet"),
        &[test_span(ts)],
    );
}

/// The failure this exists to prevent: the sidecar is lost while the blocks
/// remain. Every query would silently return nothing.
#[test]
fn reconcile_adopts_blocks_the_index_never_heard_of() {
    let dir = tempdir().unwrap();
    write_one_of_each(dir.path());

    // An index that knows about none of them, as if index.json were lost.
    let mut index = BlockIndex::new(dir.path());
    assert_eq!(index.total_blocks(), 0);

    let stats = reconcile(&mut index, SignalType::Metrics).unwrap();
    assert_eq!(stats.adopted, 1, "the metrics block must be adopted");
    assert_eq!(stats.dropped, 0);
    assert_eq!(index.total_blocks(), 1);

    let meta = &index.blocks[0];
    assert_eq!(meta.row_count, 2, "row count comes from the footer");
    assert_eq!(meta.signal_type, SignalType::Metrics);
    assert!(meta.size_bytes > 0);
    assert!(
        meta.start_timestamp_ns <= meta.end_timestamp_ns,
        "timestamps recovered from the timestamp column's statistics"
    );
    assert!(
        meta.metric_names.is_empty(),
        "a recovered block must not claim a name set it cannot know; an \
         empty set means 'unknown' and keeps the block visible. Got {:?}",
        meta.metric_names
    );
}

/// Signals share a data directory, so reconciliation must not adopt another
/// signal's blocks.
#[test]
fn reconcile_only_adopts_its_own_signal() {
    let dir = tempdir().unwrap();
    write_one_of_each(dir.path());
    for (signal, expect) in [
        (SignalType::Metrics, 1usize),
        (SignalType::Logs, 1),
        (SignalType::Traces, 1),
    ] {
        let mut index = BlockIndex::new(dir.path());
        let stats = reconcile(&mut index, signal).unwrap();
        assert_eq!(stats.adopted, expect, "wrong adoption for {signal:?}");
        assert_eq!(index.total_blocks(), expect);
        assert_eq!(index.blocks[0].signal_type, signal);
    }
}

/// Entries whose file has gone must be dropped, or queries will try to open
/// files that no longer exist.
#[test]
fn reconcile_drops_entries_whose_file_vanished() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("metrics.parquet");
    write_metrics_parquet(
        &path,
        &[Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![
                DataPoint::new(1000, MetricValue::Double(1.0), LabelSet::default()).unwrap(),
            ],
            ..Default::default()
        }],
    );
    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: path.clone(),
        start_timestamp_ns: 1000,
        end_timestamp_ns: 1000,
        row_count: 1,
        size_bytes: 0,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    assert_eq!(index.total_blocks(), 1);

    fs::remove_file(&path).unwrap();
    let stats = reconcile(&mut index, SignalType::Metrics).unwrap();
    assert_eq!(stats.dropped, 1);
    assert_eq!(index.total_blocks(), 0);
}

/// The normal case must cost one read_dir and no footer parsing.
#[test]
fn reconcile_is_a_noop_when_the_index_already_agrees() {
    let dir = tempdir().unwrap();
    write_one_of_each(dir.path());
    let mut index = BlockIndex::new(dir.path());
    reconcile(&mut index, SignalType::Metrics).unwrap();
    let before = index.blocks.clone();

    let stats = reconcile(&mut index, SignalType::Metrics).unwrap();
    assert_eq!(stats.adopted, 0);
    assert_eq!(stats.dropped, 0);
    assert_eq!(
        stats.scanned, 0,
        "no footer should be read when the index already agrees"
    );
    assert_eq!(index.blocks.len(), before.len());
}

/// Adopted blocks must be queryable, which means the sorted-by-start
/// invariant `query` binary-searches on has to hold afterwards.
#[test]
fn reconcile_restores_the_sorted_invariant() {
    let dir = tempdir().unwrap();
    for i in 0..4i64 {
        let ts = 1000 + i * 100;
        write_metrics_parquet(
            &dir.path().join(format!("b{i}.parquet")),
            &[Metric {
                name: format!("cpu{i}"),
                kind: MetricKind::Gauge,
                data_points: vec![DataPoint::new(
                    ts,
                    MetricValue::Double(1.0),
                    LabelSet::default(),
                )
                .unwrap()],
                ..Default::default()
            }],
        );
    }
    let mut index = BlockIndex::new(dir.path());
    let stats = reconcile(&mut index, SignalType::Metrics).unwrap();
    assert_eq!(stats.adopted, 4);

    // A recovered block has no name set, so a name query must still find
    // it - that is the whole point of treating empty as "unknown".
    assert_eq!(
        index.query(0, i64::MAX, Some("cpu2")).len(),
        4,
        "a name query must still match blocks whose metric names are unknown"
    );
    let starts: Vec<i64> = index.blocks.iter().map(|b| b.start_timestamp_ns).collect();
    let mut sorted = starts.clone();
    sorted.sort_unstable();
    assert_eq!(starts, sorted, "query relies on this ordering");
    // And a range query over the whole span finds them all.
    assert_eq!(index.query(0, i64::MAX, None).len(), 4);
}

#[tokio::test]
async fn test_index_persistence() {
    let dir = tempdir().unwrap();
    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: dir.path().join("b1.parquet"),
        start_timestamp_ns: 100,
        end_timestamp_ns: 200,
        row_count: 10,
        size_bytes: 100,
        metric_names: HashSet::from(["m1".into()]),
        label_names: HashSet::from(["l1".into()]),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });

    // `add` is in-memory only; persistence is the store's job.
    let store = BlockIndexStore::new(&index);
    store.mark_dirty();
    persist_blocking(&index, &store).unwrap();

    let mut index2 = BlockIndex::new(dir.path());
    index2.load().unwrap();
    assert_eq!(index2.total_blocks(), 1);
}

#[tokio::test]
async fn test_index_query_time_range() {
    let dir = tempdir().unwrap();
    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: dir.path().join("b1.parquet"),
        start_timestamp_ns: 100,
        end_timestamp_ns: 200,
        row_count: 10,
        size_bytes: 100,
        metric_names: HashSet::from(["m1".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    index.add(BlockMetadata {
        path: dir.path().join("b2.parquet"),
        start_timestamp_ns: 300,
        end_timestamp_ns: 400,
        row_count: 10,
        size_bytes: 100,
        metric_names: HashSet::from(["m2".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });

    assert_eq!(index.query(150, 250, None).len(), 1);
    assert_eq!(index.query(0, 500, None).len(), 2);
    assert_eq!(index.query(0, 500, Some("m1")).len(), 1);
}

#[tokio::test]
async fn test_index_remove() {
    let dir = tempdir().unwrap();
    let mut index = BlockIndex::new(dir.path());
    let path = dir.path().join("b1.parquet");
    index.add(BlockMetadata {
        path: path.clone(),
        start_timestamp_ns: 100,
        end_timestamp_ns: 200,
        row_count: 10,
        size_bytes: 100,
        metric_names: HashSet::from(["m1".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    assert_eq!(index.total_blocks(), 1);
    index.remove(&path);
    assert_eq!(index.total_blocks(), 0);
}

#[tokio::test]
async fn test_index_stats() {
    let dir = tempdir().unwrap();
    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: dir.path().join("b1.parquet"),
        start_timestamp_ns: 100,
        end_timestamp_ns: 200,
        row_count: 10,
        size_bytes: 500,
        metric_names: HashSet::from(["m1".into(), "m2".into()]),
        label_names: HashSet::from(["env".into()]),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    assert_eq!(index.total_rows(), 10);
    assert_eq!(index.total_bytes(), 500);
    assert_eq!(index.all_metrics().len(), 2);
    assert_eq!(index.all_labels().len(), 1);
}
