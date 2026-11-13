//! Compaction: merged blocks keep their data and their indexes.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn trace_meta(path: &std::path::Path, spans: &[Span]) -> BlockMetadata {
    BlockMetadata {
        path: path.to_path_buf(),
        start_timestamp_ns: spans.iter().map(|s| s.start_time_ns).min().unwrap_or(0),
        end_timestamp_ns: spans.iter().map(|s| s.end_time_ns).max().unwrap_or(0),
        row_count: spans.len(),
        size_bytes: fs::metadata(path).map(|m| m.len()).unwrap_or(0),
        metric_names: HashSet::new(),
        label_names: HashSet::from(["http.status_code".to_string()]),
        label_values: BTreeMap::from([(
            "http.status_code".to_string(),
            BTreeSet::from(["200".to_string()]),
        )]),
        signal_type: SignalType::Traces,
        wal_range: None,
    }
}

/// Trace blocks used to be skipped by compaction entirely, so they never
/// merged and accumulated until retention deleted them.
#[tokio::test]
async fn test_compactor_merges_trace_blocks() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        compression: "uncompressed".into(),
        compaction_max_merge_blocks: 4,
        compaction_max_merges_per_pass: 4,
        ..Default::default()
    };
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));

    // Three small trace blocks, time-ordered.
    let mut index = BlockIndex::new(dir.path());
    for b in 0..3i64 {
        let spans: Vec<Span> = (0..3).map(|i| test_span(b * 10 + i + 1)).collect();
        let path = dir.path().join(format!("t{b}.parquet"));
        write_traces_parquet(&path, &spans);
        index.add(trace_meta(&path, &spans));
    }
    let index = Arc::new(RwLock::new(index));
    assert_eq!(index.read().await.total_blocks(), 3);

    Compactor::compact_once(&index, &store, &config)
        .await
        .unwrap();

    let after = index.read().await;
    assert_eq!(
        after.total_blocks(),
        1,
        "three small trace blocks should merge into one"
    );
    let merged = &after.blocks[0];
    assert_eq!(merged.signal_type, SignalType::Traces);
    assert_eq!(merged.row_count, 9, "every span must survive the merge");

    // And the merged block must still be readable.
    let spans = Scanner::scan_traces(vec![merged.clone()], 0, i64::MAX, 1000)
        .await
        .unwrap();
    assert_eq!(spans.len(), 9, "merged trace block must decode");
}

/// Compaction used to write an empty label-value dictionary, so merging
/// silently destroyed the flush-time index that label-value autocomplete
/// reads from metadata instead of decoding blocks. Every compaction pass
/// degraded it.
#[tokio::test]
async fn test_compaction_preserves_label_values() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        compression: "uncompressed".into(),
        ..Default::default()
    };
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));

    let mut index = BlockIndex::new(dir.path());
    for b in 0..2i64 {
        let m = Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![DataPoint::new(
                b + 1,
                MetricValue::Double(1.0),
                LabelSet::try_from_iter(vec![("host", format!("h{b}"))]).unwrap(),
            )
            .unwrap()],
            ..Default::default()
        };
        let path = dir.path().join(format!("m{b}.parquet"));
        write_metrics_parquet(&path, std::slice::from_ref(&m));
        let mut meta = BlockMetadata {
            path,
            start_timestamp_ns: b + 1,
            end_timestamp_ns: b + 1,
            row_count: 1,
            size_bytes: 0,
            metric_names: HashSet::from(["cpu".to_string()]),
            label_names: HashSet::from(["host".to_string()]),
            label_values: BTreeMap::from([("host".to_string(), BTreeSet::from([format!("h{b}")]))]),
            signal_type: SignalType::Metrics,
            wal_range: None,
        };
        meta.size_bytes = fs::metadata(&meta.path).map(|m| m.len()).unwrap_or(0);
        index.add(meta);
    }
    let index = Arc::new(RwLock::new(index));

    Compactor::compact_once(&index, &store, &config)
        .await
        .unwrap();

    let after = index.read().await;
    assert_eq!(after.total_blocks(), 1, "blocks should have merged");
    let hosts = &after.blocks[0].label_values["host"];
    assert_eq!(
        hosts.len(),
        2,
        "the merged block must carry both hosts' values, not an empty dictionary"
    );
    assert!(hosts.contains("h0") && hosts.contains("h1"));
}

#[tokio::test]
async fn test_compactor_merges_small_blocks() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        compression: "uncompressed".into(),
        ..Default::default()
    };

    let p1 = dir.path().join("b1.parquet");
    let p2 = dir.path().join("b2.parquet");
    let m1 = Metric {
        name: "cpu".into(),
        kind: MetricKind::Gauge,
        data_points: vec![
            DataPoint::new(1000, MetricValue::Double(10.0), LabelSet::default()).unwrap(),
        ],
        ..Default::default()
    };
    let m2 = Metric {
        name: "cpu".into(),
        kind: MetricKind::Gauge,
        data_points: vec![
            DataPoint::new(2000, MetricValue::Double(20.0), LabelSet::default()).unwrap(),
        ],
        ..Default::default()
    };
    write_metrics_parquet(&p1, &[m1]);
    write_metrics_parquet(&p2, &[m2]);

    let mut index = BlockIndex::new(dir.path());
    index.add(BlockMetadata {
        path: p1.clone(),
        start_timestamp_ns: 1000,
        end_timestamp_ns: 1000,
        row_count: 1,
        size_bytes: fs::metadata(&p1).unwrap().len(),
        metric_names: HashSet::from(["cpu".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });
    index.add(BlockMetadata {
        path: p2.clone(),
        start_timestamp_ns: 2000,
        end_timestamp_ns: 2000,
        row_count: 1,
        size_bytes: fs::metadata(&p2).unwrap().len(),
        metric_names: HashSet::from(["cpu".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    });

    let index = Arc::new(RwLock::new(index));
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
    Compactor::compact_once(&index, &store, &config)
        .await
        .unwrap();

    let idx = index.read().await;
    assert_eq!(idx.total_blocks(), 1);
    assert_eq!(idx.blocks[0].row_count, 2);
    assert!(!p1.exists());
    assert!(!p2.exists());
}

/// A merged block is laid out series by series and carries its own series
/// table, whatever the sources looked like, so compaction never turns a
/// block that can answer a selector cheaply into one that cannot.
#[tokio::test]
async fn test_compaction_writes_a_series_table_for_the_merged_block() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        compression: "uncompressed".into(),
        ..Default::default()
    };
    let host = |h: &str| LabelSet::try_from_iter(vec![("host", h)]).unwrap();
    let mut index = BlockIndex::new(dir.path());
    for (name, base) in [("b1.parquet", 100i64), ("b2.parquet", 110)] {
        let path = dir.path().join(name);
        // Both sources interleave the two hosts.
        let points: Vec<_> = (0..6)
            .map(|i| {
                DataPoint::new(
                    base + i as i64,
                    MetricValue::Double((base + i as i64) as f64),
                    host(if i % 2 == 0 { "h0" } else { "h1" }),
                )
                .unwrap()
            })
            .collect();
        write_metrics_parquet(
            &path,
            &[Metric {
                name: "cpu".into(),
                kind: MetricKind::Gauge,
                data_points: points,
                ..Default::default()
            }],
        );
        index.add(BlockMetadata {
            path: path.clone(),
            start_timestamp_ns: base,
            end_timestamp_ns: base + 5,
            row_count: 6,
            size_bytes: fs::metadata(&path).unwrap().len(),
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        });
    }
    let index = Arc::new(RwLock::new(index));
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
    Compactor::compact_once(&index, &store, &config)
        .await
        .unwrap();
    let merged = index.read().await.blocks[0].clone();
    assert_eq!(merged.row_count, 12);

    let file = fs::File::open(&merged.path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let text = builder
        .metadata()
        .file_metadata()
        .key_value_metadata()
        .and_then(|kv| kv.iter().find(|k| k.key == SERIES_INDEX_FOOTER_KEY))
        .and_then(|k| k.value.clone())
        .expect("merged block carries a series table");
    let table = SeriesIndex::from_table(&SeriesTable::from_footer_value(&text).unwrap()).unwrap();
    // One entry per series, not one per source: 2 hosts, 6 rows each.
    assert_eq!(table.series.len(), 2);
    assert!(table.series.iter().all(|s| s.rows == 6));

    let mut sel = SeriesSelector::all()
        .with_filter(Some(Arc::new(|l: &LabelSet| l.get("host") == Some("h1"))));
    sel.equals = vec![("host".into(), "h1".into())];
    let got = Scanner::scan_block_filtered(merged, "cpu", 0, i64::MAX, &sel).unwrap();
    assert_eq!(got.rows_considered, 6);
    let ts: Vec<i64> = got.points.iter().map(|p| p.timestamp_ns).collect();
    assert_eq!(
        ts,
        vec![101, 103, 105, 111, 113, 115],
        "time order inside a series"
    );
}

/// Compaction publishes the merged block in the in-memory index and marks
/// the store dirty; it must not write the sidecar itself, because that
/// write happens under a lock every query handler needs.
#[tokio::test]
async fn test_compaction_defers_sidecar_write_to_the_store() {
    let dir = tempdir().unwrap();
    let config = BlockConfig {
        data_dir: dir.path().to_path_buf(),
        compression: "uncompressed".into(),
        ..Default::default()
    };

    let mut index = BlockIndex::new(dir.path());
    for i in 0..2i64 {
        let path = dir.path().join(format!("b{i}.parquet"));
        let ts = 1000 + i * 1000;
        let m = Metric {
            name: "cpu".into(),
            kind: MetricKind::Gauge,
            data_points: vec![DataPoint::new(
                ts,
                MetricValue::Double(10.0 + i as f64),
                LabelSet::default(),
            )
            .unwrap()],
            ..Default::default()
        };
        write_metrics_parquet(&path, &[m]);
        index.add(BlockMetadata {
            path,
            start_timestamp_ns: ts,
            end_timestamp_ns: ts,
            row_count: 1,
            size_bytes: fs::metadata(dir.path().join(format!("b{i}.parquet")))
                .unwrap()
                .len(),
            metric_names: HashSet::from(["cpu".into()]),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Metrics,
            wal_range: None,
        });
    }

    let index = Arc::new(RwLock::new(index));
    let store = Arc::new(BlockIndexStore::at_path(dir.path().join("index.json")));
    Compactor::compact_once(&index, &store, &config)
        .await
        .unwrap();

    assert_eq!(index.read().await.total_blocks(), 1, "blocks merged");
    assert!(
        store.is_dirty(),
        "the merged index must be marked for persist"
    );
    assert!(
        !dir.path().join("index.json").exists(),
        "compaction must not persist the sidecar itself"
    );

    // The store then publishes it, without holding the write lock.
    persist_once(&index, &store).await;
    assert!(dir.path().join("index.json").exists());
}
