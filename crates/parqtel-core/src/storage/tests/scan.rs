//! Metric scans through the series table and the fallback path.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

/// A block big enough to span several read batches, with interleaved
/// series from two services, written with Parquet dictionary encoding on,
/// as production blocks are.
fn filtered_scan_block(dir: &std::path::Path, rows: usize) -> BlockMetadata {
    let path = dir.join("filtered.parquet");
    let svc = |name: &str| LabelSet::try_from_iter(vec![("service.name", name)]).unwrap();
    let mk = |service: &str, parity: usize| Metric {
        name: "scan.cpu".into(),
        kind: MetricKind::Gauge,
        resource_attributes: svc(service),
        data_points: (0..rows)
            .filter(|i| i % 2 == parity)
            .map(|i| {
                DataPoint::new(
                    i as i64 + 1,
                    MetricValue::Double(i as f64),
                    LabelSet::try_from_iter(vec![
                        ("host", format!("h{}", i % 37)),
                        ("env", if i % 3 == 0 { "prod" } else { "stage" }.to_string()),
                    ])
                    .unwrap(),
                )
                .unwrap()
            })
            .collect(),
        ..Default::default()
    };
    // Another metric in the same block that a scan for `scan.cpu` must skip.
    let other = Metric {
        name: "scan.other".into(),
        kind: MetricKind::Gauge,
        data_points: vec![
            DataPoint::new(5, MetricValue::Double(1.0), LabelSet::default()).unwrap(),
        ],
        ..Default::default()
    };
    write_metrics_parquet_rg(&path, &[mk("a", 0), mk("b", 1), other], 5_000);
    BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: rows as i64,
        row_count: rows + 1,
        size_bytes: 0,
        metric_names: HashSet::from(["scan.cpu".into(), "scan.other".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    }
}

#[tokio::test]
async fn test_scanner_missing_file() {
    let dir = tempdir().unwrap();
    let b1 = BlockMetadata {
        path: dir.path().join("missing.parquet"),
        start_timestamp_ns: 100,
        end_timestamp_ns: 200,
        row_count: 10,
        size_bytes: 100,
        metric_names: HashSet::from(["m1".into()]),
        label_names: HashSet::from(["l1".into()]),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    };
    let results = Scanner::scan(vec![b1], "m1".into(), 0, 300, None)
        .await
        .unwrap();
    assert!(results.is_empty());
}

/// Pushing a series predicate into the scan must give exactly the rows an
/// unfiltered scan gives for the matching series - same values, same labels
/// (including the merged-back `service.name`) - and must report the
/// unfiltered row count so "points scanned" keeps its meaning.
#[tokio::test]
async fn test_filtered_scan_equals_unfiltered_scan_then_filter() {
    let dir = tempdir().unwrap();
    let meta = filtered_scan_block(dir.path(), 30_000);

    let all = Scanner::scan(vec![meta.clone()], "scan.cpu".into(), 0, i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 30_000);
    assert!(all
        .iter()
        .all(|p| p.labels.get("service.name").is_some() && p.labels.get("host").is_some()));

    let pred: scanner::SeriesFilter =
        Arc::new(|l: &LabelSet| l.get("host") == Some("h5") && l.get("service.name") == Some("a"));
    let got = Scanner::scan_filtered(
        vec![meta.clone()],
        "scan.cpu".into(),
        0,
        i64::MAX,
        SeriesSelector::all().with_filter(Some(pred.clone())),
    )
    .await
    .unwrap();

    let mut expected: Vec<_> = all.iter().filter(|p| pred(&p.labels)).cloned().collect();
    let mut got_points = got.points;
    expected.sort_by_key(|p| p.timestamp_ns);
    got_points.sort_by_key(|p| p.timestamp_ns);
    assert!(!expected.is_empty() && expected.len() < all.len());
    assert_eq!(got_points, expected);
    assert_eq!(got.rows_considered, 30_000);

    // A predicate nothing satisfies yields no points but still counts rows.
    let none = Scanner::scan_filtered(
        vec![meta],
        "scan.cpu".into(),
        0,
        i64::MAX,
        SeriesSelector::all().with_filter(Some(Arc::new(|_: &LabelSet| false))),
    )
    .await
    .unwrap();
    assert!(none.points.is_empty());
    assert_eq!(none.rows_considered, 30_000);
}

/// The time window and the service prune apply before the series filter,
/// and `rows_considered` counts only rows that survive them.
#[tokio::test]
async fn test_filtered_scan_counts_rows_inside_window_and_service() {
    let dir = tempdir().unwrap();
    let meta = filtered_scan_block(dir.path(), 30_000);
    let got = Scanner::scan_filtered(
        vec![meta],
        "scan.cpu".into(),
        1_001,
        2_000,
        SeriesSelector::all().with_service(Some("a".into())),
    )
    .await
    .unwrap();
    // Service "a" holds the even offsets i, stored with timestamp i + 1,
    // so the odd timestamps in the window.
    assert_eq!(got.rows_considered, 500);
    assert_eq!(got.points.len(), 500);
    assert!(got
        .points
        .iter()
        .all(|p| p.labels.get("service.name") == Some("a")
            && (1_001..=2_000).contains(&p.timestamp_ns)));
}

#[tokio::test]
async fn test_scanner_reads_real_metrics() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("test.parquet");
    let m = Metric {
        name: "cpu".into(),
        kind: MetricKind::Gauge,
        data_points: vec![
            DataPoint::new(
                1000,
                MetricValue::Double(10.0),
                LabelSet::try_from_iter(vec![("host", "h1")]).unwrap(),
            )
            .unwrap(),
            DataPoint::new(
                2000,
                MetricValue::Double(20.0),
                LabelSet::try_from_iter(vec![("host", "h2")]).unwrap(),
            )
            .unwrap(),
        ],
        ..Default::default()
    };
    write_metrics_parquet(&path, &[m]);

    let meta = BlockMetadata {
        path,
        start_timestamp_ns: 1000,
        end_timestamp_ns: 2000,
        row_count: 2,
        size_bytes: 100,
        metric_names: HashSet::from(["cpu".into()]),
        label_names: HashSet::from(["host".into()]),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    };
    let points = Scanner::scan(vec![meta], "cpu".into(), 0, 3000, None)
        .await
        .unwrap();
    assert_eq!(points.len(), 2);
}
