//! Reading blocks through their series table or log skip index.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use crate::models::storage::{MetricRun, SignalType};
use crate::storage::block_file::write_block_file;
use crate::{LabelSet, Metric, MetricKind, MetricValue};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::collections::HashSet;

const ROWS: usize = 12_000;
const RG: usize = 1_000;

fn metrics() -> Vec<Metric> {
    let svc = |name: &str| LabelSet::try_from_iter(vec![("service.name", name)]).unwrap();
    let mk = |service: &str, parity: usize| Metric {
        name: "scan.cpu".into(),
        kind: MetricKind::Gauge,
        resource_attributes: svc(service),
        // Deliberately not in time order: the writer has to sort.
        data_points: (0..ROWS)
            .rev()
            .filter(|i| i % 2 == parity)
            .map(|i| {
                DataPoint::new(
                    i as i64 + 1,
                    if i % 5 == 0 {
                        MetricValue::Int(i as i64)
                    } else {
                        MetricValue::Double(i as f64)
                    },
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
    let other = Metric {
        name: "scan.other".into(),
        kind: MetricKind::Gauge,
        data_points: vec![
            DataPoint::new(5, MetricValue::Double(1.0), LabelSet::default()).unwrap(),
        ],
        ..Default::default()
    };
    vec![mk("a", 0), mk("b", 1), other]
}

fn block_meta(path: std::path::PathBuf) -> BlockMetadata {
    BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: ROWS as i64,
        row_count: ROWS + 1,
        size_bytes: 0,
        metric_names: HashSet::from(["scan.cpu".into(), "scan.other".into()]),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    }
}

/// The same data twice: once with no series table (rows in arrival order),
/// once laid out series by series with a table, over many row groups.
fn pair(dir: &std::path::Path) -> (BlockMetadata, BlockMetadata) {
    let ms = metrics();

    let plain = dir.join("plain.parquet");
    let chunk = StorageModel::metrics_to_chunk(&ms).unwrap();
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(RG))
        .build();
    let mut w =
        ArrowWriter::try_new(File::create(&plain).unwrap(), chunk.schema(), Some(props)).unwrap();
    w.write(&chunk).unwrap();
    w.close().unwrap();

    let indexed = dir.join("indexed.parquet");
    let runs: Vec<MetricRun<'_>> = ms
        .iter()
        .map(|m| {
            let mut points: Vec<&DataPoint> = m.data_points.iter().collect();
            points.sort_by(|a, b| {
                a.labels
                    .fingerprint()
                    .cmp(&b.labels.fingerprint())
                    .then(a.timestamp_ns.cmp(&b.timestamp_ns))
            });
            MetricRun {
                name: &m.name,
                kind: m.kind,
                resource: &m.resource_attributes,
                points,
            }
        })
        .collect();
    let (batch, table) = StorageModel::metric_runs_to_block(&runs).unwrap();
    write_block_file(
        &indexed,
        batch,
        "zstd",
        None,
        RG,
        None,
        Some(crate::storage::FooterIndex::Series(&table)),
    )
    .unwrap();
    (block_meta(plain), block_meta(indexed))
}

fn canonical(mut points: Vec<DataPoint>) -> Vec<(i64, String, String)> {
    points.sort_by_key(|p| (p.timestamp_ns, p.labels.json_str().to_string()));
    points
        .iter()
        .map(|p| {
            (
                p.timestamp_ns,
                p.labels.json_str().to_string(),
                format!("{:?}", p.value),
            )
        })
        .collect()
}

fn selectors() -> Vec<(&'static str, SeriesSelector)> {
    let host5: SeriesFilter = Arc::new(|l: &LabelSet| l.get("host") == Some("h5"));
    let host5_prod_b: SeriesFilter = Arc::new(|l: &LabelSet| {
        l.get("host") == Some("h5")
            && l.get("env") == Some("prod")
            && l.get("service.name") == Some("b")
    });
    let h1x: SeriesFilter =
        Arc::new(|l: &LabelSet| l.get("host").is_some_and(|h| h.starts_with("h1")));
    let not_prod: SeriesFilter = Arc::new(|l: &LabelSet| l.get("env") != Some("prod"));
    let mut equals_two = SeriesSelector::all().with_filter(Some(host5_prod_b));
    equals_two.equals = vec![
        ("host".into(), "h5".into()),
        ("env".into(), "prod".into()),
        ("service.name".into(), "b".into()),
    ];
    let mut host_eq = SeriesSelector::all().with_filter(Some(host5.clone()));
    host_eq.equals = vec![("host".into(), "h5".into())];
    let mut regex = SeriesSelector::all().with_filter(Some(h1x));
    regex.value_tests = vec![(
        "host".into(),
        Arc::new(|v: &str| v.starts_with("h1")) as ValueTest,
    )];
    let mut missing = SeriesSelector::all()
        .with_filter(Some(Arc::new(|l: &LabelSet| l.get("host") == Some("nope"))));
    missing.equals = vec![("host".into(), "nope".into())];
    vec![
        ("everything", SeriesSelector::all()),
        (
            "service",
            // As the query layer builds it: the matcher has the last word.
            SeriesSelector::all()
                .with_service(Some("a".into()))
                .with_filter(Some(Arc::new(|l: &LabelSet| {
                    l.get("service.name") == Some("a")
                }))),
        ),
        ("host equality", host_eq),
        ("three equalities", equals_two),
        ("regex narrowing", regex),
        (
            "filter only (not-equal)",
            SeriesSelector::all().with_filter(Some(not_prod)),
        ),
        ("absent value", missing),
    ]
}

/// Whatever the selector and window, a block read through its series table
/// returns exactly the points the same data returns without one.
#[test]
fn indexed_scan_equals_unindexed_scan() {
    let dir = tempfile::tempdir().unwrap();
    let (plain, indexed) = pair(dir.path());
    assert!(open_block(&indexed.path).unwrap().unwrap().series.is_some());
    assert!(open_block(&plain.path).unwrap().unwrap().series.is_none());

    for (label, selector) in selectors() {
        for (start, end) in [
            (0, i64::MAX),
            (3_001, 4_000),
            (11_990, 20_000),
            (50_000, 60_000),
        ] {
            for metric in ["scan.cpu", "scan.other", "scan.absent", ""] {
                let want =
                    Scanner::scan_block_filtered(plain.clone(), metric, start, end, &selector)
                        .unwrap();
                let got =
                    Scanner::scan_block_filtered(indexed.clone(), metric, start, end, &selector)
                        .unwrap();
                assert_eq!(
                    canonical(got.points),
                    canonical(want.points),
                    "{label}: metric {metric:?}, window {start}..{end}"
                );
                // Reading less never means counting more.
                assert!(got.rows_considered <= want.rows_considered, "{label}");
            }
        }
    }
}

/// The point of the table: a selective query reads only the rows of the
/// series it selects.
#[test]
fn selective_scan_reads_only_the_selected_series() {
    let dir = tempfile::tempdir().unwrap();
    let (plain, indexed) = pair(dir.path());
    let (_, host_eq) = selectors().into_iter().nth(2).unwrap();
    let unindexed = Scanner::scan_block_filtered(plain, "scan.cpu", 0, i64::MAX, &host_eq).unwrap();
    let got = Scanner::scan_block_filtered(indexed, "scan.cpu", 0, i64::MAX, &host_eq).unwrap();
    assert_eq!(unindexed.rows_considered, ROWS as u64);
    assert_eq!(got.rows_considered, got.points.len() as u64);
    assert!(!got.points.is_empty() && got.points.len() < ROWS / 20);
}

/// A table the file does not agree with is ignored, not trusted.
#[test]
fn a_damaged_series_table_falls_back_to_a_full_scan() {
    let dir = tempfile::tempdir().unwrap();
    let (plain, _) = pair(dir.path());
    let ms = metrics();

    let write_with = |name: &str, value: String| {
        let path = dir.path().join(name);
        let chunk = StorageModel::metrics_to_chunk(&ms).unwrap();
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(RG))
            .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                SERIES_INDEX_FOOTER_KEY.to_string(),
                value,
            )]))
            .build();
        let mut w = ArrowWriter::try_new(File::create(&path).unwrap(), chunk.schema(), Some(props))
            .unwrap();
        w.write(&chunk).unwrap();
        w.close().unwrap();
        block_meta(path)
    };

    // Not JSON at all.
    let garbage = write_with("garbage.parquet", "{not json".to_string());
    // A well-formed table that describes a different, smaller file.
    let mut b = crate::storage::SeriesTableBuilder::new();
    b.begin_metric("scan.cpu");
    b.push_series(&LabelSet::default(), 0, 3, 1, 3);
    let wrong = write_with("wrong.parquet", b.finish().to_footer_value().unwrap());

    let selector = SeriesSelector::all();
    let want = canonical(
        Scanner::scan_block_filtered(plain, "scan.cpu", 0, i64::MAX, &selector)
            .unwrap()
            .points,
    );
    for bad in [garbage, wrong] {
        assert!(open_block(&bad.path).unwrap().unwrap().series.is_none());
        let got = Scanner::scan_block_filtered(bad, "scan.cpu", 0, i64::MAX, &selector).unwrap();
        assert_eq!(canonical(got.points), want);
    }
}

/// A log index that is not JSON, or that counts the wrong number of rows, is
/// ignored and the block still scans.
#[test]
fn a_damaged_log_index_is_ignored() {
    use crate::storage::log_index::{LogIndexBuilder, LOG_INDEX_FOOTER_KEY};
    let dir = tempfile::tempdir().unwrap();
    let logs: Vec<crate::LogRecord> = (0..10i64)
        .map(|i| {
            crate::LogRecord::new(
                i + 1,
                i + 1,
                9,
                "INFO".into(),
                format!("hello {i}"),
                LabelSet::default(),
                LabelSet::default(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        })
        .collect();
    let mut short = LogIndexBuilder::new();
    for l in &logs[..4] {
        short.push_row(
            l.timestamp_ns,
            l.severity_number,
            &l.body,
            &l.attributes,
            &l.resource_attributes,
        );
    }
    let wrong_rows = short.finish().to_footer_value().unwrap();

    for (name, value) in [("garbage", "{nope".to_string()), ("wrong", wrong_rows)] {
        let path = dir.path().join(format!("{name}.parquet"));
        let chunk = StorageModel::logs_to_chunk(&logs).unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![parquet::file::metadata::KeyValue::new(
                LOG_INDEX_FOOTER_KEY.to_string(),
                value,
            )]))
            .build();
        let mut w = ArrowWriter::try_new(File::create(&path).unwrap(), chunk.schema(), Some(props))
            .unwrap();
        w.write(&chunk).unwrap();
        w.close().unwrap();
        let opened = open_block(&path).unwrap().unwrap();
        assert!(opened.log_index.is_none(), "{name}");
        let meta = BlockMetadata {
            path,
            start_timestamp_ns: 1,
            end_timestamp_ns: 10,
            row_count: 10,
            size_bytes: 0,
            metric_names: HashSet::new(),
            label_names: HashSet::new(),
            label_values: Default::default(),
            signal_type: SignalType::Logs,
            wal_range: None,
        };
        let (got, stats, _) =
            Scanner::scan_log_block_filtered(meta, 0, i64::MAX, usize::MAX, false, None, None, 0)
                .unwrap();
        assert_eq!(got.len(), 10, "{name}");
        assert_eq!(stats, 10, "{name}");
    }
}

#[test]
fn row_selection_maps_file_rows_onto_the_groups_it_touches() {
    let dir = tempfile::tempdir().unwrap();
    let (_, indexed) = pair(dir.path());
    let opened = open_block(&indexed.path).unwrap().unwrap();
    let md = opened.meta.metadata();
    let groups = md.num_row_groups();
    assert!(groups > 5);
    // A range inside group 2 and one spanning groups 4 and 5.
    let ranges = [(2_100u64, 2_200u64), (4_900, 5_100)];
    let (picked, selection) = plan_row_selection(md, &ranges);
    assert_eq!(picked, vec![2, 4, 5]);
    let selected: usize = selection
        .iter()
        .filter(|s| !s.skip)
        .map(|s| s.row_count)
        .sum();
    assert_eq!(selected, 100 + 200);
    // 3 groups x 1000 rows, addressed relative to those groups.
    let total: usize = selection.iter().map(|s| s.row_count).sum();
    assert_eq!(total, 3 * RG);
}
