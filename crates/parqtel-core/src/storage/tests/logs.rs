//! Log scans: column prefilter, two-phase scan and the skip index.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn test_scanner_logs_missing_file() {
    let dir = tempdir().unwrap();
    let b1 = BlockMetadata {
        path: dir.path().join("missing_logs.parquet"),
        start_timestamp_ns: 100,
        end_timestamp_ns: 200,
        row_count: 10,
        size_bytes: 100,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Logs,
        wal_range: None,
    };
    let results = Scanner::scan_logs(vec![b1], 0, 300).await.unwrap();
    assert!(results.is_empty());
}

/// Rejecting rows on the time, severity and body columns before decoding must
/// not change what a log scan returns, counts or volume buckets - only how
/// much gets decoded. The row filter still decides every row that passes.
#[tokio::test]
async fn test_log_prefilter_does_not_change_scan_results() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prefilter_logs.parquet");
    let logs: Vec<LogRecord> = (0..20_000i64)
        .map(|i| {
            LogRecord::new(
                i + 1,
                i + 1,
                [5, 9, 9, 13, 17][(i % 5) as usize],
                "X".into(),
                format!(
                    "Request {i} {} path=/a",
                    if i % 7 == 0 { "TIMEOUT upstream" } else { "ok" }
                ),
                LabelSet::try_from_iter(vec![("pod", format!("p{}", i % 11))]).unwrap(),
                LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        })
        .collect();
    write_logs_parquet(&path, &logs);
    let meta = BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: 20_000,
        row_count: logs.len(),
        size_bytes: 0,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Logs,
        wal_range: None,
    };

    // The row filter is the full predicate: severity, body, and a label test
    // the prefilter knows nothing about.
    let filter: LogRowFilter = Arc::new(|l: &LogRecord| {
        l.severity_number >= 13
            && l.body.to_ascii_lowercase().contains("timeout")
            && l.attributes.get("pod") != Some("p3")
    });
    let pre = scanner::LogColumnPrefilter {
        min_severity: Some(13),
        body_contains_lower: vec!["timeout".into()],
        exact: false,
        ..Default::default()
    };

    for (start, end, keep, newest) in [
        (0, i64::MAX, usize::MAX, false),
        (5_000, 15_000, 50, true),
        (5_000, 15_000, 50, false),
        (19_990, 40_000, 10, true),
    ] {
        let (plain, plain_stats) = Scanner::scan_logs_counted_filtered(
            vec![meta.clone()],
            start,
            end,
            keep,
            newest,
            Some(filter.clone()),
        )
        .await
        .unwrap();
        let (fast, fast_stats) = Scanner::scan_logs_counted_prefiltered(
            vec![meta.clone()],
            start,
            end,
            keep,
            newest,
            Some(filter.clone()),
            Some(pre.clone()),
        )
        .await
        .unwrap();
        let stamps = |v: &[LogRecord]| v.iter().map(|l| l.timestamp_ns).collect::<Vec<_>>();
        assert_eq!(
            stamps(&fast),
            stamps(&plain),
            "rows for window {start}..{end}"
        );
        assert_eq!(fast_stats.total_matched, plain_stats.total_matched);
        assert_eq!(fast_stats.volume_buckets, plain_stats.volume_buckets);
        assert!(plain_stats.total_matched > 0 || start == 19_990);
    }
}

/// When the prefilter is the whole predicate the scan counts matches from three
/// columns and decodes the rest only for the rows it returns. The answer -
/// the rows themselves, the count and the volume buckets - must be the one the
/// row-by-row scan gives, over several row groups and windows.
#[tokio::test]
async fn test_exact_log_prefilter_matches_row_by_row_scan() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("exact_logs.parquet");
    let logs: Vec<LogRecord> = (0..30_000i64)
        .map(|i| {
            LogRecord::new(
                i + 1,
                i + 1,
                [5, 9, 9, 13, 17][(i % 5) as usize],
                "X".into(),
                format!(
                    "Request {i} {} path=/a",
                    if i % 7 == 0 { "TIMEOUT upstream" } else { "ok" }
                ),
                LabelSet::try_from_iter(vec![("pod", format!("p{}", i % 11))]).unwrap(),
                LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
                [0u8; 16],
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        })
        .collect();
    // Small row groups, so selections cross group boundaries.
    let chunk = StorageModel::logs_to_chunk(&logs).unwrap();
    let mut w = ArrowWriter::try_new(
        fs::File::create(&path).unwrap(),
        chunk.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(4_000))
                .build(),
        ),
    )
    .unwrap();
    w.write(&chunk).unwrap();
    w.close().unwrap();
    let meta = BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: 30_000,
        row_count: logs.len(),
        size_bytes: 0,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Logs,
        wal_range: None,
    };
    let filter: LogRowFilter = Arc::new(|l: &LogRecord| {
        l.severity_number >= 13 && l.body.to_ascii_lowercase().contains("timeout")
    });
    let exact = scanner::LogColumnPrefilter {
        min_severity: Some(13),
        body_contains_lower: vec!["timeout".into()],
        exact: true,
        ..Default::default()
    };
    for (start, end, keep, newest) in [
        (0, i64::MAX, usize::MAX, false),
        (0, i64::MAX, 100, true),
        (0, i64::MAX, 100, false),
        (7_000, 21_000, 37, true),
        (7_000, 21_000, 37, false),
        (29_990, 40_000, 5, true),
        (50_000, 60_000, 5, true),
        (1, 30_000, 0, true),
    ] {
        let (plain, plain_stats) = Scanner::scan_logs_counted_filtered(
            vec![meta.clone()],
            start,
            end,
            keep,
            newest,
            Some(filter.clone()),
        )
        .await
        .unwrap();
        let (fast, fast_stats) = Scanner::scan_logs_counted_prefiltered(
            vec![meta.clone()],
            start,
            end,
            keep,
            newest,
            Some(filter.clone()),
            Some(exact.clone()),
        )
        .await
        .unwrap();
        assert_eq!(fast, plain, "rows for window {start}..{end} keep {keep}");
        assert_eq!(fast_stats.total_matched, plain_stats.total_matched);
        assert_eq!(fast_stats.volume_buckets, plain_stats.volume_buckets);
    }
}

/// A log block read through its skip index returns exactly what the same rows
/// return without one - rows, count and volume buckets - for rare, common and
/// absent needles, attribute equalities, severities and time windows, over
/// several row groups and granules.
#[tokio::test]
async fn test_log_skip_index_does_not_change_scan_results() {
    let dir = tempdir().unwrap();
    const N: i64 = 30_000;
    let logs: Vec<LogRecord> = (0..N)
        .map(|i| {
            let mut attrs = vec![("pod", format!("p{}", i % 11))];
            if (9_000..9_100).contains(&i) {
                attrs.push(("zone", "z-special".to_string()));
            }
            LogRecord::new(
                i + 1,
                i + 1,
                [5, 9, 9, 13, 17][(i % 5) as usize],
                "X".into(),
                if i == 21_345 {
                    "Request RareCase needle-xyz path=/a".to_string()
                } else {
                    format!(
                        "Request {i} {} path=/a",
                        if i % 7 == 0 { "TIMEOUT upstream" } else { "ok" }
                    )
                },
                LabelSet::try_from_iter(attrs).unwrap(),
                LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
                if (17_000..17_050).contains(&i) {
                    [0xab; 16]
                } else {
                    [0u8; 16]
                },
                [0u8; 8],
                0,
                "".into(),
                "".into(),
            )
        })
        .collect();
    let meta_for = |path: std::path::PathBuf| BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: N,
        row_count: logs.len(),
        size_bytes: 0,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: Default::default(),
        signal_type: SignalType::Logs,
        wal_range: None,
    };

    let plain_path = dir.path().join("plain.parquet");
    let chunk = StorageModel::logs_to_chunk(&logs).unwrap();
    let mut w = ArrowWriter::try_new(
        fs::File::create(&plain_path).unwrap(),
        chunk.schema(),
        Some(
            WriterProperties::builder()
                .set_max_row_group_row_count(Some(7_000))
                .build(),
        ),
    )
    .unwrap();
    w.write(&chunk).unwrap();
    w.close().unwrap();

    let indexed_path = dir.path().join("indexed.parquet");
    let (chunk, table) = StorageModel::logs_to_block(&logs).unwrap();
    write_block_file(
        &indexed_path,
        chunk,
        "zstd",
        None,
        7_000,
        None,
        Some(FooterIndex::Logs(&table)),
    )
    .unwrap();
    let (plain, indexed) = (meta_for(plain_path), meta_for(indexed_path));

    type Case = (&'static str, LogRowFilter, scanner::LogColumnPrefilter);
    let cases: Vec<Case> = vec![
        (
            "rare needle, exact",
            Arc::new(|l: &LogRecord| l.body.to_ascii_lowercase().contains("needle-xyz")),
            scanner::LogColumnPrefilter {
                body_contains_lower: vec!["needle-xyz".into()],
                exact: true,
                ..Default::default()
            },
        ),
        (
            "common needle and severity, exact",
            Arc::new(|l: &LogRecord| {
                l.severity_number >= 13 && l.body.to_ascii_lowercase().contains("timeout")
            }),
            scanner::LogColumnPrefilter {
                min_severity: Some(13),
                body_contains_lower: vec!["timeout".into()],
                exact: true,
                ..Default::default()
            },
        ),
        (
            "absent needle",
            Arc::new(|l: &LogRecord| l.body.contains("never-there")),
            scanner::LogColumnPrefilter {
                body_contains_lower: vec!["never-there".into()],
                exact: true,
                ..Default::default()
            },
        ),
        (
            "rare attribute",
            Arc::new(|l: &LogRecord| l.attributes.get("zone") == Some("z-special")),
            scanner::LogColumnPrefilter {
                attr_equals: vec![("zone".into(), "z-special".into())],
                ..Default::default()
            },
        ),
        (
            "resource attribute and needle",
            Arc::new(|l: &LogRecord| {
                l.resource_attributes.get("service.name") == Some("api")
                    && l.body.to_ascii_lowercase().contains("request 2")
            }),
            scanner::LogColumnPrefilter {
                body_contains_lower: vec!["request 2".into()],
                attr_equals: vec![("service.name".into(), "api".into())],
                ..Default::default()
            },
        ),
        (
            "trace id",
            Arc::new(|l: &LogRecord| l.trace_id == [0xab; 16]),
            scanner::LogColumnPrefilter {
                attr_equals: vec![("trace_id".into(), "ab".repeat(16))],
                ..Default::default()
            },
        ),
        (
            "attribute in a different granule than the needle",
            Arc::new(|l: &LogRecord| {
                l.attributes.get("zone") == Some("z-special")
                    && l.body.to_ascii_lowercase().contains("needle-xyz")
            }),
            scanner::LogColumnPrefilter {
                body_contains_lower: vec!["needle-xyz".into()],
                attr_equals: vec![("zone".into(), "z-special".into())],
                ..Default::default()
            },
        ),
    ];
    for (label, filter, pre) in cases {
        for (start, end, keep, newest) in [
            (0, i64::MAX, usize::MAX, false),
            (0, i64::MAX, 25, true),
            (0, i64::MAX, 25, false),
            (8_000, 22_000, 10, true),
            (21_000, 22_000, 10, false),
            (40_000, 50_000, 10, true),
        ] {
            let (want, want_stats) = Scanner::scan_logs_counted_filtered(
                vec![plain.clone()],
                start,
                end,
                keep,
                newest,
                Some(filter.clone()),
            )
            .await
            .unwrap();
            let (got, got_stats) = Scanner::scan_logs_counted_prefiltered(
                vec![indexed.clone()],
                start,
                end,
                keep,
                newest,
                Some(filter.clone()),
                Some(pre.clone()),
            )
            .await
            .unwrap();
            let ctx = format!("{label}, window {start}..{end}, keep {keep}, newest {newest}");
            assert_eq!(got, want, "rows: {ctx}");
            assert_eq!(got_stats.total_matched, want_stats.total_matched, "{ctx}");
            assert_eq!(got_stats.volume_buckets, want_stats.volume_buckets, "{ctx}");
        }
    }
}

#[tokio::test]
async fn test_scanner_reads_real_logs() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("logs.parquet");
    let log = LogRecord::new(
        1000,
        1000,
        9,
        "INFO".into(),
        "hello".into(),
        LabelSet::try_from_iter(vec![("k", "v")]).unwrap(),
        LabelSet::default(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    );
    write_logs_parquet(&path, &[log]);

    let meta = BlockMetadata {
        path,
        start_timestamp_ns: 1000,
        end_timestamp_ns: 1000,
        row_count: 1,
        size_bytes: 100,
        metric_names: HashSet::new(),
        label_names: HashSet::from(["k".into()]),
        label_values: Default::default(),
        signal_type: SignalType::Logs,
        wal_range: None,
    };
    let logs = Scanner::scan_logs(vec![meta], 0, 2000).await.unwrap();
    assert_eq!(logs.len(), 1);
    assert_eq!(logs[0].body, "hello");
}
