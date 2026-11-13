//! Row-group pruning by time, metric name and service.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn builder_for(
    path: &std::path::Path,
) -> parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder<fs::File> {
    let file = fs::File::open(path).unwrap();
    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap()
}

/// `count` metrics, each with `points` points, written consecutively so that
/// row group `i` holds exactly metric `i`.
fn multi_metric_block(count: usize, points: usize) -> Vec<Metric> {
    (0..count)
        .map(|m| Metric {
            name: format!("metric_{m}"),
            kind: MetricKind::Gauge,
            data_points: (0..points)
                .map(|i| {
                    DataPoint::new(
                        i as i64 + 1,
                        MetricValue::Double(m as f64),
                        LabelSet::try_from_iter(vec![("host", format!("h{}", i % 10))]).unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

/// One metric, `services` services, one row group per service.
///
/// Uses the dotted OTLP resource key `service.name`, which is what
/// `extract_correlation_labels` reads; the underscored spelling silently
/// produces an all-null `service_name` column.
fn multi_service_block(services: usize, points: usize) -> Vec<Metric> {
    (0..services)
        .map(|s| Metric {
            name: "http_requests".into(),
            kind: MetricKind::Sum,
            resource_attributes: LabelSet::try_from_iter(vec![
                ("service.name", format!("svc-{s}")),
                ("cluster", "prod".to_string()),
            ])
            .unwrap(),
            data_points: (0..points)
                .map(|i| {
                    DataPoint::new(
                        i as i64 * 1_000_000_000 + 1,
                        MetricValue::Double(i as f64),
                        LabelSet::try_from_iter(vec![("route", "r0".to_string())]).unwrap(),
                    )
                    .unwrap()
                })
                .collect(),
            ..Default::default()
        })
        .collect()
}

/// A `{service.name="svc-3"}` selector must narrow a block to the one row
/// group that holds that service.
///
/// `service_name` statistics are exact per row group because rows are
/// written grouped by (metric, resource) — so this is an exact prune, not a
/// probabilistic one.
#[test]
fn service_pruning_narrows_to_the_matching_row_group() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("service.parquet");
    const SERVICES: usize = 8;
    const POINTS: usize = 100;
    write_block_with(
        &path,
        &multi_service_block(SERVICES, POINTS),
        POINTS,
        true,
        true,
    );

    let builder = builder_for(&path);
    assert_eq!(
        builder.metadata().num_row_groups(),
        SERVICES,
        "one row group per service"
    );
    // The metric dimension alone cannot help: every row group is the same
    // metric.
    let by_metric = scanner::row_groups_matching_value(
        &builder,
        &[0, 1, 2, 3, 4, 5, 6, 7],
        "metric_name",
        "http_requests",
    );
    assert_eq!(
        by_metric.len(),
        SERVICES,
        "metric pruning cannot discriminate within one metric"
    );

    let kept = scanner::row_groups_matching_value(
        &builder,
        &[0, 1, 2, 3, 4, 5, 6, 7],
        "service_name",
        "svc-3",
    );
    assert_eq!(
        kept,
        vec![3],
        "service statistics should keep exactly the one matching row group"
    );
}

/// An absent service must prune to nothing rather than decode every
/// row group, and must never prune a row group that does hold the service.
#[test]
fn service_pruning_is_exact_in_both_directions() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("service_absent.parquet");
    write_block_with(&path, &multi_service_block(8, 100), 100, true, true);
    let builder = builder_for(&path);
    let all: Vec<usize> = (0..8).collect();

    for s in 0..8 {
        let kept =
            scanner::row_groups_matching_value(&builder, &all, "service_name", &format!("svc-{s}"));
        assert!(
            kept.contains(&s),
            "svc-{s} lives in group {s}, got {kept:?}"
        );
    }
    assert!(
        scanner::row_groups_matching_value(&builder, &all, "service_name", "svc-nope").is_empty(),
        "an absent service should prune every row group"
    );
}

/// A bloom filter may report a false positive but must NEVER report a false
/// negative. A false negative silently drops data from a query result.
///
/// Checked against which row group actually holds each metric rather than a
/// hard-coded expectation, so it cannot drift from the data.
#[test]
fn metric_pruning_never_drops_a_row_group_that_contains_the_metric() {
    for (label, stats, bloom) in [
        ("statistics only", true, false),
        ("statistics + bloom", true, true),
        ("bloom only (no statistics)", false, true),
    ] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("sound.parquet");
        const METRICS: usize = 10;
        const POINTS: usize = 100;
        write_block_with(
            &path,
            &multi_metric_block(METRICS, POINTS),
            POINTS,
            stats,
            bloom,
        );

        let builder = builder_for(&path);
        assert_eq!(
            builder.metadata().num_row_groups(),
            METRICS,
            "{label}: one row group per metric"
        );
        let candidates: Vec<usize> = (0..METRICS).collect();
        for m in 0..METRICS {
            let name = format!("metric_{m}");
            let kept =
                scanner::row_groups_matching_metric(&builder, &candidates, "metric_name", &name);
            assert!(
                kept.contains(&m),
                "{label}: {name} lives in row group {m} but pruning returned {kept:?}"
            );
        }
    }
}

/// Statistics are the mechanism that does the real work, and they are free:
/// this is the headline result, so it is pinned separately.
#[test]
fn metric_statistics_prune_without_any_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stats_only.parquet");
    const METRICS: usize = 10;
    const POINTS: usize = 100;
    // No bloom filters at all.
    write_block_with(
        &path,
        &multi_metric_block(METRICS, POINTS),
        POINTS,
        true,
        false,
    );

    let builder = builder_for(&path);
    let candidates: Vec<usize> = (0..METRICS).collect();
    let kept =
        scanner::row_groups_matching_metric(&builder, &candidates, "metric_name", "metric_4");
    assert_eq!(
        kept,
        vec![4],
        "exact statistics should keep exactly the one row group, with no \
         filter written and nothing read"
    );
}

/// With neither statistics nor a filter, nothing can be concluded and every
/// row group must be kept. This is the fail-open rule and the reason old
/// blocks remain correct.
#[test]
fn metric_pruning_fails_open_with_neither_statistics_nor_filter() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("neither.parquet");
    write_block_with(&path, &multi_metric_block(4, 50), 50, false, false);

    let builder = builder_for(&path);
    let kept = scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "metric_name", "nope");
    assert_eq!(
        kept,
        vec![0, 1, 2, 3],
        "a block with neither statistics nor a filter must keep every row group"
    );
}

/// A query for a metric that is absent from the block prunes to nothing,
/// which is the case that would otherwise decode a whole file for an empty
/// result.
#[test]
fn metric_pruning_drops_everything_for_an_absent_metric() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("absent.parquet");
    write_block_with(&path, &multi_metric_block(4, 50), 50, true, true);

    let builder = builder_for(&path);
    let kept = scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "metric_name", "nope");
    assert!(
        kept.is_empty(),
        "an absent metric should prune every row group, kept {kept:?}"
    );
}

/// An empty metric name means "no metric filter" — several callers scan a
/// block for every series, `list_label_values` among them. Treating it as a
/// value to match would prune the entire block and silently return
/// nothing, since the empty string sorts before every real metric name.
#[test]
fn metric_pruning_treats_an_empty_name_as_no_filter() {
    for (stats, bloom) in [(true, true), (true, false), (false, true), (false, false)] {
        let dir = tempdir().unwrap();
        let path = dir.path().join("empty.parquet");
        write_block_with(&path, &multi_metric_block(4, 50), 50, stats, bloom);

        let builder = builder_for(&path);
        let kept = scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "metric_name", "");
        assert_eq!(
            kept,
            vec![0, 1, 2, 3],
            "an empty metric name must keep every row group (stats={stats}, bloom={bloom})"
        );
    }
}

/// A column that is not in the schema must also fail open.
#[test]
fn metric_pruning_fails_open_for_an_unknown_column() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nocol.parquet");
    write_block_with(&path, &multi_metric_block(4, 50), 50, true, true);

    let builder = builder_for(&path);
    let kept =
        scanner::row_groups_matching_metric(&builder, &[0, 1, 2, 3], "not_a_column", "metric_1");
    assert_eq!(
        kept,
        vec![0, 1, 2, 3],
        "unknown column must keep everything"
    );
}

/// The pruning helper must select exactly the row groups that can hold a
/// row in range, and must fail open (never prune) when it cannot tell.
#[test]
fn test_row_groups_in_range_selects_only_overlapping_groups() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune.parquet");
    // 1000 rows over 10 row groups of 100, timestamps 1..=1000.
    write_metrics_parquet_rg(&path, &prune_test_metrics(1000), 100);

    let file = fs::File::open(&path).unwrap();
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let md = builder.metadata().clone();

    assert_eq!(md.num_row_groups(), 10, "test file should have 10 groups");

    // Rows 301..=400 live entirely in the 4th row group (0-based index 3).
    let groups = scanner::row_groups_in_range(&md, "timestamp_ns", 301, 400).unwrap();
    assert_eq!(groups, vec![3]);

    // A single row on a boundary must still keep its own group.
    assert_eq!(
        scanner::row_groups_in_range(&md, "timestamp_ns", 100, 100).unwrap(),
        vec![0]
    );
    assert_eq!(
        scanner::row_groups_in_range(&md, "timestamp_ns", 101, 101).unwrap(),
        vec![1]
    );

    // A window covering everything keeps every group.
    assert_eq!(
        scanner::row_groups_in_range(&md, "timestamp_ns", 0, i64::MAX)
            .unwrap()
            .len(),
        10
    );

    // A window past the end prunes everything.
    assert!(
        scanner::row_groups_in_range(&md, "timestamp_ns", 5_000, 6_000)
            .unwrap()
            .is_empty()
    );

    // Unknown column: caller must fall back to reading every row group.
    assert!(scanner::row_groups_in_range(&md, "not_a_column", 0, 10).is_none());
}

/// Pruning must not change results: a narrow scan has to return exactly the
/// rows a full scan filtered by hand would.
#[tokio::test]
async fn test_pruned_scan_matches_full_scan() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("prune_scan.parquet");
    write_metrics_parquet_rg(&path, &prune_test_metrics(1000), 100);

    let meta = BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: 1000,
        row_count: 1000,
        size_bytes: 0,
        metric_names: HashSet::from(["prune.cpu".into()]),
        label_names: HashSet::from(["host".into()]),
        label_values: Default::default(),
        signal_type: SignalType::Metrics,
        wal_range: None,
    };

    let all = Scanner::scan(vec![meta.clone()], "prune.cpu".into(), 0, i64::MAX, None)
        .await
        .unwrap();
    assert_eq!(all.len(), 1000);

    let expected: Vec<i64> = all
        .iter()
        .map(|p| p.timestamp_ns)
        .filter(|t| (301..=400).contains(t))
        .collect();

    // Picks row group 3 only, and must agree row for row.
    let pruned = Scanner::scan(vec![meta], "prune.cpu".into(), 301, 400, None)
        .await
        .unwrap();
    let got: Vec<i64> = pruned.iter().map(|p| p.timestamp_ns).collect();
    assert_eq!(got, expected);
    assert_eq!(got.len(), 100);
}
