//! Tests for the `series_index` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn ls(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (*k, *v))).unwrap()
}

/// cpu: h0..h3 across two envs; mem: one series. Rows are 10 per series.
fn sample() -> SeriesIndex {
    let mut b = SeriesTableBuilder::new();
    b.begin_metric("cpu");
    let mut row = 0u64;
    for (i, (host, env)) in [
        ("h0", "prod"),
        ("h1", "prod"),
        ("h2", "stage"),
        ("h3", "stage"),
    ]
    .iter()
    .enumerate()
    {
        b.push_series(
            &ls(&[("host", host), ("env", env), ("service.name", "api")]),
            row,
            10,
            (i as i64) * 100,
            (i as i64) * 100 + 99,
        );
        row += 10;
    }
    b.begin_metric("mem");
    b.push_series(&ls(&[("host", "h0")]), row, 10, 0, 99);
    SeriesIndex::from_table(&b.finish()).unwrap()
}

fn pick(idx: &SeriesIndex, metric: &str, sel: &SeriesSelector) -> Vec<String> {
    idx.select(metric, sel, i64::MIN, i64::MAX)
        .into_iter()
        .map(|id| {
            idx.series[id as usize]
                .labels
                .get("host")
                .unwrap()
                .to_string()
        })
        .collect()
}

#[test]
fn table_round_trips_through_the_footer_text() {
    let mut b = SeriesTableBuilder::new();
    b.begin_metric("m");
    b.push_series(&ls(&[("a", "1")]), 0, 3, 5, 9);
    b.push_series(&ls(&[("a", "2")]), 3, 2, 1, 4);
    let table = b.finish();
    let text = table.to_footer_value().unwrap();
    assert_eq!(SeriesTable::from_footer_value(&text).unwrap(), table);
}

#[test]
fn corrupt_tables_are_rejected() {
    let mut b = SeriesTableBuilder::new();
    b.begin_metric("m");
    b.push_series(&ls(&[("a", "1")]), 0, 3, 5, 9);
    let good = b.finish();

    let mut t = good.clone();
    t.len.push(1);
    assert!(
        SeriesIndex::from_table(&t).is_err(),
        "array length mismatch"
    );
    let mut t = good.clone();
    t.labels[0][0] = 99;
    assert!(SeriesIndex::from_table(&t).is_err(), "id out of range");
    let mut t = good.clone();
    t.metrics[0].2 = 5;
    assert!(
        SeriesIndex::from_table(&t).is_err(),
        "runs overrun the series"
    );
    let mut t = good;
    t.v = 99;
    assert!(SeriesIndex::from_table(&t).is_err(), "unknown version");
    assert!(SeriesTable::from_footer_value("not json").is_err());
}

#[test]
fn equality_postings_narrow_and_scope_to_the_metric() {
    let idx = sample();
    let eq = |l: &str, v: &str| SeriesSelector {
        equals: vec![(l.into(), v.into())],
        ..Default::default()
    };
    assert_eq!(pick(&idx, "cpu", &eq("host", "h2")), vec!["h2"]);
    assert_eq!(pick(&idx, "cpu", &eq("env", "prod")), vec!["h0", "h1"]);
    // h0 also exists under `mem`, which must not leak into `cpu`.
    assert_eq!(pick(&idx, "mem", &eq("host", "h0")), vec!["h0"]);
    assert_eq!(pick(&idx, "cpu", &eq("host", "h9")), Vec::<String>::new());
    assert_eq!(pick(&idx, "cpu", &eq("nolabel", "x")), Vec::<String>::new());
    assert_eq!(
        pick(&idx, "absent", &SeriesSelector::all()),
        Vec::<String>::new()
    );
}

#[test]
fn multiple_equalities_intersect() {
    let idx = sample();
    let sel = SeriesSelector {
        equals: vec![
            ("env".into(), "stage".into()),
            ("service.name".into(), "api".into()),
        ],
        ..Default::default()
    };
    assert_eq!(pick(&idx, "cpu", &sel), vec!["h2", "h3"]);
    let none = SeriesSelector {
        equals: vec![("env".into(), "prod".into()), ("host".into(), "h3".into())],
        ..Default::default()
    };
    assert!(pick(&idx, "cpu", &none).is_empty());
}

#[test]
fn value_tests_union_matching_values_and_the_filter_has_the_last_word() {
    let idx = sample();
    let test: ValueTest = Arc::new(|v| v == "h1" || v == "h3");
    let sel = SeriesSelector {
        value_tests: vec![("host".into(), test)],
        ..Default::default()
    };
    assert_eq!(pick(&idx, "cpu", &sel), vec!["h1", "h3"]);

    // The filter can refuse what the postings let through.
    let filter: crate::storage::scanner::SeriesFilter =
        Arc::new(|l: &LabelSet| l.get("env") == Some("stage"));
    let sel = SeriesSelector {
        value_tests: vec![("host".into(), Arc::new(|v: &str| v != "h0"))],
        filter: Some(filter),
        ..Default::default()
    };
    assert_eq!(pick(&idx, "cpu", &sel), vec!["h2", "h3"]);
}

#[test]
fn series_outside_the_time_window_are_dropped() {
    let idx = sample();
    // h2 covers [200, 299].
    let ids = idx.select("cpu", &SeriesSelector::all(), 250, 260);
    assert_eq!(ids.len(), 1);
    assert_eq!(idx.series[ids[0] as usize].labels.get("host"), Some("h2"));
    assert!(idx
        .select("cpu", &SeriesSelector::all(), 1000, 2000)
        .is_empty());
}

#[test]
fn row_ranges_merge_adjacent_series() {
    let idx = sample();
    let ids = idx.select("cpu", &SeriesSelector::all(), i64::MIN, i64::MAX);
    assert_eq!(row_ranges(&idx, &ids), vec![(0, 40)]);
    // h0 and h2 are not adjacent.
    let two: Vec<u32> = vec![ids[0], ids[2]];
    assert_eq!(row_ranges(&idx, &two), vec![(0, 10), (20, 30)]);
    assert_eq!(idx.total_rows(), 50);
}

#[test]
fn the_same_series_in_two_blocks_shares_one_label_set() {
    let a = sample();
    let b = sample();
    assert!(a.series[0].labels.ptr_eq(&b.series[0].labels));
}

#[test]
fn restrict_and_intersect_helpers() {
    assert_eq!(
        restrict(&[1, 3, 5, 8, 9], &[(2, 6), (8, 10)]),
        vec![3, 5, 8, 9]
    );
    assert_eq!(restrict(&[1, 3], &[]), Vec::<u32>::new());
    assert_eq!(intersect(&[1, 2, 4, 7], &[2, 3, 7, 9]), vec![2, 7]);
    assert_eq!(intersect(&[], &[1]), Vec::<u32>::new());
}
