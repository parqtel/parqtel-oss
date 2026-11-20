//! Tests for the `series_index` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn ls(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (*k, *v))).unwrap()
}

/// cpu: h0..h3 across two envs; mem: one series. Rows are 10 per series.
fn sample_table() -> SeriesTable {
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
    b.finish()
}

fn sample() -> SeriesIndex {
    SeriesIndex::from_table(&sample_table()).unwrap()
}

fn pick(idx: &SeriesIndex, metric: &str, sel: &SeriesSelector) -> Vec<String> {
    idx.select(metric, sel, i64::MIN, i64::MAX)
        .into_iter()
        .map(|id| idx.labels(id).unwrap().get("host").unwrap().to_string())
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
fn footer_bytes_and_sample_accessors_are_unchanged() {
    let table = sample_table();
    let footer = table.to_footer_value().unwrap();
    let parsed = SeriesTable::from_footer_value(&footer).unwrap();
    assert_eq!(
        parsed.to_footer_value().unwrap().as_bytes(),
        footer.as_bytes()
    );

    for id in 0..table.string_offsets.len() - 1 {
        assert_eq!(parsed.string(id as u32), table.string(id as u32));
    }
    for series in 0..table.series_count() {
        assert_eq!(parsed.label_ids(series), table.label_ids(series));
        let expected = table.series_entry(series).unwrap();
        let actual = parsed.series_entry(series).unwrap();
        assert_eq!(actual.labels, expected.labels);
        assert_eq!(
            (actual.start, actual.rows, actual.min_ts, actual.max_ts),
            (
                expected.start,
                expected.rows,
                expected.min_ts,
                expected.max_ts
            )
        );
    }
}

#[test]
fn footer_serialization_keeps_legacy_field_bytes() {
    let mut builder = SeriesTableBuilder::new();
    builder.begin_metric("m");
    builder.push_series(&ls(&[("a", "1")]), 0, 3, 5, 9);
    builder.push_series(&ls(&[("a", "2")]), 3, 2, 1, 4);
    let table = builder.finish();
    let expected = r#"{"v":1,"strings":["m","a","1","2"],"metrics":[[0,0,2]],"start":[0,3],"len":[3,2],"min_ts":[5,1],"max_ts":[9,4],"labels":[[1,2],[1,3]]}"#;
    assert_eq!(
        table.to_footer_value().unwrap().as_bytes(),
        expected.as_bytes()
    );
    assert_eq!(
        SeriesTable::from_footer_value(expected)
            .unwrap()
            .to_footer_value()
            .unwrap()
            .as_bytes(),
        expected.as_bytes()
    );
}

#[test]
fn table_accessors_expose_strings_labels_counts_and_heap_size() {
    let table = sample_table();

    assert_eq!(table.string(0), Some("cpu"));
    assert_eq!(table.string(u32::MAX), None);

    let labels = table.label_ids(0).unwrap();
    let pairs: Vec<_> = labels
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            (
                table.string(pair[0]).unwrap(),
                table.string(pair[1]).unwrap(),
            )
        })
        .collect();
    assert_eq!(
        pairs,
        vec![("env", "prod"), ("host", "h0"), ("service.name", "api")]
    );
    assert_eq!(table.label_ids(5), None);
    assert_eq!(table.label_pair_count(), 13);

    let expected_heap_bytes = table
        .string_bytes
        .capacity()
        .saturating_add(
            table
                .string_offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            table
                .metrics
                .capacity()
                .saturating_mul(std::mem::size_of::<(u32, u32, u32)>()),
        )
        .saturating_add(
            table
                .start
                .capacity()
                .saturating_mul(std::mem::size_of::<u64>()),
        )
        .saturating_add(
            table
                .len
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            table
                .min_ts
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>()),
        )
        .saturating_add(
            table
                .max_ts
                .capacity()
                .saturating_mul(std::mem::size_of::<i64>()),
        )
        .saturating_add(
            table
                .label_ids
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        )
        .saturating_add(
            table
                .label_offsets
                .capacity()
                .saturating_mul(std::mem::size_of::<u32>()),
        );
    assert_eq!(table.approx_heap_bytes(), expected_heap_bytes);
}

#[test]
fn table_exposes_compaction_runs_without_building_postings() {
    let mut builder = SeriesTableBuilder::new();
    builder.begin_metric("m");
    builder.push_series(&ls(&[("host", "a")]), 0, 3, 5, 9);
    builder.begin_metric("m");
    builder.push_series(&ls(&[("host", "b")]), 3, 2, 10, 12);
    let table = builder.finish();

    let first = table.metric_run(0).unwrap();
    assert_eq!(first.metric_name, "m");
    assert_eq!(
        (first.first_series, first.series_count, first.first_row),
        (0, 1, 0)
    );
    let second = table.metric_run(1).unwrap();
    assert_eq!(
        (second.first_series, second.series_count, second.first_row),
        (1, 1, 3)
    );
    assert_eq!(table.total_rows(), 5);
    assert_eq!(table.series_entry(1).unwrap().labels, ls(&[("host", "b")]));
}

#[test]
fn large_footer_indexes_are_compressed_and_legacy_json_still_reads() {
    let mut b = SeriesTableBuilder::new();
    b.begin_metric("m");
    for i in 0..4_000u64 {
        let host = format!("host-{i:05}");
        b.push_series(&ls(&[("host", &host)]), i, 1, i as i64, i as i64);
    }
    let table = b.finish();
    let legacy = serde_json::to_string(&table).unwrap();
    let compressed = table.to_footer_value().unwrap();

    assert!(compressed.starts_with(COMPRESSED_FOOTER_PREFIX));
    assert!(compressed.len() < legacy.len());
    assert_eq!(SeriesTable::from_footer_value(&compressed).unwrap(), table);
    assert_eq!(SeriesTable::from_footer_value(&legacy).unwrap(), table);
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
    t.label_ids[0] = 99;
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
    assert_eq!(idx.labels(ids[0]).unwrap().get("host"), Some("h2"));
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
    let a_labels = a.labels(0).unwrap();
    let b_labels = b.labels(0).unwrap();
    assert!(a_labels.ptr_eq(&b_labels));
}

#[test]
fn selector_candidates_match_the_full_matcher() {
    let idx = sample();
    let filter: crate::storage::scanner::SeriesFilter = Arc::new(|labels: &LabelSet| {
        labels.get("env") == Some("stage")
            && labels.get("service.name") == Some("api")
            && labels.get("host").is_some_and(|host| host.starts_with('h'))
    });
    let expected = idx.select(
        "cpu",
        &SeriesSelector {
            filter: Some(filter.clone()),
            ..Default::default()
        },
        i64::MIN,
        i64::MAX,
    );
    let selector = SeriesSelector {
        equals: vec![
            ("service.name".into(), "api".into()),
            ("env".into(), "stage".into()),
        ],
        value_tests: vec![(
            "host".into(),
            Arc::new(|value: &str| value.starts_with('h')),
        )],
        filter: Some(filter),
        ..Default::default()
    };
    assert_eq!(idx.select("cpu", &selector, i64::MIN, i64::MAX), expected);

    for (label, value) in [("missing", "value"), ("host", ""), ("host", "absent")] {
        assert!(idx
            .select(
                "cpu",
                &SeriesSelector {
                    equals: vec![(label.into(), value.into())],
                    ..Default::default()
                },
                i64::MIN,
                i64::MAX,
            )
            .is_empty());
    }
}

#[test]
fn labels_are_materialised_lazily() {
    let idx = sample();
    assert_eq!(idx.materialized_label_count(), 0);

    let selector = SeriesSelector {
        equals: vec![("host".into(), "h2".into())],
        ..Default::default()
    };
    assert_eq!(idx.select("cpu", &selector, i64::MIN, i64::MAX), vec![2]);
    assert_eq!(idx.materialized_label_count(), 0);

    let filtered = SeriesSelector {
        filter: Some(Arc::new(|labels: &LabelSet| {
            labels.get("env") == Some("stage")
        })),
        ..selector
    };
    assert_eq!(idx.select("cpu", &filtered, i64::MIN, i64::MAX), vec![2]);
    assert_eq!(idx.materialized_label_count(), 1);
}

#[test]
fn labels_reject_corrupt_pairs_as_internal_errors() {
    let mut table = sample_table();
    table.label_ids[2] = table.label_ids[0];
    let idx = SeriesIndex::from_owned_table(table).unwrap();
    assert!(matches!(idx.labels(0), Err(Error::Internal(_))));
}

#[test]
fn approx_bytes_is_stable_across_scans_and_materialization() {
    let table = sample_table();
    let table_bytes = table.approx_heap_bytes();
    let idx = SeriesIndex::from_owned_table(table).unwrap();
    let before = idx.approx_bytes();
    let label_slots = idx
        .series_count()
        .saturating_mul(std::mem::size_of::<OnceLock<LabelSet>>());
    assert!(before >= table_bytes.saturating_add(label_slots));

    let selector = SeriesSelector {
        equals: vec![("host".into(), "h2".into())],
        filter: Some(Arc::new(|_| true)),
        ..Default::default()
    };
    assert_eq!(idx.select("cpu", &selector, i64::MIN, i64::MAX), vec![2]);
    assert_eq!(idx.approx_bytes(), before);
}

#[test]
fn restrict_helper() {
    assert_eq!(
        restrict(&[1, 3, 5, 8, 9], &[(2, 6), (8, 10)]),
        vec![3, 5, 8, 9]
    );
    assert_eq!(restrict(&[1, 3], &[]), Vec::<u32>::new());
}

#[test]
fn intersect_all_returns_empty_for_empty_input() {
    assert_eq!(intersect_all(&mut []), Vec::<u32>::new());
}

#[test]
fn intersect_all_returns_one_list_unchanged() {
    assert_eq!(intersect_all(&mut [&[1, 3, 5]]), vec![1, 3, 5]);
}

#[test]
fn intersect_all_handles_disjoint_and_identical_lists() {
    assert!(intersect_all(&mut [&[1, 3], &[2, 4], &[1, 2, 3, 4]]).is_empty());
    assert_eq!(
        intersect_all(&mut [&[1, 3, 5], &[1, 3, 5], &[1, 3, 5]]),
        vec![1, 3, 5]
    );
}

#[test]
fn intersect_all_matches_btree_set_intersection_for_seeded_inputs() {
    fn next(seed: &mut u64) -> u32 {
        *seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
        (*seed >> 32) as u32
    }

    let mut seed = 0x5eed_cafe_d00d_f00d;
    for case in 0..200 {
        let list_count = (next(&mut seed) % 7) as usize;
        let owned: Vec<Vec<u32>> = (0..list_count)
            .map(|_| {
                let len = (next(&mut seed) % 30) as usize;
                let mut list: Vec<u32> = (0..len).map(|_| next(&mut seed) % 80).collect();
                list.sort_unstable();
                list.dedup();
                list
            })
            .collect();

        let expected = if let Some(first) = owned.first() {
            let mut intersection: std::collections::BTreeSet<u32> = first.iter().copied().collect();
            for list in owned.iter().skip(1) {
                intersection.retain(|id| list.binary_search(id).is_ok());
            }
            intersection.into_iter().collect()
        } else {
            Vec::new()
        };
        let mut lists: Vec<&[u32]> = owned.iter().map(Vec::as_slice).collect();

        assert_eq!(intersect_all(&mut lists), expected, "case {case}");
    }
}
