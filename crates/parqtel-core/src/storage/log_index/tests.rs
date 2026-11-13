//! Tests for the `log_index` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn ls(pairs: &[(&str, &str)]) -> LabelSet {
    LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (*k, *v))).unwrap()
}

/// 3 granules + a short tail. Row `i` has timestamp `i + 1`; "needle-xyz" is
/// only in row 5_000 (granule 1); "pod=p7" only in granule 2.
fn sample() -> LogSkipIndex {
    let mut b = LogIndexBuilder::new();
    let none = LabelSet::default();
    let p7 = ls(&[("pod", "p7")]);
    let other = ls(&[("pod", "p1")]);
    let res = ls(&[("service.name", "api")]);
    for i in 0..(3 * GRANULE_ROWS + 100) {
        let body = if i == 5_000 {
            "an unusual NEEDLE-xyz appears".to_string()
        } else {
            format!("request {i} handled ok")
        };
        let attrs = if (2 * GRANULE_ROWS..3 * GRANULE_ROWS).contains(&i) {
            &p7
        } else if i % 2 == 0 {
            &other
        } else {
            &none
        };
        let sev = if i == 100 { 17 } else { 9 };
        b.push_row(i as i64 + 1, sev, &body, attrs, &res);
    }
    let table = b.finish();
    let text = table.to_footer_value().unwrap();
    LogSkipIndex::from_table(&LogIndexTable::from_footer_value(&text).unwrap()).unwrap()
}

fn pre(needles: &[&str], pairs: &[(&str, &str)], sev: Option<i32>) -> LogColumnPrefilter {
    LogColumnPrefilter {
        min_severity: sev,
        body_contains_lower: needles.iter().map(|s| s.to_string()).collect(),
        attr_equals: pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        exact: false,
    }
}

#[test]
fn a_rare_body_term_leaves_only_its_granule() {
    let ix = sample();
    let g = GRANULE_ROWS as u64;
    let got = ix.candidate_ranges(0, i64::MAX, Some(&pre(&["needle-xyz"], &[], None)));
    assert_eq!(got, vec![(g, 2 * g)]);
    // Case is folded by the caller; a common term keeps everything.
    let all = ix.candidate_ranges(0, i64::MAX, Some(&pre(&["handled"], &[], None)));
    assert_eq!(all, vec![(0, 3 * g + 100)]);
    // A term nowhere in the block.
    assert!(ix
        .candidate_ranges(0, i64::MAX, Some(&pre(&["absent-term"], &[], None)))
        .is_empty());
}

#[test]
fn short_needles_and_no_prefilter_prune_nothing_but_time() {
    let ix = sample();
    let g = GRANULE_ROWS as u64;
    assert_eq!(
        ix.candidate_ranges(0, i64::MAX, Some(&pre(&["ab"], &[], None))),
        vec![(0, 3 * g + 100)]
    );
    // Timestamps are row + 1: a window inside granule 1 keeps just it.
    assert_eq!(
        ix.candidate_ranges(g as i64 + 10, g as i64 + 20, None),
        vec![(g, 2 * g)]
    );
}

#[test]
fn attribute_pairs_severity_and_conjunction() {
    let ix = sample();
    let g = GRANULE_ROWS as u64;
    assert_eq!(
        ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[("pod", "p7")], None))),
        vec![(2 * g, 3 * g)]
    );
    // A resource attribute is indexed too, and matches everywhere.
    assert_eq!(
        ix.candidate_ranges(
            0,
            i64::MAX,
            Some(&pre(&[], &[("service.name", "api")], None))
        ),
        vec![(0, 3 * g + 100)]
    );
    // Severity 17 occurs only in granule 0.
    assert_eq!(
        ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[], Some(13)))),
        vec![(0, g)]
    );
    // Every condition must hold in the same granule: the needle is in granule
    // 1 and the pod in granule 2.
    assert!(ix
        .candidate_ranges(
            0,
            i64::MAX,
            Some(&pre(&["needle-xyz"], &[("pod", "p7")], None))
        )
        .is_empty());
}

#[test]
fn trace_and_span_ids_narrow_to_their_granule() {
    use crate::models::logs::LogRecord;
    let mut b = LogIndexBuilder::new();
    for i in 0..(2 * GRANULE_ROWS) {
        let (trace, span) = if i == GRANULE_ROWS + 7 {
            ([0xab; 16], [0xcd; 8])
        } else {
            ([0u8; 16], [0u8; 8])
        };
        b.push_record(&LogRecord::new(
            i as i64 + 1,
            i as i64 + 1,
            9,
            "INFO".into(),
            format!("row {i}"),
            LabelSet::default(),
            LabelSet::default(),
            trace,
            span,
            0,
            "".into(),
            "".into(),
        ));
    }
    let ix = LogSkipIndex::from_table(&b.finish()).unwrap();
    let g = GRANULE_ROWS as u64;
    let trace = "ab".repeat(16);
    let span = "cd".repeat(8);
    assert_eq!(
        ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[("trace_id", &trace)], None))),
        vec![(g, 2 * g)]
    );
    assert_eq!(
        ix.candidate_ranges(0, i64::MAX, Some(&pre(&[], &[("span_id", &span)], None))),
        vec![(g, 2 * g)]
    );
    // The all-zero id is "none", not a value to look up.
    assert!(ix
        .candidate_ranges(
            0,
            i64::MAX,
            Some(&pre(&[], &[("trace_id", &"00".repeat(16))], None))
        )
        .is_empty());
}

#[test]
fn corrupt_tables_are_rejected() {
    let ix_table = {
        let mut b = LogIndexBuilder::new();
        for i in 0..10 {
            b.push_row(
                i + 1,
                9,
                "hello world",
                &LabelSet::default(),
                &LabelSet::default(),
            );
        }
        b.finish()
    };
    assert!(LogSkipIndex::from_table(&ix_table).is_ok());
    let mut t = ix_table.clone();
    t.rows = 999_999;
    assert!(LogSkipIndex::from_table(&t).is_err());
    let mut t = ix_table.clone();
    t.bloom.truncate(t.bloom.len() / 2);
    assert!(LogSkipIndex::from_table(&t).is_err());
    let mut t = ix_table;
    t.v = 99;
    assert!(LogIndexTable::from_footer_value(&serde_json::to_string(&t).unwrap()).is_err());
    assert!(LogIndexTable::from_footer_value("{nope").is_err());
}
