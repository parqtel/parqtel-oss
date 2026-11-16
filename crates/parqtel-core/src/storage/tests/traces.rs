//! Trace scans and trace-ID predicate pushdown.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn trace_id_filter_runs_before_span_limit() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("traces.parquet");
    let target_id = [0x42; 16];
    let mut spans: Vec<_> = (0..201).map(|i| test_span(i + 1)).collect();
    for start in [1_000, 1_500] {
        let mut target = test_span(start);
        target.trace_id = target_id;
        spans.push(target);
    }
    write_traces_parquet(&path, &spans);

    let meta = BlockMetadata {
        path,
        start_timestamp_ns: 1,
        end_timestamp_ns: 2_000,
        row_count: spans.len(),
        size_bytes: 0,
        metric_names: HashSet::new(),
        label_names: HashSet::new(),
        label_values: BTreeMap::new(),
        signal_type: SignalType::Traces,
        wal_range: None,
    };

    let unfiltered = Scanner::scan_traces(vec![meta.clone()], 0, i64::MAX, 200)
        .await
        .unwrap();
    assert!(unfiltered.iter().all(|span| span.trace_id != target_id));

    let filtered =
        Scanner::scan_traces_filtered(vec![meta.clone()], 0, i64::MAX, 200, Some(target_id))
            .await
            .unwrap();
    assert_eq!(filtered.len(), 2);
    assert!(filtered.iter().all(|span| span.trace_id == target_id));

    let newest = Scanner::scan_traces_filtered(vec![meta], 0, i64::MAX, 1, Some(target_id))
        .await
        .unwrap();
    assert_eq!(newest.len(), 1);
    assert_eq!(newest[0].start_time_ns, 1_500);
}
