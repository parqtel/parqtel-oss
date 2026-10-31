//! Metric queries and executor construction.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[tokio::test]
async fn test_ast_volume_summary_counts_all_matched_points() {
    // The AST path must return a volume_summary counting EVERY matched
    // point in the visible timeline — not only the samples that survive
    // series truncation — so the UI volume bars stay complete (same
    // contract as the legacy plan path).
    let (exec, _dir) = setup_with_data().await;
    // `cpu` has points at ts 1000 (h1), 2000 (h1), 3000 (h2); host="h1"
    // matches 2 of them. Timeline [0, 4000] ns covers all three.
    let expr = crate::parser::parse_expr("avg_over_time(cpu{host=\"h1\"}[5m])").unwrap();
    let res = exec.execute_ast(&expr, 0, 4000, Some(1000)).await.unwrap();
    assert_eq!(
        res.volume_summary.iter().sum::<u64>(),
        2,
        "AST volume summary must count all matched points pre-truncation"
    );

    // Without the label matcher all 3 `cpu` points count.
    let expr = crate::parser::parse_expr("avg_over_time(cpu[5m])").unwrap();
    let res = exec.execute_ast(&expr, 0, 4000, Some(1000)).await.unwrap();
    assert_eq!(res.volume_summary.iter().sum::<u64>(), 3);

    // Points timestamped before the timeline start (lookback) must not
    // be bucketed into the visible range.
    let res = exec
        .execute_ast(&expr, 2500, 4000, Some(1000))
        .await
        .unwrap();
    assert_eq!(
        res.volume_summary.iter().sum::<u64>(),
        1,
        "only the ts=3000 point lies within [2500, 4000]"
    );
}

/// The evaluator must not run on an async worker.
///
/// On a current-thread runtime there is exactly one worker, so a CPU-bound
/// stretch with no yield point starves every other task for its whole
/// duration. A background ticker therefore measures directly whether the
/// evaluation was offloaded: inline it would get ~no ticks for the length of
/// the query, offloaded it gets one per millisecond.
#[tokio::test]
async fn test_query_evaluation_does_not_block_the_async_worker() {
    let dir = tempdir().unwrap();
    let index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(dir.path())));
    let exec = QueryExecutor::new(index, log_index, dir.path().join("traces"));

    // Buffer-only data: enough series and steps that evaluation is
    // measurable, with no Parquet decode in the way.
    const SERIES: usize = 300;
    const POINTS: usize = 400;
    let buffer = exec.memory_buffer();
    for s in 0..SERIES {
        let points: Vec<parqtel_core::DataPoint> = (0..POINTS)
            .map(|i| {
                parqtel_core::DataPoint::new(
                    // Timestamps must be > 0; start at 1s and step 1s.
                    (i as i64 + 1) * 1_000_000_000,
                    MetricValue::Double(i as f64),
                    LabelSet::try_from_iter(vec![
                        ("host", format!("h{}", s % 20)),
                        ("idx", s.to_string()),
                    ])
                    .unwrap(),
                )
                .unwrap()
            })
            .collect();
        buffer.push_metrics("bench", &points).await;
    }

    let expr = crate::parser::parse_expr("sum by (host) (bench)").unwrap();

    // 1ms ticker on the same single-threaded runtime.
    use std::sync::atomic::{AtomicUsize, Ordering};
    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker = {
        let ticks = ticks.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                ticks.fetch_add(1, Ordering::Relaxed);
            }
        })
    };

    // Run the query inline on this task: the ticker can only advance when
    // the runtime gets control back, which is exactly what is being tested.
    let result = exec
        .execute_ast(&expr, 1_000_000_000, 500_000_000_000, Some(1_000_000_000))
        .await
        .unwrap();
    let observed = ticks.load(Ordering::Relaxed);
    ticker.abort();

    assert!(!result.series.is_empty(), "query must return data");
    // Threshold 3: measured at 0 ticks with the evaluation inlined, so this
    // discriminates rather than merely passing. Avoids asserting an exact
    // count, which would be timing-dependent.
    assert!(
        observed >= 3,
        "the async worker was starved during query evaluation: a 1ms ticker \
         only advanced {observed} times, so the CPU half is not offloaded"
    );
}

#[tokio::test]
async fn test_query_executor_empty() {
    let index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/ne"))));
    let trace_data_dir = PathBuf::from("/tmp/ne/traces");
    std::fs::create_dir_all(&trace_data_dir).ok();
    let exec = QueryExecutor::new(index, log_index, trace_data_dir);
    let plan = QueryPlan::new("cpu".into(), vec![], 0, 100, None, 10, 100, None, None).unwrap();
    let res = exec.execute(plan).await.unwrap();
    assert_eq!(res.series.len(), 0);
    assert_eq!(res.points_scanned, 0);
    assert_eq!(res.volume_summary.len(), 60);
}

/// `total_series_count` used to increment once per *point* in the AST
/// path, so a single series with three samples reported 3. The plan path
/// has always counted distinct label sets; the two must agree.
#[tokio::test]
async fn test_execute_ast_counts_series_not_points() {
    let (exec, _dir) = setup_with_data().await;
    let expr = crate::parser::parse_expr("cpu").unwrap();
    let res = exec.execute_ast(&expr, 0, 4000, Some(1000)).await.unwrap();
    assert_eq!(
        res.total_series_count, 2,
        "`cpu` has 2 series (host=h1 with 2 points, host=h2 with 1)"
    );

    let plan = QueryPlan::new("cpu".into(), vec![], 0, 4000, None, 10, 100, None, None).unwrap();
    let plan_res = exec.execute(plan).await.unwrap();
    assert_eq!(
        res.total_series_count, plan_res.total_series_count,
        "AST and plan paths must agree on the series count"
    );
}

/// The AST path previously had no result limits at all, so a query could
/// materialise any number of series regardless of `query.max_series`. It
/// must now cap series while still reporting the full matched count —
/// matching the plan path's truncation semantics.
#[tokio::test]
async fn test_execute_ast_applies_max_series_limit() {
    let (exec, _dir) = setup_with_data().await;
    // 3 points across 2 series (h1, h2): with a cap of 1 the result is
    // truncated but `total_series_count` still reflects both matches.
    let exec = exec.with_result_limits(1, 10_000);
    let expr = crate::parser::parse_expr("cpu").unwrap();
    let res = exec.execute_ast(&expr, 0, 4000, Some(1000)).await.unwrap();
    assert_eq!(res.total_series_count, 2, "both series matched");
    assert_eq!(res.series.len(), 1, "only one series survives the cap");
}

/// Likewise `max_samples_per_series` must bound a single wide series.
///
/// Asserted through `count_over_time`, which observes the number of
/// *input* samples the evaluator sees. The instant-vector result of a
/// plain selector is not a direct probe: every step re-reports the last
/// sample within lookback, so a series with one retained sample still
/// yields one output sample per step.
#[tokio::test]
async fn test_execute_ast_applies_max_samples_per_series_limit() {
    let (exec, _dir) = setup_with_data().await;

    // host=h1 has points at ts 1000 and 2000. Uncapped, count_over_time
    // reaches 2; capping at 1 sample per series must hold it at 1.
    let expr = crate::parser::parse_expr("count_over_time(cpu{host=\"h1\"}[5m])").unwrap();

    let uncapped = exec.execute_ast(&expr, 0, 4000, Some(1000)).await.unwrap();
    assert_eq!(
        uncapped.series[0].samples.last().unwrap().value,
        2.0,
        "without the cap both samples are visible"
    );

    let capped = exec
        .with_result_limits(10, 1)
        .execute_ast(&expr, 0, 4000, Some(1000))
        .await
        .unwrap();
    assert_eq!(
        capped.series[0].samples.last().unwrap().value,
        1.0,
        "the per-series sample cap must bound what the evaluator sees"
    );
}

#[test]
fn result_limits_are_clamped_above_zero() {
    // A zero cap would silently return nothing, which is a far worse
    // failure than ignoring the value.
    let exec = QueryExecutor::new(
        Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/nl")))),
        Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/nl")))),
        PathBuf::from("/tmp/nl/traces"),
    )
    .with_result_limits(0, 0);
    assert_eq!(exec.max_series, 1);
    assert_eq!(exec.max_samples_per_series, 1);
}

/// Every public constructor must produce an executor with the same
/// defaults; they previously duplicated their initialisation blocks, so a
/// new field could be added to one and forgotten in the others.
#[test]
fn all_constructors_share_defaults() {
    let index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/nc"))));
    let log_index = Arc::new(RwLock::new(BlockIndex::new(Path::new("/tmp/nc"))));
    let dir = PathBuf::from("/tmp/nc/traces");

    let builders = vec![
        QueryExecutor::new(index.clone(), log_index.clone(), dir.clone()),
        QueryExecutor::with_buffer(
            index.clone(),
            log_index.clone(),
            parqtel_core::MemoryBuffer::new(),
            dir.clone(),
        ),
        QueryExecutor::with_trace_index(
            index.clone(),
            log_index.clone(),
            Arc::new(RwLock::new(BlockIndex::new(&dir))),
            parqtel_core::MemoryBuffer::new(),
            dir.clone(),
        ),
    ];
    for exec in builders {
        assert_eq!(exec.lookback_ns, 5 * 60 * 1_000_000_000);
        assert_eq!(exec.max_series, 1000);
        assert_eq!(exec.max_samples_per_series, 10_000);
        assert_eq!(exec.trace_data_dir, dir, "trace dir must be honoured");
    }
}

#[tokio::test]
async fn test_execute_with_real_data() {
    let (exec, _dir) = setup_with_data().await;
    let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 10, 100, None, None).unwrap();
    let res = exec.execute(plan).await.unwrap();
    assert_eq!(res.points_scanned, 3);
    assert!(!res.series.is_empty());
    assert_eq!(res.total_series_count, 2); // h1 and h2
}

#[tokio::test]
async fn test_execute_with_label_matcher() {
    let (exec, _dir) = setup_with_data().await;
    let matchers = vec![LabelMatcher::equal("host", "h1")];
    let plan = QueryPlan::new("cpu".into(), matchers, 0, 5000, None, 10, 100, None, None).unwrap();
    let res = exec.execute(plan).await.unwrap();
    assert_eq!(res.total_series_count, 1);
    assert_eq!(res.series[0].samples.len(), 2);
    // The block's series table finds the matching series without reading the
    // others, so only the 2 rows that matched are read - not all 3 of the
    // metric.
    assert_eq!(res.points_scanned, 2);
    // The volume histogram counts matched points only.
    assert_eq!(res.volume_summary.iter().sum::<u64>(), 2);
}

/// The AST path narrows by series too: rows of series the matcher rejects
/// are never read, so they are not counted.
#[tokio::test]
async fn test_execute_ast_matcher_pushdown_counts_scanned_rows() {
    let (exec, _dir) = setup_with_data().await;
    let expr = crate::parser::parse_expr(r#"sum(cpu{host="h1"})"#).unwrap();
    let res = exec.execute_ast(&expr, 0, 5000, Some(1000)).await.unwrap();
    assert_eq!(res.total_series_count, 1);
    assert_eq!(res.points_scanned, 2);

    let expr = crate::parser::parse_expr(r#"sum(cpu{host="nope"})"#).unwrap();
    let res = exec.execute_ast(&expr, 0, 5000, Some(1000)).await.unwrap();
    assert_eq!(res.total_series_count, 0);
    assert_eq!(res.points_scanned, 0);
    assert!(res.series.is_empty());
}

#[tokio::test]
async fn test_execute_max_series_truncation() {
    let (exec, _dir) = setup_with_data().await;
    let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 1, 100, None, None).unwrap();
    let res = exec.execute(plan).await.unwrap();
    // max_series=1 so only 1 series returned, but total_series_count reflects all matched
    assert_eq!(res.series.len(), 1);
    assert_eq!(res.total_series_count, 2);
}

#[tokio::test]
async fn test_execute_with_step_aggregation() {
    let (exec, _dir) = setup_with_data().await;
    let plan = QueryPlan::new(
        "cpu".into(),
        vec![],
        0,
        5000,
        Some(5000),
        10,
        100,
        Some(crate::plan::AggregationOp::Avg),
        None,
    )
    .unwrap();
    let res = exec.execute(plan).await.unwrap();
    assert!(!res.series.is_empty());
}

/// A `{service.name="x"}` selector narrows the scan at the block level. The
/// result must be identical to the unfiltered query — a prune that dropped a
/// row group holding the requested service would be silent data loss.
///
/// Uses its own fixture rather than `setup_with_data`, whose resource
/// attributes use the underscored `service_name` spelling. That spelling
/// populates neither the `service_name` column nor the reconstructed series
/// labels, so a `service.name` selector against it correctly matches
/// nothing — which would make this test prove nothing.
#[tokio::test]
async fn test_service_selector_matches_the_unfiltered_query() {
    let dir = tempdir().unwrap();
    let engine = Arc::new(ParquetStorageEngine::new(BlockConfig {
        data_dir: dir.path().to_path_buf(),
        ..Default::default()
    }));
    let storage: Arc<dyn StorageEngine> = engine.clone();

    // Two services, two series each — the dotted OTLP resource key, which
    // is what `extract_correlation_labels` reads.
    let metrics: Vec<parqtel_core::Metric> = ["web", "api"]
        .iter()
        .flat_map(|svc| {
            ["h1", "h2"].map(move |host| parqtel_core::Metric {
                name: "cpu".into(),
                kind: parqtel_core::MetricKind::Gauge,
                resource_attributes: LabelSet::try_from_iter(vec![(
                    "service.name",
                    (*svc).to_string(),
                )])
                .unwrap(),
                data_points: vec![
                    parqtel_core::DataPoint::new(
                        1000,
                        parqtel_core::MetricValue::Double(10.0),
                        LabelSet::try_from_iter(vec![("host", host)]).unwrap(),
                    )
                    .unwrap(),
                    parqtel_core::DataPoint::new(
                        2000,
                        parqtel_core::MetricValue::Double(20.0),
                        LabelSet::try_from_iter(vec![("host", host)]).unwrap(),
                    )
                    .unwrap(),
                ],
                ..Default::default()
            })
        })
        .collect();
    storage.write_metrics_batch(metrics).await.unwrap();
    // Persist off the runtime thread, as the other tests do: the sidecar
    // write is blocking I/O.
    let engine_for_persist = engine.clone();
    tokio::task::spawn_blocking(move || {
        let idx = engine_for_persist.metrics_index().blocking_read();
        let store = parqtel_core::BlockIndexStore::new(&idx);
        store.mark_dirty();
        parqtel_core::storage::persist_blocking(&idx, &store)
    })
    .await
    .unwrap()
    .unwrap();
    let mut index = BlockIndex::new(dir.path());
    index.load().unwrap();
    let exec = QueryExecutor::new(
        Arc::new(tokio::sync::RwLock::new(index)),
        Arc::new(tokio::sync::RwLock::new(BlockIndex::new(
            &dir.path().join("logs"),
        ))),
        dir.path().join("traces"),
    );

    let plain = crate::parser::parse_expr("cpu").unwrap();
    let unfiltered = exec.execute_ast(&plain, 0, 5000, Some(1000)).await.unwrap();
    assert_eq!(unfiltered.total_series_count, 4, "2 services x 2 hosts");

    let web = crate::parser::parse_expr(r#"cpu{service.name="web"}"#).unwrap();
    let filtered = exec.execute_ast(&web, 0, 5000, Some(1000)).await.unwrap();
    assert_eq!(
        filtered.total_series_count, 2,
        "service pruning must keep every series of the requested service"
    );
    assert!(
        filtered
            .series
            .iter()
            .all(|s| s.labels.get("service.name") == Some("web")),
        "only web series should come back"
    );

    let absent = crate::parser::parse_expr(r#"cpu{service.name="nope"}"#).unwrap();
    let absent = exec
        .execute_ast(&absent, 0, 5000, Some(1000))
        .await
        .unwrap();
    assert_eq!(absent.total_series_count, 0, "absent service -> no series");
}

#[tokio::test]
async fn test_execute_volume_summary() {
    let (exec, _dir) = setup_with_data().await;
    let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 10, 100, None, None).unwrap();
    let res = exec.execute(plan).await.unwrap();
    assert_eq!(res.volume_summary.len(), 60);
    let total: u64 = res.volume_summary.iter().sum();
    assert_eq!(total, 3); // 3 data points
}

#[tokio::test]
async fn test_execute_max_samples_per_series() {
    let (exec, _dir) = setup_with_data().await;
    let plan = QueryPlan::new("cpu".into(), vec![], 0, 5000, None, 10, 1, None, None).unwrap();
    let res = exec.execute(plan).await.unwrap();
    // Each series should have at most 1 sample
    for s in &res.series {
        assert!(s.samples.len() <= 1);
    }
}
