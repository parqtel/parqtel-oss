//! Tests for the `eval` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::aggregation::{grouping_fingerprint, normalize_grouping};
use super::*;

fn mk_data() -> SeriesData {
    let mut data = SeriesData::new();
    let mk_labels = |svc: &str| {
        LabelSet::try_from_iter(vec![("service".to_string(), svc.to_string())]).unwrap()
    };
    // counter rising 1/sec at 10s intervals for 100s
    let points: Vec<(i64, f64)> = (0..10).map(|i| (i * 10_000_000_000, i as f64)).collect();
    data.insert(
        "requests".into(),
        vec![
            (crate::ast::shared_labels(mk_labels("a")), points.clone()),
            (crate::ast::shared_labels(mk_labels("b")), points.clone()),
        ],
    );
    data
}

fn eval_query(q: &str, ts: i64) -> InstantVector {
    let expr = crate::parser::parse_expr(q).unwrap();
    let data = mk_data();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: ts,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    ev.eval(&expr, ctx).unwrap()
}

// ─────────────────────────────────────────────────────────────────────
// Per-function coverage: every PromQL function the engine implements,
// exercised end-to-end through parse → eval.
// ─────────────────────────────────────────────────────────────────────

fn scalar(q: &str, ts: i64) -> f64 {
    let v = eval_query(q, ts);
    v.series[0].1
}

#[test]
fn fn_math_unary_family() {
    // abs/ceil/floor/sqrt/exp/ln/log2/log10/sgn on a known value.
    // system series isn't in mk_data; use requests at t=90s → value 9.
    let t = 95_000_000_000;
    assert_eq!(scalar("abs(requests)", t), 9.0);
    assert_eq!(scalar("ceil(requests)", t), 9.0);
    assert_eq!(scalar("floor(requests)", t), 9.0);
    assert_eq!(scalar("sqrt(requests)", t), 3.0);
    assert_eq!(scalar("sgn(requests)", t), 1.0);
    assert!((scalar("exp(requests)", t) - 9.0f64.exp()).abs() < 1e-9);
    assert!((scalar("ln(requests)", t) - 9.0f64.ln()).abs() < 1e-9);
    assert!((scalar("log2(requests)", t) - 9.0f64.log2()).abs() < 1e-9);
    assert!((scalar("log10(requests)", t) - 9.0f64.log10()).abs() < 1e-9);
}

#[test]
fn fn_trig_family() {
    let t = 95_000_000_000;
    // sin/cos/tan at value 9 rad; compare against Rust std.
    assert!((scalar("sin(requests)", t) - 9.0f64.sin()).abs() < 1e-9);
    assert!((scalar("cos(requests)", t) - 9.0f64.cos()).abs() < 1e-9);
    assert!((scalar("tan(requests)", t) - 9.0f64.tan()).abs() < 1e-9);
    // asin(9)/acos(9) are NaN → NaN-valued series are dropped
    // (Prometheus: no sample rather than a NaN output).
    let v = eval_query("asin(requests)", t);
    assert!(v.series.is_empty(), "asin(9)=NaN drops series");
    let v = eval_query("acos(requests)", t);
    assert!(v.series.is_empty(), "acos(9)=NaN drops series");
    assert!((scalar("atan(requests)", t) - 9.0f64.atan()).abs() < 1e-9);
    assert!((scalar("sinh(requests)", t) - 9.0f64.sinh()).abs() < 1e-9);
    assert!((scalar("cosh(requests)", t) - 9.0f64.cosh()).abs() < 1e-9);
    assert!((scalar("tanh(requests)", t) - 9.0f64.tanh()).abs() < 1e-9);
    assert!((scalar("asinh(requests)", t) - 9.0f64.asinh()).abs() < 1e-9);
    assert!((scalar("acosh(requests)", t) - 9.0f64.acosh()).abs() < 1e-9);
    // atanh(9) is NaN (|x|>1) → series dropped.
    let v = eval_query("atanh(requests)", t);
    assert!(v.series.is_empty(), "atanh(9)=NaN drops series");
    // deg/rad round-trip: 9 rad → deg → rad == 9.
    assert!((scalar("rad(deg(requests))", t) - 9.0).abs() < 1e-9);
}

#[test]
fn fn_pi() {
    let v = eval_query("pi()", 0);
    assert_eq!(v.series.len(), 1);
    assert!((v.series[0].1 - std::f64::consts::PI).abs() < 1e-12);
}

#[test]
fn fn_atan2() {
    let t = 95_000_000_000;
    // atan2(y=9, x=1): y from the vector, x scalar.
    assert!((scalar("atan2(requests, 1)", t) - 9.0f64.atan2(1.0)).abs() < 1e-9);
    assert!((scalar("atan2(requests, -1)", t) - 9.0f64.atan2(-1.0)).abs() < 1e-9);
}

#[test]
fn fn_round_clamp() {
    let t = 95_000_000_000;
    assert_eq!(scalar("round(requests, 4)", t), 8.0); // 9 → nearest 4
    assert_eq!(scalar("clamp(requests, 2, 5)", t), 5.0);
    assert_eq!(scalar("clamp_min(requests, 20)", t), 20.0);
    assert_eq!(scalar("clamp_max(requests, 4)", t), 4.0);
}

#[test]
fn fn_time_and_date_family() {
    // Fixed timestamp: 2026-06-15 12:34:56 UTC = 1782 guy... use
    // a known epoch: 2024-01-01T00:00:00Z = 1704067200.
    let ts = 1_704_067_200_000_000_000i64; // Mon Jan 1 2024 00:00:00 UTC
    assert_eq!(scalar("time()", ts), 1_704_067_200.0);
    assert_eq!(scalar("hour()", ts), 0.0);
    assert_eq!(scalar("minute()", ts), 0.0);
    assert_eq!(scalar("day_of_week()", ts), 1.0); // Monday
    assert_eq!(scalar("day_of_month()", ts), 1.0);
    assert_eq!(scalar("day_of_year()", ts), 1.0);
    assert_eq!(scalar("month()", ts), 1.0);
    assert_eq!(scalar("year()", ts), 2024.0);
    assert_eq!(scalar("days_in_month()", ts), 31.0);
    // Vector-arg form: pass epoch seconds as a scalar vector.
    // requests at t=95s has value 9 — meaningless as a date; instead
    // verify the per-series mapping shape only.
    let v = eval_query("hour(requests)", 95_000_000_000);
    assert_eq!(v.series.len(), 2, "date fn applies per series");
}

#[test]
fn fn_timestamp() {
    // All series present at eval time get the eval timestamp.
    let v = eval_query("timestamp(requests)", 95_000_000_000);
    assert_eq!(v.series.len(), 2);
    for (_, val) in &v.series {
        assert!((val - 95.0).abs() < 1e-9, "timestamp = {val}");
    }
}

#[test]
fn fn_vector_scalar() {
    let v = eval_query("vector(42)", 0);
    assert_eq!(v.series.len(), 1);
    assert_eq!(v.series[0].1, 42.0);
    let s = eval_query("scalar(requests)", 95_000_000_000);
    // scalar() of a 2-series vector → NaN per Prometheus.
    assert!(s.series[0].1.is_nan(), "scalar() of multi-series = NaN");
}

#[test]
fn fn_absent_family() {
    // absent(existing) → empty; absent(missing) → 1.
    let v = eval_query("absent(requests)", 95_000_000_000);
    assert_eq!(v.series.len(), 0, "existing metric → no series");
    let v = eval_query("absent(nope)", 0);
    assert_eq!(v.series.len(), 1, "missing metric → 1");
    // absent over a missing metric with equality matchers keeps labels
    let v = eval_query(r#"absent(nope{service="a"})"#, 0);
    assert_eq!(v.series.len(), 1);
    assert_eq!(v.series[0].1, 1.0);
    assert_eq!(
        v.series[0].0.get("service").map(|s| s.to_string()),
        Some("a".to_string())
    );
    // absent_over_time: same semantics via range window.
    let v = eval_query("absent_over_time(nope[5m])", 0);
    assert_eq!(v.series.len(), 1);
    assert_eq!(v.series[0].1, 1.0);
    let v = eval_query("absent_over_time(requests[5m])", 95_000_000_000);
    assert_eq!(v.series.len(), 0);
}

#[test]
fn fn_label_replace_join_del() {
    // label_replace copies host label into dst with regex capture.
    let data = mk_data();
    let _ = data; // labels only have `service`
    let v = eval_query(
        r#"label_replace(requests, "svc", "$1-x", "service", "(.*)")"#,
        95_000_000_000,
    );
    assert_eq!(v.series.len(), 2);
    assert_eq!(
        v.series[0].0.get("svc").map(|s| s.to_string()),
        Some("a-x".to_string())
    );
    // label_join concatenates.
    let v = eval_query(
        r#"label_join(requests, "combo", "-", "service")"#,
        95_000_000_000,
    );
    assert_eq!(
        v.series[0].0.get("combo").map(|s| s.to_string()),
        Some("a".to_string())
    );
    // label_del removes.
    let v = eval_query(r#"label_del(requests, "service")"#, 95_000_000_000);
    assert!(v.series[0].0.get("service").is_none());
}

#[test]
fn fn_sort_family() {
    // Build a metric with 3 distinct values.
    let mut data = mk_data();
    let mk =
        |n: &str| LabelSet::try_from_iter(vec![("service".to_string(), n.to_string())]).unwrap();
    data.insert(
        "vals".into(),
        vec![
            (
                crate::ast::shared_labels(mk("a")),
                vec![(95_000_000_000, 30.0)],
            ),
            (
                crate::ast::shared_labels(mk("b")),
                vec![(95_000_000_000, 10.0)],
            ),
            (
                crate::ast::shared_labels(mk("c")),
                vec![(95_000_000_000, 20.0)],
            ),
        ],
    );
    let ev = Evaluator::new(&data);
    let expr = crate::parser::parse_expr("sort(vals)").unwrap();
    let ctx = EvalContext {
        ts_ns: 95_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    let vals: Vec<f64> = v.series.iter().map(|(_, x)| *x).collect();
    assert_eq!(vals, vec![10.0, 20.0, 30.0]);
    let expr = crate::parser::parse_expr("sort_desc(vals)").unwrap();
    let v = ev.eval(&expr, ctx).unwrap();
    let vals: Vec<f64> = v.series.iter().map(|(_, x)| *x).collect();
    assert_eq!(vals, vec![30.0, 20.0, 10.0]);
    // sort_by_label: ascending service name a,b,c → values 30,10,20.
    let expr = crate::parser::parse_expr(r#"sort_by_label(vals, "service")"#).unwrap();
    let v = ev.eval(&expr, ctx).unwrap();
    let vals: Vec<f64> = v.series.iter().map(|(_, x)| *x).collect();
    assert_eq!(vals, vec![30.0, 10.0, 20.0]);
    let expr = crate::parser::parse_expr(r#"sort_by_label_desc(vals, "service")"#).unwrap();
    let v = ev.eval(&expr, ctx).unwrap();
    let vals: Vec<f64> = v.series.iter().map(|(_, x)| *x).collect();
    assert_eq!(vals, vec![20.0, 10.0, 30.0]);
}

#[test]
fn fn_range_over_time_family() {
    // t=95s over a 1m window: samples at 40..90s (6 points, values 4..9).
    let t = 95_000_000_000;
    assert_eq!(scalar("count_over_time(requests[1m])", t), 6.0);
    assert!((scalar("avg_over_time(requests[1m])", t) - 6.5).abs() < 1e-9);
    assert_eq!(scalar("min_over_time(requests[1m])", t), 4.0);
    assert_eq!(scalar("max_over_time(requests[1m])", t), 9.0);
    assert_eq!(scalar("sum_over_time(requests[1m])", t), 39.0);
    assert_eq!(scalar("last_over_time(requests[1m])", t), 9.0);
    assert_eq!(scalar("present_over_time(requests[1m])", t), 1.0);
    // stdvar/stddev: values 4..9, mean 6.5, squared-dev sum 17.5/6.
    assert!((scalar("stdvar_over_time(requests[1m])", t) - 17.5 / 6.0).abs() < 1e-6);
    assert!((scalar("stddev_over_time(requests[1m])", t) - (17.5f64 / 6.0).sqrt()).abs() < 1e-6);
}

#[test]
fn fn_range_counter_family() {
    let t = 95_000_000_000;
    // rate over 1m at t=95s: samples 40..90s (values 4..9) → 5/50s = 0.1/s.
    assert!(
        (scalar("rate(requests[1m])", t) - 0.1).abs() < 0.02,
        "rate = 0.1/s"
    );
    // increase ≈ rate × window ≈ 0.1 × 60 ≈ 6 (± extrapolation).
    assert!(
        (scalar("increase(requests[1m])", t) - 6.0).abs() < 1.5,
        "increase"
    );
    // delta: 9-4 = 5.
    assert_eq!(scalar("delta(requests[1m])", t), 5.0);
    // idelta: last pair 8→9 → 1.
    assert_eq!(scalar("idelta(requests[1m])", t), 1.0);
    // irate: last interval 10s × 1 unit → 0.1/s.
    assert!(
        (scalar("irate(requests[1m])", t) - 0.1).abs() < 1e-9,
        "irate = 0.1/s"
    );
}

/// The aggregation group key is now a hash, so a collision would silently
/// merge two groups. These cover the cases where the projection looks
/// similar but must stay distinct.
/// Streaming must not change what is produced, only how much is resident
/// at once. Asserts `eval_steps_into` and `eval_steps` agree exactly.
#[test]
fn eval_steps_into_matches_collecting_eval_steps() {
    let mk = |v: &str| {
        LabelSet::try_from_iter(vec![
            ("host".to_string(), "h1".to_string()),
            ("dc".to_string(), v.to_string()),
        ])
        .unwrap()
    };
    let mut data = SeriesData::new();
    data.insert(
        "m".into(),
        vec![
            (
                crate::ast::shared_labels(mk("a")),
                vec![(0, 1.0), (10_000_000_000, 2.0), (20_000_000_000, 4.0)],
            ),
            (
                crate::ast::shared_labels(mk("b")),
                vec![(0, 10.0), (10_000_000_000, 20.0), (20_000_000_000, 40.0)],
            ),
        ],
    );
    let eval = Evaluator::new(&data);
    let expr = crate::parser::parse_expr("sum by (dc) (m)").unwrap();

    let collected = eval
        .eval_steps(&expr, 0, 30_000_000_000, 10_000_000_000)
        .unwrap();

    let mut streamed = Vec::new();
    eval.eval_steps_into(&expr, 0, 30_000_000_000, 10_000_000_000, |ts, v| {
        streamed.push((ts, v));
        Ok(())
    })
    .unwrap();

    assert_eq!(streamed.len(), collected.len(), "same step count");
    for ((ts_a, a), (ts_b, b)) in streamed.iter().zip(collected.iter()) {
        assert_eq!(ts_a, ts_b);
        assert_eq!(a.series.len(), b.series.len());
        for ((la, va), (lb, vb)) in a.series.iter().zip(b.series.iter()) {
            assert_eq!(la.as_ref(), lb.as_ref(), "labels must match");
            assert_eq!(va, vb, "values must match");
        }
    }
    assert_eq!(streamed.len(), 3, "steps at 0/10/20ms");
}

/// `eval_steps_into` must surface a sink error rather than swallowing it,
/// otherwise a consumer that rejects a step would silently get a truncated
/// result.
#[test]
fn eval_steps_into_propagates_sink_errors() {
    let mut data = SeriesData::new();
    data.insert(
        "m".into(),
        vec![(
            crate::ast::shared_labels(
                LabelSet::try_from_iter(vec![("a".to_string(), "1".to_string())]).unwrap(),
            ),
            vec![(0, 1.0), (10_000_000_000, 2.0)],
        )],
    );
    let eval = Evaluator::new(&data);
    let expr = crate::parser::parse_expr("m").unwrap();

    let mut seen = 0usize;
    let result: Result<()> =
        eval.eval_steps_into(&expr, 0, 30_000_000_000, 10_000_000_000, |_ts, _v| {
            seen += 1;
            if seen == 2 {
                return Err(Error::Validation("sink said no".into()));
            }
            Ok(())
        });
    assert!(result.is_err(), "a sink error must abort the evaluation");
    assert_eq!(seen, 2, "the sink must stop being called after it errors");
}

#[test]
fn grouping_fingerprint_separates_distinct_projections() {
    let ls = |pairs: &[(&str, &str)]| {
        LabelSet::try_from_iter(pairs.iter().map(|(k, v)| (k.to_string(), v.to_string()))).unwrap()
    };
    let by = |l: &[&str]| Grouping::By(l.iter().map(|s| s.to_string()).collect());

    let a = ls(&[("a", "1"), ("b", "2")]);
    let b = ls(&[("a", "1"), ("b", "3")]);
    assert_ne!(
        grouping_fingerprint(&a, &by(&["a", "b"])),
        grouping_fingerprint(&b, &by(&["a", "b"])),
        "different values must not collide"
    );

    // A missing label is part of the key: `by(a)` with a="1" and with a
    // absent must not collapse into one group.
    let missing = ls(&[("b", "2")]);
    assert_ne!(
        grouping_fingerprint(&a, &by(&["a"])),
        grouping_fingerprint(&missing, &by(&["a"])),
        "absent label must be distinguished from a present one"
    );

    // The same projection from differently-ordered input sets must agree.
    let reordered = ls(&[("b", "2"), ("a", "1")]);
    assert_eq!(
        grouping_fingerprint(&a, &by(&["a", "b"])),
        grouping_fingerprint(&reordered, &by(&["a", "b"])),
        "label order in the source set must not matter"
    );

    // `by(a,b)` and `by(b,a)` select the same pairs and must agree. The
    // hot loop cannot sort per series, so `eval_aggregation` normalises the
    // list once per call; this asserts that contract.
    assert_eq!(
        grouping_fingerprint(&a, &normalize_grouping(&by(&["a", "b"]))),
        grouping_fingerprint(&a, &normalize_grouping(&by(&["b", "a"]))),
        "selecting the same pairs in a different declared order must agree"
    );

    // `without` excludes labels; two sets differing only in an excluded
    // label belong to the same group.
    let without_a = |l: &[&str]| Grouping::Without(l.iter().map(|s| s.to_string()).collect());
    let with_extra = ls(&[("a", "1"), ("b", "2"), ("z", "9")]);
    assert_eq!(
        grouping_fingerprint(&a, &without_a(&["a", "z"])),
        grouping_fingerprint(&with_extra, &without_a(&["a", "z"])),
        "an excluded label must not split a group"
    );

    // Grouping::None puts everything in one group.
    assert_eq!(
        grouping_fingerprint(&a, &Grouping::None),
        grouping_fingerprint(&b, &Grouping::None)
    );
}

/// A group's labels are built once and reused across steps; the values must
/// still be recomputed per step. This pins the two halves of that split.
#[test]
fn group_labels_are_cached_across_steps_but_values_are_not() {
    let mk = |v: &str| {
        LabelSet::try_from_iter(vec![
            ("host".to_string(), "h1".to_string()),
            ("dc".to_string(), v.to_string()),
        ])
        .unwrap()
    };
    let mut data = SeriesData::new();
    data.insert(
        "m".into(),
        vec![
            (
                crate::ast::shared_labels(mk("a")),
                vec![(0, 1.0), (10_000_000_000, 2.0)],
            ),
            (
                crate::ast::shared_labels(mk("b")),
                vec![(0, 10.0), (10_000_000_000, 20.0)],
            ),
        ],
    );
    let eval = Evaluator::new(&data);
    let expr = crate::parser::parse_expr("sum by (dc) (m)").unwrap();

    let steps = eval
        .eval_steps(&expr, 0, 20_000_000_000, 10_000_000_000)
        .unwrap();
    assert_eq!(steps.len(), 2, "two steps");
    for (_, iv) in &steps {
        assert_eq!(iv.series.len(), 2, "one group per dc value");
    }
    // Values differ per step; labels do not.
    let v0: Vec<f64> = steps[0].1.series.iter().map(|(_, v)| *v).collect();
    let v1: Vec<f64> = steps[1].1.series.iter().map(|(_, v)| *v).collect();
    assert_ne!(v0, v1, "each step must recompute the aggregate");
    assert_eq!(
        steps[0].1.series[0].0.as_ref(),
        steps[1].1.series[0].0.as_ref(),
        "group labels must be identical across steps"
    );
    // The cache holds one entry per distinct group.
    assert_eq!(eval.group_cache.lock().unwrap().len(), 2);
}

#[test]
fn fn_range_changes_resets_deriv() {
    // Series with a reset and a change:
    let mut data = SeriesData::new();
    let ls = LabelSet::try_from_iter(vec![("service".to_string(), "a".to_string())]).unwrap();
    // values: 1,2,3 (rising), 1 (reset), 2 (rising)
    data.insert(
        "cnt".into(),
        vec![(
            crate::ast::shared_labels(ls.clone()),
            vec![
                (0, 1.0),
                (10_000_000_000, 2.0),
                (20_000_000_000, 3.0),
                (30_000_000_000, 1.0),
                (40_000_000_000, 2.0),
            ],
        )],
    );
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 45_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let e = crate::parser::parse_expr("changes(cnt[1m])").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert_eq!(v.series[0].1, 4.0, "4 value changes");
    let e = crate::parser::parse_expr("resets(cnt[1m])").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert_eq!(v.series[0].1, 1.0, "1 counter reset");
    let e = crate::parser::parse_expr("deriv(cnt[1m])").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    // (2-1)/40s = 0.025/s overall slope
    assert!((v.series[0].1 - 0.025).abs() < 1e-9, "deriv");
}

#[test]
fn fn_range_quantile_mad() {
    let t = 95_000_000_000;
    // Window values 4..9 → median 6.5, q0.5 interp, q1 = 9.
    assert!((scalar("quantile_over_time(0.5, requests[1m])", t) - 6.5).abs() < 1e-9);
    assert!((scalar("quantile_over_time(1, requests[1m])", t) - 9.0).abs() < 1e-9);
    // mad: deviations |4..9 - 6.5| = 2.5,1.5,.5,.5,1.5,2.5 → median 1.5? sort: .5,.5,1.5,1.5,2.5,2.5 → 1.5
    assert!(
        (scalar("mad_over_time(requests[1m])", t) - 1.5).abs() < 1e-9,
        "mad"
    );
}

#[test]
fn fn_range_predict_holt() {
    let t = 95_000_000_000;
    // Slope 0.1/s from t0=40s (value 4). Predict at 95+60=155s:
    // x = 155-40 = 115 → 4 + 0.1×115 = 15.5.
    let p = scalar("predict_linear(requests[1m], 60)", t);
    assert!((p - 15.5).abs() < 1.0, "predict_linear ≈ 15.5, got {p}");
    // holt_winters over a linear ramp with sf=0.9 tf=0.1 → near last.
    let hw = scalar("holt_winters(requests[1m], 0.9, 0.1)", t);
    assert!(hw.is_finite());
    assert!(hw > 5.0 && hw < 15.0, "holt_winters sanity, got {hw}");
    // double_exponential_smoothing is the documented alias.
    let ds = scalar("double_exponential_smoothing(requests[1m], 0.9, 0.1)", t);
    assert!((ds - hw).abs() < 1e-12, "alias identical");
}

#[test]
fn fn_histogram_classic_quantile() {
    // Classic le-bucket series.
    let mut data = SeriesData::new();
    let mk = |le: &str| {
        LabelSet::try_from_iter(vec![
            ("le".to_string(), le.to_string()),
            ("service".to_string(), "a".to_string()),
        ])
        .unwrap()
    };
    data.insert(
        "lat_bucket".into(),
        vec![
            (
                crate::ast::shared_labels(mk("1")),
                vec![(95_000_000_000, 10.0)],
            ),
            (
                crate::ast::shared_labels(mk("5")),
                vec![(95_000_000_000, 40.0)],
            ),
            (
                crate::ast::shared_labels(mk("+Inf")),
                vec![(95_000_000_000, 50.0)],
            ),
        ],
    );
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 95_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    // q0.5 of 50 obs: target 25 falls in le=5 bucket (10..40): 1 + (25-10)/(30)*4 = 3.0
    let e = crate::parser::parse_expr("histogram_quantile(0.5, lat_bucket)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert_eq!(v.series.len(), 1);
    assert!((v.series[0].1 - 3.0).abs() < 1e-9, "classic quantile");
}

#[test]
fn fn_histogram_native_family() {
    // Native OTLP explicit-bucket histograms.
    let mk_hist = |ts: i64| crate::ast::HistSample {
        timestamp_ns: ts,
        count: 50,
        sum: 150.0,
        boundaries: vec![1.0, 5.0, 10.0],
        counts: vec![10, 30, 10], // +10 in +Inf bucket
    };
    let mut hist = crate::ast::HistData::new();
    let ls = LabelSet::try_from_iter(vec![("service".to_string(), "a".to_string())]).unwrap();
    hist.insert(
        "lat".into(),
        vec![(crate::ast::shared_labels(ls), vec![mk_hist(95_000_000_000)])],
    );
    // SeriesData must still exist for the same selector to be scanned
    // as numeric — histogram series live in hist only; give it an
    // empty numeric series so refs resolve.
    let mut data = SeriesData::new();
    data.insert("lat".into(), vec![]);
    let ev = Evaluator::new(&data).with_hist_data(&hist);
    let ctx = EvalContext {
        ts_ns: 95_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };

    let e = crate::parser::parse_expr("histogram_count(lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert_eq!(v.series.len(), 1);
    assert_eq!(v.series[0].1, 50.0, "histogram_count");

    let e = crate::parser::parse_expr("histogram_sum(lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert_eq!(v.series[0].1, 150.0, "histogram_sum");

    let e = crate::parser::parse_expr("histogram_avg(lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert!((v.series[0].1 - 3.0).abs() < 1e-9, "histogram_avg");

    // q0.5 of 50: target 25 in bucket [1,5) (10..40): 1 + (25-10)/30*4 = 3.0
    let e = crate::parser::parse_expr("histogram_quantile(0.5, lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert_eq!(v.series.len(), 1, "native histogram_quantile");
    assert!((v.series[0].1 - 3.0).abs() < 1e-9, "native quantile = 3.0");

    // fraction in [1, 5): 30/50 = 0.6
    let e = crate::parser::parse_expr("histogram_fraction(1, 5, lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert!((v.series[0].1 - 0.6).abs() < 1e-9, "fraction [1,5)");

    // stdvar over midpoints 0.5(10),3(30),7.5(10): mean=3.3, var.
    let e = crate::parser::parse_expr("histogram_stdvar(lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    let mu: f64 = (0.5 * 10.0 + 3.0 * 30.0 + 7.5 * 10.0) / 50.0;
    let var: f64 =
        (10.0 * (0.5 - mu).powi(2) + 30.0 * (3.0 - mu).powi(2) + 10.0 * (7.5 - mu).powi(2)) / 50.0;
    assert!((v.series[0].1 - var).abs() < 1e-9, "histogram_stdvar");
    let e = crate::parser::parse_expr("histogram_stddev(lat)").unwrap();
    let v = ev.eval(&e, ctx).unwrap();
    assert!(
        (v.series[0].1 - var.sqrt()).abs() < 1e-9,
        "histogram_stddev"
    );
}

#[test]
fn fn_string_literal_errors_in_value_position() {
    // String in value position → validation error, not silent NaN.
    let r = crate::parser::parse_expr(r#""hello""#);
    assert!(r.is_ok());
    let data = mk_data();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 0,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let e = r.unwrap();
    let out = ev.eval(&e, ctx);
    assert!(out.is_err(), "string literal in value position errors");
}

#[test]
fn fn_count_values_string_param() {
    // count_values("mode", expr) — the label name must come through as
    // the string literal, not "NaN".
    let mut data = SeriesData::new();
    let mk =
        |n: &str| LabelSet::try_from_iter(vec![("service".to_string(), n.to_string())]).unwrap();
    data.insert(
        "vals".into(),
        vec![
            (
                crate::ast::shared_labels(mk("a")),
                vec![(95_000_000_000, 1.0)],
            ),
            (
                crate::ast::shared_labels(mk("b")),
                vec![(95_000_000_000, 1.0)],
            ),
            (
                crate::ast::shared_labels(mk("c")),
                vec![(95_000_000_000, 2.0)],
            ),
        ],
    );
    let ev = Evaluator::new(&data);
    let expr = crate::parser::parse_expr(r#"count_values("mode", vals)"#).unwrap();
    let ctx = EvalContext {
        ts_ns: 95_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    assert_eq!(v.series.len(), 2, "two distinct values");
    // The param label must be named "mode" and carry the value.
    for (labels, _) in &v.series {
        let mode = labels.get("mode").map(|s| s.to_string());
        assert!(
            mode == Some("1".to_string()) || mode == Some("2".to_string()),
            "label mode = {mode:?}"
        );
    }
}

#[test]
fn nested_sum_rate() {
    // sum(rate(requests[1m])) at t=100s: rate=1/sec per series, summed=2
    let v = eval_query("sum(rate(requests[1m]))", 100_000_000_000);
    assert_eq!(v.series.len(), 1);
    let val = v.series[0].1;
    // window [40s,100s): 6 samples, counter 4->9, rate=0.1/s per series,
    // sum across 2 series ≈ 0.2 (plus extrapolation slack).
    assert!((val - 0.2).abs() < 0.06, "sum(rate) = {val}");
}

#[test]
fn sum_by_service() {
    let v = eval_query("sum by (service) (requests)", 95_000_000_000);
    assert_eq!(v.series.len(), 2);
}

#[test]
fn binary_ratio() {
    // requests / requests = 1.0 per series
    let v = eval_query("requests / requests", 95_000_000_000);
    assert_eq!(v.series.len(), 2);
    for (_, val) in &v.series {
        assert!((val - 1.0).abs() < 1e-9);
    }
}

#[test]
fn scalar_arith() {
    let v = eval_query("requests * 2", 95_000_000_000);
    assert!(!v.is_empty());
    let (_, val) = &v.series[0];
    assert!((val - 18.0).abs() < 1e-9, "9*2=18, got {val}");
}

#[test]
fn avg_over_time_window() {
    // avg over [1m] ending at 100s: samples at 40..100 = 4..9, avg 6.5
    let v = eval_query("avg_over_time(requests[1m])", 100_000_000_000);
    assert_eq!(v.series.len(), 2);
    let (_, val) = &v.series[0];
    assert!((val - 6.5).abs() < 1e-9, "avg_over_time = {val}");
}

#[test]
fn max_over_time_family() {
    let v = eval_query("max_over_time(requests[1m])", 100_000_000_000);
    let (_, val) = &v.series[0];
    assert!((val - 9.0).abs() < 1e-9);
}

#[test]
fn count_over_time() {
    let v = eval_query("count_over_time(requests[1m])", 100_000_000_000);
    let (_, val) = &v.series[0];
    assert!(
        (val - 6.0).abs() < 1e-9,
        "6 samples in [40s,100s), got {val}"
    );
}

#[test]
fn comparison_filters() {
    // only series with value > 8 at t=95: value is 9 (last sample ≤95s is at 90s=9)
    let v = eval_query("requests > 8", 95_000_000_000);
    assert_eq!(v.series.len(), 2);
    let v = eval_query("requests > 8.5", 95_000_000_000);
    assert_eq!(v.series.len(), 2);
    let v = eval_query("requests > 9.5", 95_000_000_000);
    assert_eq!(v.series.len(), 0);
}

#[test]
fn bool_comparison() {
    let v = eval_query("requests > bool 5", 95_000_000_000);
    assert_eq!(v.series.len(), 2);
    for (_, val) in v.series {
        assert_eq!(val, 1.0);
    }
}

#[test]
fn on_projection_drops_non_on_labels() {
    // a * on(service) b keeps ONLY service (+ group_left extras),
    // dropping pod/instance from the result — PromQL semantics.
    let mut data = SeriesData::new();
    let l1 = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("pod".to_string(), "p1".to_string()),
    ])
    .unwrap();
    let l2 = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("pod".to_string(), "p2".to_string()),
    ])
    .unwrap();
    data.insert(
        "m1".into(),
        vec![(
            (crate::ast::shared_labels(l1.clone())),
            vec![(0, 2.0), (100_000_000_000, 2.0)],
        )],
    );
    data.insert(
        "m2".into(),
        vec![(
            (crate::ast::shared_labels(l2.clone())),
            vec![(0, 3.0), (100_000_000_000, 3.0)],
        )],
    );
    let expr = crate::parser::parse_expr("m1 * on(service) m2").unwrap();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 100_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    assert_eq!(v.series.len(), 1);
    let (labels, val) = &v.series[0];
    assert!((*val - 6.0).abs() < 1e-9);
    // ONLY the on-label survives.
    assert_eq!(
        labels.get("service").map(|s| s.to_string()).as_deref(),
        Some("a")
    );
    assert!(
        labels.get("pod").is_none(),
        "pod must be dropped by on() projection"
    );
}

#[test]
fn vector_matching_on() {
    // Two metrics sharing service label; join via on(service)
    let mut data = SeriesData::new();
    let la = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("instance".to_string(), "1".to_string()),
    ])
    .unwrap();
    let lb = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("instance".to_string(), "2".to_string()),
    ])
    .unwrap();
    data.insert(
        "m1".into(),
        vec![(
            (crate::ast::shared_labels(la.clone())),
            vec![(0, 10.0), (100_000_000_000, 10.0)],
        )],
    );
    data.insert(
        "m2".into(),
        vec![(
            (crate::ast::shared_labels(lb.clone())),
            vec![(0, 5.0), (100_000_000_000, 5.0)],
        )],
    );
    let expr = crate::parser::parse_expr("m1 * on(service) m2").unwrap();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 100_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    assert_eq!(v.series.len(), 1);
    let (_, val) = &v.series[0];
    assert!((val - 50.0).abs() < 1e-9, "10*5=50, got {val}");
}

#[test]
fn group_left_many_to_one() {
    let mut data = SeriesData::new();
    let s1 = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("pod".to_string(), "p1".to_string()),
    ])
    .unwrap();
    let s2 = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("pod".to_string(), "p2".to_string()),
    ])
    .unwrap();
    let info = LabelSet::try_from_iter(vec![
        ("service".to_string(), "a".to_string()),
        ("team".to_string(), "core".to_string()),
    ])
    .unwrap();
    data.insert(
        "metric".into(),
        vec![
            (
                crate::ast::shared_labels(s1.clone()),
                vec![(0, 1.0), (100_000_000_000, 1.0)],
            ),
            (
                crate::ast::shared_labels(s2.clone()),
                vec![(0, 2.0), (100_000_000_000, 2.0)],
            ),
        ],
    );
    data.insert(
        "info".into(),
        vec![(
            crate::ast::shared_labels(info),
            vec![(0, 100.0), (100_000_000_000, 100.0)],
        )],
    );
    let expr = crate::parser::parse_expr("metric * on(service) group_left(team) info").unwrap();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 100_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    assert_eq!(v.series.len(), 2, "both pods match");
    for (labels, val) in &v.series {
        assert_eq!(
            labels.get("team").map(|s| s.to_string()).as_deref(),
            Some("core")
        );
        assert!(*val == 100.0 || *val == 200.0);
    }
}

#[test]
fn absent_emits_one() {
    let v = eval_query("absent(nonexistent{job=\"x\"})", 50_000_000_000);
    assert_eq!(v.series.len(), 1);
    let (labels, val) = &v.series[0];
    assert_eq!(*val, 1.0);
    assert_eq!(
        labels.get("job").map(|s| s.to_string()).as_deref(),
        Some("x")
    );
}

#[test]
fn topk_returns_top_series() {
    let mut data = SeriesData::new();
    for i in 0..5 {
        let l = LabelSet::try_from_iter(vec![("i".to_string(), i.to_string())]).unwrap();
        data.entry("m".into()).or_default().push((
            crate::ast::shared_labels(l),
            vec![(0, i as f64), (100_000_000_000, i as f64)],
        ));
    }
    let expr = crate::parser::parse_expr("topk(2, m)").unwrap();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 100_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    assert_eq!(v.series.len(), 2);
    let vals: Vec<f64> = v.series.iter().map(|(_, v)| *v).collect();
    assert!(vals.contains(&4.0) && vals.contains(&3.0));
}

#[test]
fn histogram_quantile_from_buckets() {
    let mut data = SeriesData::new();
    // classic histogram buckets for one series
    for (le, count) in [
        (0.1, 10.0),
        (1.0, 50.0),
        (10.0, 90.0),
        (f64::INFINITY, 100.0),
    ] {
        let l = LabelSet::try_from_iter(vec![
            ("le".to_string(), le.to_string()),
            ("route".to_string(), "r".to_string()),
        ])
        .unwrap();
        data.entry("latency_bucket".into()).or_default().push((
            crate::ast::shared_labels(l),
            vec![(0, count), (100_000_000_000, count)],
        ));
    }
    let expr =
        crate::parser::parse_expr("histogram_quantile(0.9, sum by (le, route) (latency_bucket))")
            .unwrap();
    let ev = Evaluator::new(&data);
    let ctx = EvalContext {
        ts_ns: 100_000_000_000,
        range_ns: 0,
        offset_ns: 0,
        subquery_step_ns: None,
    };
    let v = ev.eval(&expr, ctx).unwrap();
    assert_eq!(v.series.len(), 1);
    let (_, qv) = &v.series[0];
    // 90th of 100 obs falls in the (1.0, 10.0] bucket: 1 + (90-50)/(90-50)*9 = 10...
    // count>=90 first at le=10.0: 1.0 + (90-50)/(90-50)*(10-1)=10.0 → cap semantics: ~10
    assert!(
        (qv - 10.0).abs() < 1e-6 || (qv - 9.0).abs() < 2.0,
        "q90 = {qv}"
    );
}

#[test]
fn set_ops_and_or() {
    let v = eval_query(
        r#"requests{service="a"} and requests{service="b"}"#,
        95_000_000_000,
    );
    assert!(v.is_empty(), "no common label sets");
    let v = eval_query(
        r#"requests{service="a"} or requests{service="b"}"#,
        95_000_000_000,
    );
    assert_eq!(v.series.len(), 2);
    let v = eval_query(
        r#"requests{service="a"} unless requests{service="b"}"#,
        95_000_000_000,
    );
    assert_eq!(v.series.len(), 1);
}
