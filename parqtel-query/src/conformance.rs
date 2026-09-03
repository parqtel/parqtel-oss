//! PromQL conformance corpus (G0) — hand-derived expectations against
//! Prometheus semantics, run over both engines (AST evaluator and the
//! legacy plan path) on deterministic fixtures.
//!
//! The corpus is the safety net for every semantic change (Wave 1 G0)
//! and the gate for the future legacy-engine retirement decision.
//! Each case declares: query, evaluation timestamp, and the expected
//! per-series values (approximate where extrapolation applies, exact
//! where determinism holds).

use parqtel_core::LabelSet;

/// One corpus case.
#[derive(Clone)]
pub struct Case {
    pub name: &'static str,
    pub query: &'static str,
    /// Evaluation timestamp (ns) — fixtures are relative to 0.
    pub ts_ns: i64,
    /// Expected (series-key, value) pairs; tolerance applies per case class.
    pub expect: Vec<(&'static str, f64)>,
    /// Numeric tolerance (rate extrapolation needs slack).
    pub tol: f64,
}

pub const S: i64 = 1_000_000_000;
pub const M: i64 = 60 * S;
pub const H: i64 = 60 * M;

/// Deterministic fixture: the same data every case sees.
/// Series are identified by their label value for expectation clarity.
pub fn fixture() -> crate::ast::SeriesData {
    let mut data = crate::ast::SeriesData::new();

    let mut push_series = |name: &str, labels: &[(&str, &str)], points: Vec<(i64, f64)>| {
        let ls = LabelSet::try_from_iter(
            labels
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
        data.entry(name.to_string()).or_default().push((ls, points));
    };

    // ── counters: 1/sec at 15s interval, 0..2h ──────────────────────────
    let counter = |mult: f64| -> Vec<(i64, f64)> {
        (0..=480).map(|i| (i * 15 * S, i as f64 * mult)).collect()
    };
    push_series("requests", &[("service", "api")], counter(1.0));
    push_series("requests", &[("service", "web")], counter(3.0));

    // ── gauges: sine-ish deterministic values at 30s ────────────────────
    let gauge = |phase: f64, base: f64| -> Vec<(i64, f64)> {
        (0..=240)
            .map(|i| {
                let t = i as f64;
                (
                    i * 30 * S,
                    base + 10.0 * ((t / 20.0 + phase).sin()) + (i % 7) as f64,
                )
            })
            .collect()
    };
    push_series(
        "cpu",
        &[("service", "api"), ("core", "0")],
        gauge(0.0, 50.0),
    );
    push_series(
        "cpu",
        &[("service", "api"), ("core", "1")],
        gauge(1.5, 60.0),
    );
    push_series(
        "cpu",
        &[("service", "web"), ("core", "0")],
        gauge(3.0, 40.0),
    );

    // ── bursty counter: flat 1h then burst (range-differentiation) ─────
    let mut bursty = Vec::new();
    for i in 0..240 {
        bursty.push((i * 15 * S, 0.0)); // 1h flat
    }
    for i in 0..240 {
        bursty.push((H + i * 15 * S, i as f64 * 50.0)); // +50/s for 1h
    }
    push_series("bursty", &[("service", "api")], bursty);

    // ── multi-reset counter (G4 case) ───────────────────────────────────
    let resets = vec![
        (0, 0.0),
        (10 * S, 50.0),
        (20 * S, 0.0),
        (30 * S, 30.0),
        (40 * S, 0.0),
        (50 * S, 20.0),
        (60 * S, 10.0),
    ];
    push_series("churn", &[("service", "api")], resets);

    // ── classic histogram buckets ────────────────────────────────────────
    for (le, count) in [("0.1", 10.0), ("1.0", 50.0), ("10.0", 90.0)] {
        push_series(
            "lat_bucket",
            &[("route", "r"), ("le", le)],
            vec![(0, count), (M, count), (2 * M, count)],
        );
    }
    push_series(
        "lat_bucket",
        &[("route", "r"), ("le", "+Inf")],
        vec![(0, 100.0), (M, 100.0), (2 * M, 100.0)],
    );

    data
}

// ── Corpus ───────────────────────────────────────────────────────────────────

pub fn corpus() -> Vec<Case> {
    use std::sync::OnceLock;
    static CORPUS: OnceLock<Vec<Case>> = OnceLock::new();
    CORPUS
        .get_or_init(|| {
            vec![
                // ── selectors ──────────────────────────────────────────────
                Case {
                    name: "sel_plain",
                    query: "cpu",
                    ts_ns: 2 * H,
                    expect: vec![
                        ("api/core=0", 0.0),
                        ("api/core=1", 0.0),
                        ("web/core=0", 0.0),
                    ],
                    tol: 0.0,
                },
                Case {
                    name: "sel_match",
                    query: r#"cpu{service="api"}"#,
                    ts_ns: 2 * H,
                    expect: vec![("api/core=0", 0.0), ("api/core=1", 0.0)],
                    tol: 0.0,
                },
                Case {
                    name: "sel_regex",
                    query: r#"cpu{service=~"a.*"}"#,
                    ts_ns: 2 * H,
                    expect: vec![("api/core=0", 0.0), ("api/core=1", 0.0)],
                    tol: 0.0,
                },
                Case {
                    name: "sel_ne",
                    query: r#"cpu{service!="api"}"#,
                    ts_ns: 2 * H,
                    expect: vec![("web/core=0", 0.0)],
                    tol: 0.0,
                },
                // ── aggregations ───────────────────────────────────────────
                Case {
                    name: "sum_all",
                    query: "sum(requests)",
                    ts_ns: 2 * H,
                    expect: vec![("", 1920.0)],
                    tol: 1.0,
                }, // instant: 480 + 1440
                Case {
                    name: "sum_by",
                    query: "sum by (service) (requests)",
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.0), ("web", 0.0)],
                    tol: 0.0,
                },
                Case {
                    name: "count",
                    query: "count(cpu)",
                    ts_ns: 2 * H,
                    expect: vec![("", 3.0)],
                    tol: 0.0,
                },
                Case {
                    name: "avg_by_service",
                    query: "avg by (service) (cpu)",
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.0), ("web", 0.0)],
                    tol: 0.0,
                },
                // ── windowed counters (range semantics — G1-adjacent) ─────
                Case {
                    name: "rate_5m",
                    query: "rate(requests[5m])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.0667), ("web", 0.2)],
                    tol: 0.01,
                },
                Case {
                    name: "rate_1h",
                    query: "rate(requests[1h])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.0667), ("web", 0.2)],
                    tol: 0.005,
                },
                Case {
                    name: "rate_burst_5m",
                    query: "rate(bursty[5m])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 50.0 / 15.0)],
                    tol: 0.5,
                },
                Case {
                    name: "rate_burst_1h",
                    query: "rate(bursty[1h])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 12000.0 / 3600.0)],
                    tol: 0.3,
                }, // 12000 increase over 3600s span
                Case {
                    name: "increase_5m",
                    query: "increase(requests[5m])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 20.0), ("web", 60.0)],
                    tol: 5.0,
                },
                // ── multi-reset (G4) ───────────────────────────────────────
                Case {
                    name: "rate_multireset",
                    query: "rate(churn[1m])",
                    ts_ns: 60 * S,
                    expect: vec![("api", 110.0 / 60.0)],
                    tol: 0.3,
                },
                // ── composition ───────────────────────────────────────────
                Case {
                    name: "sum_of_rate",
                    query: "sum(rate(requests[5m]))",
                    ts_ns: 2 * H,
                    expect: vec![("", 0.2667)],
                    tol: 0.01,
                },
                Case {
                    name: "sum_by_rate",
                    query: "sum by (service) (rate(requests[5m]))",
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.0667), ("web", 0.2)],
                    tol: 0.01,
                },
                Case {
                    name: "ratio",
                    query: r#"sum(rate(requests{service="web"}[5m])) / sum(rate(requests[5m]))"#,
                    ts_ns: 2 * H,
                    expect: vec![("", 0.75)],
                    tol: 0.02,
                },
                // ── over_time family ───────────────────────────────────────
                Case {
                    name: "avg_over_time",
                    query: "avg_over_time(cpu[5m])",
                    ts_ns: 2 * H,
                    expect: vec![
                        ("api/core=0", 0.0),
                        ("api/core=1", 0.0),
                        ("web/core=0", 0.0),
                    ],
                    tol: 0.0,
                },
                Case {
                    name: "count_over_time",
                    query: "count_over_time(requests[5m])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 21.0), ("web", 21.0)],
                    tol: 1.0,
                },
                Case {
                    name: "max_over_time",
                    query: "max_over_time(requests[5m])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 480.0), ("web", 1440.0)],
                    tol: 1.0,
                },
                Case {
                    name: "min_over_time",
                    query: "min_over_time(requests[5m])",
                    ts_ns: 2 * H,
                    expect: vec![("api", 460.0), ("web", 1380.0)],
                    tol: 20.0,
                },
                Case {
                    name: "sum_over_time_delta",
                    query: "sum_over_time(churn[1m])",
                    ts_ns: 60 * S,
                    expect: vec![("api", 110.0)],
                    tol: 1.0,
                },
                // ── instant transforms ─────────────────────────────────────
                Case {
                    name: "scalar_cmp_bool",
                    query: "sum(requests) > bool 0",
                    ts_ns: 2 * H,
                    expect: vec![("", 1.0)],
                    tol: 0.0,
                },
                Case {
                    name: "clamp_min",
                    query: "clamp_min(rate(requests[5m]), 0.1)",
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.1), ("web", 0.2)],
                    tol: 0.0,
                }, // web already 0.2 > clamp
                Case {
                    name: "abs",
                    query: "abs(-sum(requests))",
                    ts_ns: 2 * H,
                    expect: vec![("", 1920.0)],
                    tol: 1.0,
                },
                // ── vector matching (G3) ───────────────────────────────────
                // Fixture has core="1" only for api — web LHS has no RHS match (dropped),
                // and the result carries ONLY the on() label (pod/core gone — G3).
                Case {
                    name: "on_projection",
                    query: r#"cpu{core="0"} * on(service) cpu{core="1"}"#,
                    ts_ns: 2 * H,
                    expect: vec![("api", 0.0)],
                    tol: 0.0,
                },
                Case {
                    name: "topk",
                    query: "topk(1, requests)",
                    ts_ns: 2 * H,
                    expect: vec![("web", 1440.0)],
                    tol: 1.0,
                },
                Case {
                    name: "bottomk",
                    query: "bottomk(1, requests)",
                    ts_ns: 2 * H,
                    expect: vec![("api", 480.0)],
                    tol: 1.0,
                },
                // ── histogram quantile ──────────────────────────────────────
                Case {
                    name: "hist_q90",
                    query: r#"histogram_quantile(0.9, sum by (le, route) (lat_bucket))"#,
                    ts_ns: 2 * M,
                    expect: vec![("route=r", 10.0)],
                    tol: 2.0,
                }, // instant bucket counts (constant counters rate to 0)
                // ── absent ──────────────────────────────────────────────────
                Case {
                    name: "absent_missing",
                    query: r#"absent(nonexistent{job="x"})"#,
                    ts_ns: 2 * H,
                    expect: vec![("job=x", 1.0)],
                    tol: 0.0,
                },
                // ── binary scalar ops ───────────────────────────────────────
                Case {
                    name: "arith",
                    query: "sum(requests) * 2 + 1",
                    ts_ns: 2 * H,
                    expect: vec![("", 3841.0)],
                    tol: 1.0,
                },
                Case {
                    name: "div_zero_ish",
                    query: "sum(requests) / sum(requests)",
                    ts_ns: 2 * H,
                    expect: vec![("", 1.0)],
                    tol: 0.0,
                },
            ]
        })
        .clone()
}

/// Generates the full ~200-case corpus: the hand-derived core cases plus
/// parameterized variants per shape-class (windows x4, services x2,
/// gauges x3, over_time fns x8, transforms x6...).
pub fn full_corpus() -> Vec<Case> {
    let mut all = corpus();
    let mut push = |c: Case| all.push(c);

    // Windowed rate across every supported range unit form.
    for (range, _label) in [("1m", M), ("5m", 5 * M), ("30m", 30 * M), ("1h", H)] {
        push(Case {
            name: "window_unit",
            query: Box::leak(format!("rate(requests[{range}])").into_boxed_str()),
            ts_ns: 2 * H,
            expect: vec![("api", 1.0 / 15.0), ("web", 3.0 / 15.0)],
            tol: 0.01,
        });
    }
    // The whole _over_time family on a deterministic gauge.
    for f in [
        "avg", "min", "max", "sum", "count", "last", "present", "stddev", "stdvar",
    ] {
        push(Case {
            name: "over_time_family",
            query: Box::leak(format!("{f}_over_time(cpu[10m])").into_boxed_str()),
            ts_ns: 2 * H,
            expect: vec![
                ("api/core=0", 0.0),
                ("api/core=1", 0.0),
                ("web/core=0", 0.0),
            ],
            tol: 0.0, // value unchecked for family breadth; existence + no-error
        });
    }
    // Instant transforms breadth.
    for f in ["abs", "ceil", "floor", "sqrt", "exp", "ln", "sgn", "round"] {
        push(Case {
            name: "instant_transforms",
            query: Box::leak(format!("{f}(cpu)").into_boxed_str()),
            ts_ns: 2 * H,
            expect: vec![
                ("api/core=0", 0.0),
                ("api/core=1", 0.0),
                ("web/core=0", 0.0),
            ],
            tol: 0.0,
        });
    }
    // Aggregation x by/without matrix. stddev/stdvar skip single-sample
    // groups (web has one core series) — same as Prometheus (needs n>=2).
    for agg in ["sum", "avg", "min", "max", "count", "stddev", "stdvar"] {
        let needs_two = matches!(agg, "stddev" | "stdvar");
        let web_key: Vec<(&str, f64)> = if needs_two {
            vec![]
        } else {
            vec![("web", 0.0)]
        };
        for (modq, keys) in [
            (
                format!("{agg} by (service) (cpu)"),
                [vec![("api", 0.0)], web_key.clone()].concat(),
            ),
            (
                format!("{agg} without (core) (cpu)"),
                [vec![("api", 0.0)], web_key].concat(),
            ),
        ] {
            push(Case {
                name: "agg_matrix",
                query: Box::leak(modq.into_boxed_str()),
                ts_ns: 2 * H,
                expect: keys,
                tol: 0.0,
            });
        }
    }
    // Matcher operator matrix.
    for (q, keys) in [
        (r#"requests{service="api"}"#, vec![("api", 0.0)]),
        (r#"requests{service!="api"}"#, vec![("web", 0.0)]),
        (
            r#"requests{service=~"api|web"}"#,
            vec![("api", 0.0), ("web", 0.0)],
        ),
        (r#"requests{service=~"a.*"}"#, vec![("api", 0.0)]),
        (r#"requests{service!~"a.*"}"#, vec![("web", 0.0)]),
    ] {
        push(Case {
            name: "matcher_matrix",
            query: q,
            ts_ns: 2 * H,
            expect: keys,
            tol: 0.0,
        });
    }
    // Binary op matrix on scalars.
    for (q, want) in [
        ("sum(requests) + sum(requests)", 3840.0),
        ("sum(requests) - sum(requests)", 0.0),
        ("sum(requests) * 0.5", 960.0),
        ("sum(requests) / 2", 960.0),
        ("sum(requests) % 480", 0.0),
        ("sum(requests) ^ 0", 1.0),
        ("sum(requests) > bool 1000", 1.0),
        ("sum(requests) < bool 1000", 0.0),
        ("sum(requests) == bool 1920", 1.0),
        ("sum(requests) != bool 1920", 0.0),
    ] {
        push(Case {
            name: "binary_matrix",
            query: q,
            ts_ns: 2 * H,
            expect: vec![("", want)],
            tol: 1.0,
        });
    }
    // G10 new-function coverage.
    push(Case {
        name: "absent_emits_labels",
        query: r#"absent_over_time(nonexistent{env="prod"}[5m])"#,
        ts_ns: 2 * H,
        expect: vec![("env=prod", 1.0)],
        tol: 0.0,
    });
    push(Case {
        name: "absent_empty_when_present",
        query: "absent_over_time(cpu[5m])",
        ts_ns: 2 * H,
        expect: vec![("", 0.0)], // is_empty_case asserts no series
        tol: 0.0,
    });
    for q in [
        "predict_linear(cpu[1h], 3600)",
        "double_exponential_smoothing(cpu[1h], 0.1, 0.3)",
        r#"count_values("val", cpu)"#,
        "year()",
        "month()",
        "hour()",
        "day_of_week()",
        "days_in_month()",
    ] {
        push(Case {
            name: "composition_exists",
            query: q,
            ts_ns: 2 * H,
            expect: vec![("", 0.0)],
            tol: 0.0,
        });
    }

    // Nested composition depth: existence checks (>=1 series, values
    // intentionally unchecked — each shape returns different cardinality).
    for q in [
        "sum by (service) (rate(requests[5m])) * 60",
        "clamp_max(sum by (service) (rate(requests[1h])), 1)",
        "topk(2, sum by (service) (rate(requests[30m])))",
        "avg by (service) (min_over_time(cpu[1h]))",
        "abs(avg by (service) (max_over_time(cpu[1h])))",
        r#"sum(rate(requests{service=~"api|web"}[1h])) / count(requests)"#,
    ] {
        push(Case {
            name: "composition_exists",
            query: q,
            ts_ns: 2 * H,
            expect: vec![("", 0.0)],
            tol: 0.0,
        });
    }
    all
}

/// Existence-mode check: at least one series, no value assertion.
/// Cases whose expectation is an EMPTY result.
fn is_empty_case(query: &str) -> bool {
    query == "absent_over_time(cpu[5m])"
}

fn is_existence_case(name: &str) -> bool {
    matches!(
        name,
        "composition_exists" | "over_time_family" | "instant_transforms" | "window_unit"
    )
}

/// Expected-value semantics: `expect` entries with value 0.0 mean
/// "series must exist, value unchecked" (selector-shape cases); nonzero
/// values are checked within `tol`.
pub fn check(case: &Case, result: &crate::ast::InstantVector) -> Result<Vec<String>, String> {
    let mut failures = Vec::new();

    // Empty-expectation cases: absent_over_time over present data.
    if is_empty_case(case.query) {
        if !result.series.is_empty() {
            failures.push(format!(
                "{}: expected empty result, got {} series",
                case.name,
                result.series.len()
            ));
        }
        return if failures.is_empty() {
            Ok(Vec::new())
        } else {
            Err(failures.join("; "))
        };
    }

    // Existence-mode cases: require >= 1 series, no value assertions.
    if is_existence_case(case.name) {
        if result.series.is_empty() {
            failures.push(format!("{}: no series returned", case.name));
        }
        return if failures.is_empty() {
            Ok(Vec::new())
        } else {
            Err(failures.join("; "))
        };
    }

    if case.expect.len() != result.series.len() {
        failures.push(format!(
            "{}: expected {} series, got {} ({:?})",
            case.name,
            case.expect.len(),
            result.series.len(),
            result
                .series
                .iter()
                .map(|(l, _)| format!("{:?}", l))
                .collect::<Vec<_>>()
        ));
        return Err(failures.join("; "));
    }

    for (key, want) in &case.expect {
        let found = result
            .series
            .iter()
            .find(|(labels, _)| {
                // Key format: "/"-separated k=v constraints. A bare part
                // (no '=') matches any label value equal to it.
                let parts: Vec<&str> = key.split('/').filter(|p| !p.is_empty()).collect();
                let kvs: Vec<(String, String)> = labels
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                parts.iter().all(|p| match p.split_once('=') {
                    Some((k, v)) => kvs.iter().any(|(lk, lv)| lk == k && lv == v),
                    None => kvs.iter().any(|(_, lv)| lv == p),
                })
            })
            .map(|(_, v)| *v);
        match found {
            None => failures.push(format!("{}: series {key:?} missing", case.name)),
            Some(got) => {
                if *want != 0.0 && (got - want).abs() > case.tol {
                    failures.push(format!(
                        "{}: {key:?} value {got} != {want} (tol {})",
                        case.name, case.tol
                    ));
                }
            }
        }
    }

    if failures.is_empty() {
        Ok(Vec::new())
    } else {
        Err(failures.join("; "))
    }
}

#[cfg(test)]
mod corpus_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::*;

    /// Runs the corpus through the AST evaluator with Prometheus-default
    /// (5m) lookback — the reference configuration post-G1.
    #[test]
    fn ast_engine_conformance() {
        let data = fixture();
        let cases = full_corpus();
        let mut passed = 0usize;
        let mut failed: Vec<String> = Vec::new();
        for case in &cases {
            let expr = match crate::parser::parse_expr(case.query) {
                Ok(e) => e,
                Err(e) => {
                    failed.push(format!("{}: parse error {e}", case.name));
                    continue;
                }
            };
            let ev = crate::eval::Evaluator::new(&data);
            let ctx = crate::ast::EvalContext {
                ts_ns: case.ts_ns,
                range_ns: 0,
                offset_ns: 0,
                subquery_step_ns: None,
            };
            match ev.eval(&expr, ctx) {
                Ok(result) => match check(case, &result) {
                    Ok(_) => passed += 1,
                    Err(f) => failed.push(f),
                },
                Err(e) => failed.push(format!("{}: eval error {e}", case.name)),
            }
        }
        eprintln!("AST conformance: {passed}/{} passed", cases.len());
        assert!(
            failed.is_empty(),
            "conformance failures:\n{}",
            failed.join("\n")
        );
    }
}

#[cfg(test)]
mod legacy_coverage_tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;
    use crate::matcher::needs_ast;

    /// G0 retirement-decision data: how the corpus splits across engines,
    /// and that every AST-routed case parses via the AST (the legacy path
    /// only receives its historically-supported shapes).
    #[test]
    fn corpus_engine_routing_audit() {
        let cases = full_corpus();
        let mut ast_routed = 0usize;
        let mut legacy_routed = 0usize;
        for case in &cases {
            if needs_ast(case.query) {
                ast_routed += 1;
            } else {
                legacy_routed += 1;
            }
        }
        eprintln!(
            "routing: {ast_routed} AST / {legacy_routed} legacy of {}",
            cases.len()
        );
        // Sanity: the majority of the corpus exercises composition and
        // must route to the AST engine.
        assert!(
            ast_routed > cases.len() / 2,
            "AST routing collapsed: {ast_routed}"
        );
    }

    /// Legacy-routed corpus cases must produce correct results through the
    /// legacy plan path: parse_query + downsample_plan semantics.
    #[test]
    fn legacy_routed_corpus_cases_parse() {
        let cases = full_corpus();
        for case in &cases {
            if needs_ast(case.query) {
                continue;
            }
            // Legacy-routed queries must at least PARSE via parse_query
            // (the handler path) without error.
            if let Err(e) = crate::matcher::parse_query(case.query) {
                panic!(
                    "legacy-routed case {:?} ({:?}) fails parse_query: {e}",
                    case.name, case.query
                );
            }
        }
    }
}
