//! Tests for the `parser` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn assert_parses(q: &str) -> Expr {
    parse_expr(q).unwrap_or_else(|e| panic!("{q:?} failed: {e}"))
}

#[test]
fn lexer_duration_tokens() {
    let mut l = Lexer::new("cpu > bool 80 offset 5m");
    let toks: Vec<Tok> = vec![
        Tok::Ident("cpu".into()),
        Tok::Gt,
        Tok::Ident("bool".into()),
        Tok::Number(80.0),
        Tok::Ident("offset".into()),
        Tok::Ident("5m".into()),
        Tok::Eof,
    ];
    for want in toks {
        let got = l.next_token().unwrap();
        assert_eq!(got, want, "token mismatch");
    }
}

#[test]
fn bare_selector() {
    assert_parses("cpu_usage");
}

#[test]
fn avg_over_time_parses() {
    let e = assert_parses("avg_over_time(cpu_usage[5m])");
    match &e {
        Expr::Call(c) => {
            assert_eq!(c.name, "avg_over_time");
            assert_eq!(c.args.len(), 1);
            assert!(matches!(c.args[0], Expr::Range(_)));
        }
        other => panic!("expected call, got {other:?}"),
    }
}

#[test]
fn selector_with_matchers() {
    assert_parses(r#"http_requests_total{service.name="api",method=~"GET|POST"}"#);
}

#[test]
fn nested_call_aggregation() {
    let e = assert_parses("sum by (job) (rate(http_requests_total[5m]))");
    assert!(matches!(e, Expr::Aggregation(_)));
}

#[test]
fn trailing_grouping_by() {
    // `sum(x) by (job)` — grouping AFTER the aggregation body.
    let e = assert_parses("sum(http_requests_total) by (job)");
    match &e {
        Expr::Aggregation(a) => {
            assert!(matches!(&a.grouping, Grouping::By(ls) if ls == &vec!["job".to_string()]));
        }
        other => panic!("expected aggregation, got {other:?}"),
    }
}

#[test]
fn trailing_grouping_without_multi_label() {
    let e = assert_parses("count(http_requests_total{a=\"b\"}) without (job, instance)");
    match &e {
        Expr::Aggregation(a) => {
            assert!(matches!(
                &a.grouping,
                Grouping::Without(ls)
                    if ls == &vec!["job".to_string(), "instance".to_string()]
            ));
        }
        other => panic!("expected aggregation, got {other:?}"),
    }
}

#[test]
fn leading_grouping_still_wins_over_trailing() {
    // `sum by (job) (x)` must keep the LEADING grouping; a stray trailing
    // `by` after the body is a parse error in PromQL and must not silently
    // override. (We accept the leading one and reject ambiguity by only
    // parsing a trailing clause when no leading clause was present.)
    let e = assert_parses("sum by (job) (http_requests_total)");
    match &e {
        Expr::Aggregation(a) => {
            assert!(matches!(&a.grouping, Grouping::By(ls) if ls == &vec!["job".to_string()]));
        }
        other => panic!("expected aggregation, got {other:?}"),
    }
}

#[test]
fn trailing_grouping_with_nested_expr() {
    let e = assert_parses("sum(rate(http_requests_total[5m])) by (job)");
    match &e {
        Expr::Aggregation(a) => {
            assert!(matches!(&a.grouping, Grouping::By(ls) if ls == &vec!["job".to_string()]));
            assert!(matches!(&*a.expr, Expr::Call(_)));
        }
        other => panic!("expected aggregation, got {other:?}"),
    }
}

#[test]
fn binary_ratio() {
    let e = assert_parses(r#"a{f="x"} / b{f="y"}"#);
    match e {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Div);
            assert!(b.matching.is_none());
        }
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn vector_matching_on_group_left() {
    let e = assert_parses(r#"a * on(x, y) group_left(z) b"#);
    match e {
        Expr::Binary(b) => {
            let (vm, card) = b.matching.unwrap();
            assert!(matches!(vm, VectorMatch::On(ls) if ls == vec!["x", "y"]));
            assert!(matches!(card, MatchCardinality::ManyToOne(ls) if ls == vec!["z"]));
        }
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn precedence_mul_over_add() {
    // 1 + 2 * 3 parses as 1 + (2*3)
    let e = assert_parses("1 + 2 * 3");
    match e {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Add);
            assert!(matches!(*b.rhs, Expr::Binary(r) if r.op == BinaryOp::Mul));
        }
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn paren_grouping_overrides() {
    // (1 + 2) * 3 parses as (1+2) * 3
    let e = assert_parses("(1 + 2) * 3");
    match e {
        Expr::Binary(b) => {
            assert_eq!(b.op, BinaryOp::Mul);
            assert!(matches!(*b.lhs, Expr::Paren(_)));
        }
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn subquery_range_with_step() {
    let e = assert_parses("max_over_time(rate(x[5m])[1h:30s])");
    assert!(matches!(e, Expr::Call(_)));
}

#[test]
fn aggregation_without() {
    assert_parses("sum without (instance) (cpu)");
}

#[test]
fn unary_minus() {
    assert_parses("-cpu_usage");
}

#[test]
fn offset_on_selector() {
    assert_parses("cpu_usage offset 5m");
}

#[test]
fn bool_modifier() {
    let e = assert_parses("cpu > bool 80");
    match e {
        Expr::Binary(b) => assert!(b.return_bool),
        other => panic!("expected binary, got {other:?}"),
    }
}

#[test]
fn topk_two_args() {
    let e = assert_parses("topk(5, cpu_usage)");
    match e {
        Expr::Aggregation(a) => {
            assert_eq!(a.op, AggregationOp::TopK);
            assert!(a.param.is_some());
        }
        other => panic!("expected aggregation, got {other:?}"),
    }
}

#[test]
fn absent_range_parses() {
    let e = assert_parses(r#"absent_over_time(nonexistent{env="prod"}[5m])"#);
    match &e {
        Expr::Call(call) => {
            assert_eq!(call.name, "absent_over_time");
            assert!(
                matches!(call.args[0], Expr::Range(_)),
                "arg0 is {:?}",
                call.args[0]
            );
        }
        other => panic!("expected call, got {other:?}"),
    }
}

#[test]
fn chained_composition() {
    // The canonical RED pattern
    assert_parses(
        r#"histogram_quantile(0.9, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))"#,
    );
}

#[test]
fn rejects_at_modifier() {
    assert!(parse_expr("x @ 100").is_err());
}

#[test]
fn rejects_trailing_garbage() {
    assert!(parse_expr("cpu_usage )").is_err());
}

#[test]
fn comparison_ops() {
    assert_parses("cpu_usage > 0.8");
    assert_parses("cpu_usage >= 0.8");
    assert_parses("cpu_usage < 0.8");
    assert_parses("cpu_usage <= 0.8");
    assert_parses("cpu_usage == 0.8");
    assert_parses("cpu_usage != 0.8");
}

#[test]
fn set_operators() {
    assert_parses(r#"a{f="1"} and b"#);
    assert_parses(r#"a{f="1"} unless b"#);
    assert_parses(r#"a or b"#);
}
