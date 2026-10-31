//! Tests for the `matcher` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

#[test]
fn new_function_families_route_to_ast() {
    // Every function the legacy plan engine doesn't know must take
    // the AST path — needs_ast() true.
    for q in [
        "pi()",
        "time()",
        "minute()",
        "hour()",
        "sin(x)",
        "atan2(x, 1)",
        "deg(x)",
        "rad(x)",
        "idelta(x[5m])",
        "quantile_over_time(0.9, x[5m])",
        "mad_over_time(x[5m])",
        "timestamp(x)",
        r#"label_del(x, "a")"#,
        r#"sort_by_label(x, "a")"#,
        r#"sort_by_label_desc(x, "a")"#,
        "histogram_count(x)",
        "histogram_sum(x)",
        "histogram_avg(x)",
        "histogram_stddev(x)",
        "histogram_stdvar(x)",
        "histogram_fraction(0, 1, x)",
        "histogram_quantile(0.9, x)",
    ] {
        assert!(needs_ast(q), "AST routing expected for: {q}");
    }
    // Legacy-compatible shapes still use the plan engine.
    assert!(!needs_ast("x"));
    assert!(!needs_ast(r#"x{a="b"}"#));
}

#[test]
fn test_label_matching() {
    let m = LabelMatcher::new("method".into(), MatchOp::Equal, "GET".into()).unwrap();
    assert!(m.matches(Some("GET")));
    assert!(!m.matches(Some("POST")));
    assert!(!m.matches(None));

    let m2 = LabelMatcher::new("code".into(), MatchOp::NotEqual, "500".into()).unwrap();
    assert!(m2.matches(Some("200")));
    assert!(!m2.matches(Some("500")));
    assert!(m2.matches(None)); // None != "500" is true

    let m3 = LabelMatcher::new("host".into(), MatchOp::RegexMatch, "api-.*".into()).unwrap();
    assert!(m3.matches(Some("api-v1")));
    assert!(!m3.matches(Some("web-v1")));

    let m4 = LabelMatcher::new("host".into(), MatchOp::RegexNotMatch, "api-.*".into()).unwrap();
    assert!(!m4.matches(Some("api-v1")));
    assert!(m4.matches(Some("web-v1")));
}

#[test]
fn test_selector_parsing() {
    let (name, matchers) =
        parse_selector("http_requests_total{method=\"GET\", status!=\"500\"}").unwrap();
    assert_eq!(name, Some("http_requests_total".into()));
    assert_eq!(matchers.len(), 2);
    assert_eq!(matchers[0].name, "method");
    assert_eq!(matchers[0].op, MatchOp::Equal);
    assert_eq!(matchers[1].name, "status");
    assert_eq!(matchers[1].op, MatchOp::NotEqual);

    // Regex and quotes
    let (_, matchers) = parse_selector("m{a=~\"v.*\", b!~'x.*'}").unwrap();
    assert_eq!(matchers[0].op, MatchOp::RegexMatch);
    assert_eq!(matchers[0].value, "v.*");
    assert_eq!(matchers[1].op, MatchOp::RegexNotMatch);
    assert_eq!(matchers[1].value, "'x.*'"); // Single quotes not stripped by current logic, only double
}

#[test]
fn test_parse_query() {
    let (name, _, agg, q, topk, by, without, lr, scalar, clamp, _range) =
        parse_query("avg(cpu_usage{host=\"h1\"})").unwrap();
    assert_eq!(name, "cpu_usage");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Avg));
    assert!(q.is_none());
    assert!(topk.is_none());
    assert!(by.is_empty());
    assert!(without.is_empty());
    assert!(lr.is_none());
    assert!(scalar.is_none());
    assert!(clamp.is_none());

    let (name, _, agg, q, ..) = parse_query("histogram_quantile(0.9, latency)").unwrap();
    assert_eq!(name, "latency");
    assert_eq!(agg, Some(crate::plan::AggregationOp::HistogramQuantile));
    assert_eq!(q, Some(0.9));

    let (name, _, agg, ..) = parse_query("rate(http_requests[5m])").unwrap();
    assert_eq!(name, "http_requests");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Rate));

    let (name, _, agg, ..) = parse_query("irate(http_requests[1m])").unwrap();
    assert_eq!(name, "http_requests");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Irate));

    let (name, _, agg, ..) = parse_query("increase(http_requests[5m])").unwrap();
    assert_eq!(name, "http_requests");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Increase));

    let (name, _, agg, ..) = parse_query("delta(temperature[10m])").unwrap();
    assert_eq!(name, "temperature");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Delta));

    let (name, _, agg, ..) = parse_query("stddev(cpu)").unwrap();
    assert_eq!(name, "cpu");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Stddev));

    // topk / bottomk
    let (name, _, agg, _, n, ..) = parse_query("topk(5, http_requests)").unwrap();
    assert_eq!(name, "http_requests");
    assert_eq!(agg, Some(crate::plan::AggregationOp::TopK));
    assert_eq!(n, Some(5));

    let (name, _, agg, _, n, ..) = parse_query("bottomk(3, errors_total)").unwrap();
    assert_eq!(name, "errors_total");
    assert_eq!(agg, Some(crate::plan::AggregationOp::BottomK));
    assert_eq!(n, Some(3));

    // instant transforms
    let (name, _, agg, ..) = parse_query("abs(cpu)").unwrap();
    assert_eq!(name, "cpu");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Abs));

    let (_name, _, agg, ..) = parse_query("ceil(cpu)").unwrap();
    assert_eq!(agg, Some(crate::plan::AggregationOp::Ceil));
    let (name2, _, agg2, ..) = parse_query("floor(cpu)").unwrap();
    assert_eq!(name2, "cpu");
    assert_eq!(agg2, Some(crate::plan::AggregationOp::Floor));

    // round with optional to_nearest
    let (_, _, agg, _, _, _, _, _, scalar, _, _) = parse_query("round(cpu, 0.5)").unwrap();
    assert_eq!(agg, Some(crate::plan::AggregationOp::Round));
    assert_eq!(scalar, Some(0.5));

    // clamp_min / clamp_max
    let (_, _, agg, _, _, _, _, _, _, clamp, _) = parse_query("clamp_min(cpu, 0.0)").unwrap();
    assert_eq!(agg, Some(crate::plan::AggregationOp::ClampMin));
    assert_eq!(clamp, Some((Some(0.0), None)));

    let (_, _, agg, _, _, _, _, _, _, clamp, _) = parse_query("clamp_max(cpu, 100.0)").unwrap();
    assert_eq!(agg, Some(crate::plan::AggregationOp::ClampMax));
    assert_eq!(clamp, Some((None, Some(100.0))));

    // sum with by grouping
    let (name, _, agg, _, _, by, without, ..) =
        parse_query("sum(http_requests{} by (method, status))").unwrap();
    assert_eq!(name, "http_requests");
    assert_eq!(agg, Some(crate::plan::AggregationOp::Sum));
    assert_eq!(by, vec!["method", "status"]);
    assert!(without.is_empty());

    // label_replace
    let (name, _, agg, _, _, _, _, lr, ..) =
        parse_query("label_replace(cpu, \"host_short\", \"$1\", \"host\", \"([^.]+).*\")").unwrap();
    assert_eq!(name, "cpu");
    assert_eq!(agg, Some(crate::plan::AggregationOp::LabelReplace));
    let lr = lr.unwrap();
    assert_eq!(lr.0, "host_short");
    assert_eq!(lr.2, "host");
}

#[test]
fn test_invalid_selector() {
    assert!(parse_selector("").is_err());
    assert!(parse_selector("m{a=").is_err());
    assert!(parse_selector("m{a").is_err());
}
