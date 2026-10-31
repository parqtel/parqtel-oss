//! Tests for the `dql` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;

fn fields(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn test_eq_operator() {
    let cond = DqlParser::parse(r#"service_name = "api-server""#).unwrap();
    assert!(cond.evaluate(&fields(&[("service_name", "api-server")])));
    assert!(!cond.evaluate(&fields(&[("service_name", "web")])));
}

#[test]
fn test_neq_operator() {
    let cond = DqlParser::parse(r#"env != "prod""#).unwrap();
    assert!(cond.evaluate(&fields(&[("env", "staging")])));
    assert!(!cond.evaluate(&fields(&[("env", "prod")])));
}

#[test]
fn test_gt_operator() {
    let cond = DqlParser::parse("severity_number > 8").unwrap();
    assert!(cond.evaluate(&fields(&[("severity_number", "9")])));
    assert!(!cond.evaluate(&fields(&[("severity_number", "8")])));
}

#[test]
fn test_gte_operator() {
    let cond = DqlParser::parse("severity_number >= 9").unwrap();
    assert!(cond.evaluate(&fields(&[("severity_number", "9")])));
    assert!(!cond.evaluate(&fields(&[("severity_number", "8")])));
}

#[test]
fn test_lt_operator() {
    let cond = DqlParser::parse("count < 5").unwrap();
    assert!(cond.evaluate(&fields(&[("count", "3")])));
    assert!(!cond.evaluate(&fields(&[("count", "5")])));
}

#[test]
fn test_lte_operator() {
    let cond = DqlParser::parse("count <= 5").unwrap();
    assert!(cond.evaluate(&fields(&[("count", "5")])));
    assert!(!cond.evaluate(&fields(&[("count", "6")])));
}

#[test]
fn test_contains_operator() {
    let cond = DqlParser::parse(r#"body contains "error""#).unwrap();
    assert!(cond.evaluate(&fields(&[("body", "an error occurred")])));
    assert!(!cond.evaluate(&fields(&[("body", "all good")])));
}

#[test]
fn test_matches_operator() {
    let cond = DqlParser::parse(r#"k8s_pod_name matches "^frontend-""#).unwrap();
    assert!(cond.evaluate(&fields(&[("k8s_pod_name", "frontend-abc123")])));
    assert!(!cond.evaluate(&fields(&[("k8s_pod_name", "backend-xyz")])));
}

#[test]
fn test_exists_operator() {
    let cond = DqlParser::parse("trace_id exists").unwrap();
    assert!(cond.evaluate(&fields(&[("trace_id", "abc")])));
    assert!(!cond.evaluate(&fields(&[("other", "x")])));
}

#[test]
fn test_between_operator() {
    let cond = DqlParser::parse("status between 400 and 599").unwrap();
    assert!(cond.evaluate(&fields(&[("status", "404")])));
    assert!(cond.evaluate(&fields(&[("status", "500")])));
    assert!(!cond.evaluate(&fields(&[("status", "200")])));
}

#[test]
fn test_in_operator() {
    let cond = DqlParser::parse(r#"env in ("prod", "staging")"#).unwrap();
    assert!(cond.evaluate(&fields(&[("env", "prod")])));
    assert!(cond.evaluate(&fields(&[("env", "staging")])));
    assert!(!cond.evaluate(&fields(&[("env", "dev")])));
}

#[test]
fn test_and_logic() {
    let cond = DqlParser::parse(r#"service_name = "api" AND severity_number >= 9"#).unwrap();
    assert!(cond.evaluate(&fields(&[
        ("service_name", "api"),
        ("severity_number", "9")
    ])));
    assert!(!cond.evaluate(&fields(&[
        ("service_name", "api"),
        ("severity_number", "5")
    ])));
}

#[test]
fn test_or_logic() {
    let cond = DqlParser::parse(r#"env = "prod" OR env = "staging""#).unwrap();
    assert!(cond.evaluate(&fields(&[("env", "prod")])));
    assert!(cond.evaluate(&fields(&[("env", "staging")])));
    assert!(!cond.evaluate(&fields(&[("env", "dev")])));
}

#[test]
fn test_not_logic() {
    let cond = DqlParser::parse(r#"NOT env = "dev""#).unwrap();
    assert!(cond.evaluate(&fields(&[("env", "prod")])));
    assert!(!cond.evaluate(&fields(&[("env", "dev")])));
}

#[test]
fn test_parenthesized_expression() {
    let cond =
        DqlParser::parse(r#"(env = "prod" OR env = "staging") AND severity_number > 8"#).unwrap();
    assert!(cond.evaluate(&fields(&[("env", "prod"), ("severity_number", "9")])));
    assert!(!cond.evaluate(&fields(&[("env", "dev"), ("severity_number", "9")])));
}

#[test]
fn test_invalid_expression() {
    assert!(DqlParser::parse("").is_err());
    assert!(DqlParser::parse("field ??? value").is_err());
}
