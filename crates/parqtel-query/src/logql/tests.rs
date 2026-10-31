//! Tests for the `logql` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use parqtel_core::{LabelSet, LogRecord};

fn log(body: &str, sev: &str, sev_num: i32, svc: &str) -> LogRecord {
    LogRecord {
        timestamp_ns: 1,
        observed_timestamp_ns: 1,
        severity_number: sev_num,
        severity_text: sev.into(),
        body: body.into(),
        attributes: LabelSet::try_from_iter(vec![(
            "http.status_code".to_string(),
            "500".to_string(),
        )])
        .unwrap(),
        resource_attributes: LabelSet::try_from_iter(vec![(
            "service.name".to_string(),
            svc.to_string(),
        )])
        .unwrap(),
        trace_id: [1u8; 16],
        span_id: [2u8; 8],
        flags: 0,
        scope_name: String::new(),
        scope_version: String::new(),
    }
}

#[test]
fn plain_terms_and_exclude() {
    let q = parse_search("error timeout -retry");
    assert_eq!(q.terms.len(), 3);
    assert!(q.terms[2].negate);
    // Body containing the negated term must NOT match.
    let l = log("connection error after timeout retry=2", "ERROR", 17, "api");
    assert!(!log_matches(&q, &l, &HashMap::new()));
    // Body without it matches.
    let l2 = log("connection error after timeout", "ERROR", 17, "api");
    assert!(log_matches(&q, &l2, &HashMap::new()));
    // AND semantics: all terms required.
    let q2 = parse_search("error absentterm");
    assert!(!log_matches(&q2, &l2, &HashMap::new()));
}

#[test]
fn field_equality() {
    let q = parse_search("service=api error");
    let l = log("error boom", "ERROR", 17, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
    let l2 = log("error boom", "ERROR", 17, "web");
    assert!(!log_matches(&q, &l2, &HashMap::new()));
}

#[test]
fn colon_syntax_equals() {
    // (unchanged: field:value with a space after the colon)
    let q = parse_search("service:api");
    let l = log("x", "INFO", 9, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
}

#[test]
fn attr_fields() {
    let q = parse_search("attr.http.status_code=500");
    let l = log("boom", "ERROR", 17, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
    let q2 = parse_search("http.status_code=500");
    assert!(log_matches(&q2, &l, &HashMap::new()));
}

#[test]
fn severity_min() {
    let q = parse_search("severity>=WARN");
    let l = log("x", "INFO", 9, "api");
    assert!(!log_matches(&q, &l, &HashMap::new()));
    let l2 = log("x", "WARN", 13, "api");
    assert!(log_matches(&q, &l2, &HashMap::new()));
    let l3 = log("x", "ERROR", 17, "api");
    assert!(log_matches(&q, &l3, &HashMap::new()));
}

#[test]
fn numeric_range() {
    let q = parse_search("duration:100-500");
    let mut extra = HashMap::new();
    extra.insert("duration".to_string(), "250".to_string());
    let l = log("x", "INFO", 9, "api");
    assert!(log_matches(&q, &l, &extra));
}

#[test]
fn comparison() {
    let q = parse_search("attr.http.status_code >= 400");
    let l = log("x", "INFO", 9, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
}

#[test]
fn exists() {
    let q = parse_search("trace_id:*");
    let l = log("x", "INFO", 9, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
}

#[test]
fn phrase_search() {
    let q = parse_search("\"connection refused\" -timeout");
    let l = log("upstream connection refused", "ERROR", 17, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
}

#[test]
fn wildcard_field() {
    let q = parse_search("service=api-*");
    let l = log("x", "INFO", 9, "api-gateway");
    assert!(log_matches(&q, &l, &HashMap::new()));
}

#[test]
fn not_on_range_clause_flattens_exact() {
    // NOT duration>500 must invert the comparison exactly,
    // not downgrade to positive matching.
    let q = parse_search("NOT duration>500");
    // Flat shape (single Not-of-clause flattens to Clause::Not).
    assert_eq!(
        q.clauses,
        vec![Clause::Not(Box::new(Clause::Cmp {
            field: "duration".to_string(),
            op: crate::logql::CmpOp::Gt,
            value: 500.0,
        }))]
    );
    let mut extra = HashMap::new();
    extra.insert("duration".to_string(), "250".to_string());
    let l = log("x", "INFO", 9, "api");
    assert!(log_matches(&q, &l, &extra), "250 is NOT > 500");
    let mut extra2 = HashMap::new();
    extra2.insert("duration".to_string(), "900".to_string());
    assert!(!log_matches(&q, &l, &extra2), "900 IS > 500");
}

#[test]
fn not_on_exists_clause() {
    let q = parse_search("NOT trace_id:*");
    // All test logs carry trace_id [1;16] — inverted: none match.
    let l = log("x", "INFO", 9, "api");
    assert!(!log_matches(&q, &l, &HashMap::new()));
}

#[test]
fn or_semantics() {
    // OR between terms
    let pred = parse_predicate("error OR timeout").unwrap();
    let l = log("upstream timeout", "INFO", 9, "api");
    assert!(log_matches_predicate(&pred, &l, &HashMap::new()));
    let l2 = log("unrelated message", "INFO", 9, "api");
    assert!(!log_matches_predicate(&pred, &l2, &HashMap::new()));
}

#[test]
fn or_between_field_clauses() {
    let pred = parse_predicate("service=api OR service=web").unwrap();
    let l1 = log("x", "INFO", 9, "api");
    let l2 = log("x", "INFO", 9, "web");
    let l3 = log("x", "INFO", 9, "billing");
    assert!(log_matches_predicate(&pred, &l1, &HashMap::new()));
    assert!(log_matches_predicate(&pred, &l2, &HashMap::new()));
    assert!(!log_matches_predicate(&pred, &l3, &HashMap::new()));
}

#[test]
fn not_semantics() {
    let pred = parse_predicate("NOT service=api").unwrap();
    let l1 = log("x", "INFO", 9, "web");
    assert!(log_matches_predicate(&pred, &l1, &HashMap::new()));
    let l2 = log("x", "INFO", 9, "api");
    assert!(!log_matches_predicate(&pred, &l2, &HashMap::new()));
}

#[test]
fn paren_grouping_with_and_or() {
    // (service=api AND error) OR (service=web AND timeout)
    let pred = parse_predicate("(service=api AND error) OR (service=web AND timeout)").unwrap();
    let l1 = log("fatal error", "ERROR", 17, "api");
    let l2 = log("upstream timeout", "INFO", 9, "web");
    let l3 = log("fatal error", "ERROR", 17, "web");
    let l4 = log("nothing here", "INFO", 9, "api");
    assert!(log_matches_predicate(&pred, &l1, &HashMap::new()));
    assert!(log_matches_predicate(&pred, &l2, &HashMap::new()));
    assert!(!log_matches_predicate(&pred, &l3, &HashMap::new()));
    assert!(!log_matches_predicate(&pred, &l4, &HashMap::new()));
}

/// A regex must be compiled once per *query*, not once per row.
///
/// Compiling a pattern costs ~1-10us against ~10ns for the match, so the
/// per-row form was spending three orders of magnitude more time setting up
/// than searching.
#[test]
fn regex_patterns_are_compiled_once_per_query() {
    let q = parse_search(r#"service=api body=~"(?i)ERROR|WARN" svc-* pay*"#);
    let prepared = PreparedLogQuery::new(&q);
    let patterns = prepared.compiled_patterns();
    assert!(
        patterns >= 2,
        "expected the =~ clause and both wildcards to be compiled, got {patterns}"
    );

    // Matching many records must not change the count: there is no pattern
    // string left on the prepared query to compile.
    for i in 0..500 {
        let log = parqtel_core::LogRecord::new(
            1_000 + i,
            1_000 + i,
            9,
            "INFO".into(),
            format!("request {i} handled"),
            LabelSet::try_from_iter(vec![("service.name", "api")]).unwrap(),
            LabelSet::try_from_iter(vec![("svc", "checkout")]).unwrap(),
            [0u8; 16],
            [0u8; 8],
            0,
            "".into(),
            "".into(),
        );
        let _ = prepared.matches(&log, &HashMap::new());
    }
    assert_eq!(
        prepared.compiled_patterns(),
        patterns,
        "matching rows must not compile anything"
    );
}

/// Pins the semantics of every clause and term form against explicit
/// expectations, so a future change to the prepared path cannot quietly
/// alter what a query means.
///
/// Deliberately *not* a comparison between the prepared path and the
/// `log_matches` wrapper: the wrapper is implemented on top of the prepared
/// path, so comparing the two can only ever catch them diverging from each
/// other, not either of them being wrong. These expectations were derived
/// from the pre-refactor implementation, which lowercased the haystack and
/// used `str::contains`.
#[test]
fn clause_and_term_semantics_are_pinned() {
    let log = parqtel_core::LogRecord::new(
        1_000,
        2_000,
        17, // ERROR
        "ERROR".into(),
        "payment TIMEOUT after 250ms".into(),
        LabelSet::try_from_iter(vec![
            ("http_status_code", "503".to_string()),
            ("service", "api".to_string()),
        ])
        .unwrap(),
        LabelSet::try_from_iter(vec![("service.name", "checkout")]).unwrap(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    );
    let extra = HashMap::new();

    // (query, expected match on the record above)
    let cases: &[(&str, bool)] = &[
        // bare terms: case-insensitive substring of the body
        ("payment", true),
        ("PAYMENT", true),
        ("timeout", true),
        ("after", true),
        ("nonexistent", false),
        // negated term
        ("-payment", false),
        ("-nonexistent", true),
        // wildcard term: (?i) so it matches the raw body
        ("pay*", true),
        ("*timeout", true),
        ("*250ms", true),
        // Only `*` sets the wildcard flag; `?` is a literal character, so
        // `pay?ent` looks for that exact substring and finds nothing.
        ("pay?ent", false),
        ("zzz*", false),
        // body: / body= is a case-insensitive substring too
        ("body:payment", true),
        ("body:PAYMENT", true),
        ("body:nonexistent", false),
        ("body!payment", false),
        ("body!nonexistent", true),
        // `service` is a DEDICATED field: it resolves through
        // resource_attributes["service.name"], shadowing the same-named
        // record attribute. Equality is case-sensitive.
        ("service=checkout", true),
        ("service=CHECKOUT", false),
        ("service!=api", true),
        // the record attribute is still reachable explicitly
        ("attr.service=api", true),
        ("attr.service=worker", false),
        // resource attributes resolve through `service` / `service.name`
        ("service.name=checkout", true),
        ("service.name=checkout-api", false),
        ("service.name!=api", true),
        // explicit prefixes
        ("attr.http_status_code=503", true),
        ("res.service.name=checkout", true),
        ("attr.nonexistent=x", false),
        // regex
        (r#"service=~"c.*""#, true),
        (r#"service=~"z.*""#, false),
        (r#"body=~"TIMEOUT""#, true),
        (r#"body=~"[[invalid""#, false),
        // numeric comparison
        ("attr.http_status_code>=500", true),
        ("attr.http_status_code>500", true),
        ("attr.http_status_code>=503", true),
        ("attr.http_status_code>503", false),
        ("attr.http_status_code<=503", true),
        ("attr.http_status_code<503", false),
        ("attr.http_status_code>=9999", false),
        // range
        ("attr.http_status_code:500-600", true),
        ("attr.http_status_code:600-700", false),
        // exists
        ("attr.http_status_code:*", true),
        ("attr.nonexistent:*", false),
        // Severity maps to severity_number. The record is 17, and the
        // severity table tops out at 17, so ERROR and FATAL both hold.
        ("severity>=ERROR", true),
        ("severity>=WARN", true),
        ("severity>=FATAL", true),
        ("severity>=TRACE", true),
        ("severity>=INFO", true),
        // negation
        ("NOT service=checkout", false),
        ("NOT service=worker", true),
        ("NOT NOT service=checkout", true),
        // AND flattens into the flat SearchQuery form.
        ("service=checkout AND timeout", true),
        ("service=checkout AND nonexistent", false),
    ];

    for (qs, expected) in cases {
        let q = parse_search(qs);
        assert_eq!(
            log_matches(&q, &log, &extra),
            *expected,
            "wrapper disagrees for {qs:?}"
        );
        let prepared = PreparedLogQuery::new(&q);
        assert_eq!(
            prepared.matches(&log, &extra),
            *expected,
            "prepared path disagrees for {qs:?}"
        );
    }
}

/// Boolean composition must be pinned through the *predicate* API.
///
/// `parse_search` deliberately does not flatten a top-level `OR`: it falls
/// back to an unconstrained query, so a search string containing `OR`
/// matches everything. That is pre-existing behaviour and is why the
/// composition cases live here, where `parse_predicate` is what the
/// handlers use for OR/NOT trees.
#[test]
fn predicate_composition_semantics_are_pinned() {
    let log = parqtel_core::LogRecord::new(
        1_000,
        2_000,
        17,
        "ERROR".into(),
        "payment TIMEOUT after 250ms".into(),
        LabelSet::try_from_iter(vec![("http_status_code", "503".to_string())]).unwrap(),
        LabelSet::try_from_iter(vec![("service.name", "checkout")]).unwrap(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    );
    let extra = HashMap::new();

    let cases: &[(&str, bool)] = &[
        ("service=checkout AND timeout", true),
        ("service=checkout AND nonexistent", false),
        ("service=worker OR timeout", true),
        ("service=worker OR nonexistent", false),
        ("service=checkout OR service=worker", true),
        ("NOT service=checkout", false),
        ("NOT service=worker", true),
        ("NOT NOT service=checkout", true),
        ("NOT (service=checkout OR severity=ERROR)", false),
        ("(service=worker OR severity=ERROR) AND timeout", true),
        ("(service=worker OR nonexistent) AND timeout", false),
        ("service=~\"checkout\" AND NOT nonexistent", true),
    ];

    for (qs, expected) in cases {
        let pred = parse_predicate(qs).unwrap_or_else(|_| panic!("parse: {qs}"));
        assert_eq!(
            log_matches_predicate(&pred, &log, &extra),
            *expected,
            "predicate wrapper disagrees for {qs:?}"
        );
        let prepared = PreparedPredicate::new(&pred);
        assert_eq!(
            prepared.matches(&log, &extra),
            *expected,
            "prepared predicate disagrees for {qs:?}"
        );
    }
}

/// The two public entry points must agree over a corpus, so that hoisting
/// work to prepare time can never make them diverge from each other.
///
/// This does **not** establish that either is correct — see
/// `clause_and_term_semantics_are_pinned` for that.
#[test]
fn prepared_and_wrapper_paths_agree() {
    let queries = [
        "service=api",
        r#"service=~"ap.*""#,
        "body:timeout",
        "attr.http_status_code >= 400",
        "severity>=ERROR",
        r#"NOT service=api"#,
        "timeout",
        "-timeout",
        "pai*",
        r#"service=api AND attr.code=200"#,
        r#"service=api OR service=worker"#,
    ];
    let bodies = ["request completed", "payment TIMEOUT after 12ms"];
    for qs in queries {
        let q = parse_search(qs);
        let prepared = PreparedLogQuery::new(&q);
        for body in bodies {
            for (code, svc, sev) in [(200, "api", 9), (503, "checkout", 17)] {
                let l = parqtel_core::LogRecord::new(
                    1_000,
                    2_000,
                    sev,
                    "INFO".into(),
                    body.to_string(),
                    LabelSet::try_from_iter(vec![("http_status_code", code.to_string())]).unwrap(),
                    LabelSet::try_from_iter(vec![("service.name", svc)]).unwrap(),
                    [0u8; 16],
                    [0u8; 8],
                    0,
                    "".into(),
                    "".into(),
                );
                let extra = HashMap::new();
                assert_eq!(
                    prepared.matches(&l, &extra),
                    log_matches(&q, &l, &extra),
                    "prepared vs wrapper disagree on {qs:?} body={body:?} \
                     code={code} svc={svc} sev={sev}"
                );
            }
        }
    }
}

/// A pipeline-enriched field must not shadow an attribute of the same name.
///
/// The bare-name fallback order (attributes -> resource -> `extra`) is
/// load-bearing and easy to invert accidentally.
#[test]
fn extra_fields_do_not_shadow_record_attributes() {
    let l = parqtel_core::LogRecord::new(
        1,
        2,
        9,
        "INFO".into(),
        "body".into(),
        LabelSet::default(),
        LabelSet::try_from_iter(vec![("env", "prod")]).unwrap(),
        [0u8; 16],
        [0u8; 8],
        0,
        "".into(),
        "".into(),
    );
    let mut extra = HashMap::new();
    extra.insert("env".to_string(), "staging".to_string());

    let q = parse_search("env=prod");
    assert!(
        log_matches(&q, &l, &extra),
        "the record attribute must win over `extra`"
    );
    let q = parse_search("env=staging");
    assert!(!log_matches(&q, &l, &extra));

    let prepared = PreparedLogQuery::new(&parse_search("env=prod"));
    assert!(prepared.matches(&l, &extra));
}

/// `contains_ci` is the allocation-free replacement for lowercasing the body
/// per term per row, so it has to agree with the allocation it replaces.
#[test]
fn contains_ci_matches_lowercase_contains() {
    let cases = [
        ("Request Completed", "request"),
        ("TIMEOUT", "timeout"),
        ("", "anything"),
        ("anything", ""),
        ("short", "much longer needle"),
        ("MiXeD", "xEd"),
        ("MiXeD", "mixed"),
        ("MiXeD", "mIxEd"),
        ("MiXeD", "mixedx"),
    ];
    for (hay, needle) in cases {
        let via_lower = hay.to_lowercase().contains(&needle.to_lowercase());
        assert_eq!(
            contains_ci(hay, &needle.to_lowercase()),
            via_lower,
            "contains_ci({hay:?}, {needle:?}) disagrees with the lowercase form"
        );
    }
}

fn prefilter_of(q: &str) -> ColumnPrefilter {
    PreparedPredicate::new(&parse_predicate(q).unwrap()).column_prefilter()
}

/// The prefilter may only demand what *every* matching row satisfies: the
/// conjuncts of the predicate. Anything under an OR or a NOT is optional or
/// inverted, and demanding it would drop rows the predicate accepts.
#[test]
fn column_prefilter_takes_only_conjuncts() {
    let pre = prefilter_of("Timeout severity>=WARN");
    assert_eq!(pre.body_contains_lower, vec!["timeout".to_string()]);
    assert_eq!(pre.min_severity, Some(13));

    // Both branches of an OR are optional.
    let pre = prefilter_of("alpha OR beta");
    assert!(pre.is_empty(), "OR contributes nothing: {pre:?}");

    // A conjunct next to an OR still counts; the OR does not.
    let pre = prefilter_of("must (alpha OR beta)");
    assert_eq!(pre.body_contains_lower, vec!["must".to_string()]);

    // Negation inverts the requirement.
    assert!(prefilter_of("-nope").is_empty());
    assert!(prefilter_of("NOT alpha").is_empty());

    // A field equality never decides a row by itself: it only names a pair
    // the record must carry, and the predicate stays the judge.
    let pre = prefilter_of("service=api");
    assert_eq!(
        pre.attr_equals,
        vec![("service.name".to_string(), "api".to_string())]
    );
    assert!(!pre.exact);
    assert!(pre.body_contains_lower.is_empty() && pre.min_severity.is_none());
    // A bare name could be a pipeline-enriched field, so it is left out
    // unless the caller says there are none.
    assert!(prefilter_of("pod=p3").attr_equals.is_empty());
    let bare = PreparedPredicate::new(&parse_predicate("pod=p3 attr.zone=z res.k=v").unwrap())
        .column_prefilter_without_extra();
    assert_eq!(bare.attr_equals.len(), 3);
    // Equalities under OR/NOT are not necessary conditions.
    assert!(prefilter_of("service=api OR service=web")
        .attr_equals
        .is_empty());
    assert!(prefilter_of("NOT service=api").attr_equals.is_empty());
}

#[test]
fn column_prefilter_for_flat_search_queries() {
    let q = parse_search("error -debug body:Slow severity>=ERROR");
    let pre = PreparedLogQuery::new(&q).column_prefilter();
    let mut needles = pre.body_contains_lower.clone();
    needles.sort();
    assert_eq!(needles, vec!["error".to_string(), "slow".to_string()]);
    assert_eq!(pre.min_severity, Some(17));
}

#[test]
fn and_or_precedence() {
    // a AND b OR c == (a AND b) OR c
    let pred = parse_predicate("service=api error OR timeout").unwrap();
    // matches: (api + body error) OR (body timeout)
    let l1 = log("error", "INFO", 9, "api");
    let l2 = log("timeout", "INFO", 9, "web");
    let l3 = log("error", "INFO", 9, "web"); // c matches? no term 'timeout', service!=api -> false
    assert!(log_matches_predicate(&pred, &l1, &HashMap::new()));
    assert!(log_matches_predicate(&pred, &l2, &HashMap::new()));
    assert!(!log_matches_predicate(&pred, &l3, &HashMap::new()));
}

#[test]
fn or_flattens_backwards_compatible() {
    // AND-only queries still flatten into SearchQuery
    let q = parse_search("service=api severity>=ERROR timeout");
    assert_eq!(q.clauses.len(), 2);
    assert_eq!(q.terms.len(), 1);
}

#[test]
fn legacy_selector_shape_converts() {
    let q = parse_search("{service=\"api\",severity>=WARN}");
    // mixed: service clause + severity clause
    assert!(!q.clauses.is_empty());
    let q2 = parse_search("{}");
    assert!(q2.is_empty());
}

#[test]
fn body_prefix_is_contains() {
    // explicit body: prefix → contains semantics (not full equality).
    let q = parse_search("body:timeout");
    let l = log("upstream timeout after 5000ms", "ERROR", 17, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
    let q2 = parse_search("body=timeout");
    assert!(log_matches(&q2, &l, &HashMap::new()));
    let q3 = parse_search("body:nosuchword");
    assert!(!log_matches(&q3, &l, &HashMap::new()));
}

#[test]
fn url_values_keep_colons() {
    // colons INSIDE values stay part of the value term.
    let q = parse_search("url:https://api.example.com:8443/health");
    assert!(
        q.clauses.iter().any(|c| matches!(
            c,
            Clause::Eq { field, value }
                if field == "url"
                    && value == "https://api.example.com:8443/health"
        )),
        "clauses={:?}",
        q.clauses
    );
}

#[test]
fn colon_operator_still_recognized() {
    // `field: value` (space) and `field:value ` (end) remain operators.
    let q = parse_search("service: api");
    assert!(q
        .clauses
        .iter()
        .any(|c| matches!(c, Clause::Eq { field, .. } if field == "service")));
    let q2 = parse_search("service:api");
    assert!(q2
        .clauses
        .iter()
        .any(|c| matches!(c, Clause::Eq { field, .. } if field == "service")));
}

#[test]
fn lenient_never_fails() {
    // Garbage input must still produce a usable query, not an error.
    let q = parse_search("!!! ??? (( ]]");
    // Lenient: garbage parses to SOMETHING without error.
    let _ = &q;
    let q2 = parse_search("");
    assert!(q2.is_empty());
}

#[test]
fn negated_field() {
    let q = parse_search("service!=api");
    let l = log("x", "INFO", 9, "web");
    assert!(log_matches(&q, &l, &HashMap::new()));
    let l2 = log("x", "INFO", 9, "api");
    assert!(!log_matches(&q, &l2, &HashMap::new()));
}

#[test]
fn combined_clauses_and_terms() {
    let q = parse_search("service=api severity>=ERROR timeout");
    let l = log("upstream timeout waiting", "ERROR", 17, "api");
    assert!(log_matches(&q, &l, &HashMap::new()));
    let l2 = log("upstream timeout waiting", "ERROR", 17, "web");
    assert!(!log_matches(&q, &l2, &HashMap::new()));
}
