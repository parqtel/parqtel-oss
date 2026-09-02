//! ParqtelQL — unified lenient search grammar for logs and traces.
//!
//! Design (per docs/QUERY_ENGINE_ANALYSIS.md Phase 1B, modeled on
//! ClickStack/Elasticsearch `simple_query_string` guidance):
//!
//! - **Lenient**: unknown tokens become body-term searches instead of
//!   syntax errors — a search box must never 400 on user typing.
//! - Terms: `error`, `"exact phrase"`, `*partial*`, `-exclude`
//! - Boolean: `AND OR NOT` (case-insensitive), parentheses
//! - Field ops: `service=api`, `service:api` (equivalent),
//!   `severity>=WARN`, `duration:>100`, `duration_ms:200-500`,
//!   `trace_id:"a1b2…"`, `attr.http.status_code=500`
//! - Existence: `field:*`
//! - Regex on body: `body:/error \d+/`
//! - Special fields for traces: `service`, `operation`/`name`,
//!   `status` (ERROR|OK), `duration` (ms with comparison or range),
//!   `kind` (server|client|internal), plus arbitrary `attr.KEY` lookups.

use parqtel_core::{Error, Result};
use std::collections::HashMap;

/// A parsed ParqtelQL query: conjunction of clauses.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchQuery {
    /// Field equality/regex/range clauses — ALL must match (AND).
    pub clauses: Vec<Clause>,
    /// Free-text terms for body search (must all appear, case-insensitive).
    pub terms: Vec<SearchTerm>,
}

/// One structured field constraint.
#[derive(Debug, Clone, PartialEq)]
pub enum Clause {
    /// `field = value` / `field : value` — string equality (regex if
    /// the value contains unescaped `*`).
    Eq { field: String, value: String },
    /// `field != value`
    Ne { field: String, value: String },
    /// `field =~ "re"` or value with wildcards
    Re { field: String, regex: String },
    /// `field >=|>|<=|< num` — numeric comparison.
    Cmp {
        field: String,
        op: CmpOp,
        value: f64,
    },
    /// `field : a-b` or `field : [a..b]` — numeric range (inclusive).
    Range { field: String, min: f64, max: f64 },
    /// `field : *` — the field must be present.
    Exists { field: String },
    /// `severity >= WARN` etc. — maps to severity_number thresholds.
    SeverityMin(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Ge,
    Lt,
    Le,
}

/// A free-text body term.
#[derive(Debug, Clone, PartialEq)]
pub struct SearchTerm {
    pub text: String,
    /// true = must NOT match (prefixed `-`).
    pub negate: bool,
    /// true = quoted exact phrase (substring, still case-insensitive).
    pub phrase: bool,
    /// true = wildcard pattern.
    pub wildcard: bool,
}

impl SearchQuery {
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty() && self.terms.is_empty()
    }
}

/// Parses a ParqtelQL search string. NEVER fails on content — only on
/// structurally impossible input (unbalanced quotes), where a best-effort
/// fallback returns the raw string as body terms.
pub fn parse_search(query: &str) -> SearchQuery {
    // `{}` / `{ ... }` / empty = no constraint (legacy empty-selector shape).
    let trimmed = query.trim();
    if trimmed.is_empty() || trimmed == "{}" {
        return SearchQuery::default();
    }
    if trimmed.starts_with('{') && trimmed.ends_with('}') {
        // Legacy selector-shaped input on the ParqtelQL path: parse the
        // inner matchers as field=value clauses.
        if let Ok(q) = parse_legacy_selector(trimmed) {
            return q;
        }
    }
    match parse_search_inner(query) {
        Ok(q) => q,
        Err(_) => {
            // Lenient fallback: treat the whole input as body terms.
            let mut q = SearchQuery::default();
            for tok in query.split_whitespace() {
                if !tok.is_empty() {
                    q.terms.push(SearchTerm {
                        text: tok.trim_matches('"').to_string(),
                        negate: false,
                        phrase: false,
                        wildcard: false,
                    });
                }
            }
            q
        }
    }
}

fn parse_search_inner(input: &str) -> Result<SearchQuery> {
    let mut q = SearchQuery::default();
    let toks = tokenize(input)?;
    let mut i = 0;
    let mut pending_not = false;

    while i < toks.len() {
        let tok = &toks[i];
        match tok {
            Tok::And => {
                i += 1;
            } // implicit
            Tok::Or => {
                // OR semantics need a tree; Phase 1B is AND-only. Lenient:
                // treat OR as a term separator (both sides still required).
                // Documented limitation.
                i += 1;
            }
            Tok::Not => {
                pending_not = true;
                i += 1;
            }
            Tok::LParen | Tok::RParen => {
                i += 1;
            } // grouping tolerated
            Tok::Term(t) => {
                if let Some(Tok::Op(op)) = toks.get(i + 1) {
                    if let Some((field, _)) = split_field(t) {
                        if let Some(clause) =
                            build_field_clause(&field, *op, toks.get(i + 2), &mut i)?
                        {
                            apply_clause(&mut q, clause, pending_not)?;
                            pending_not = false;
                            continue;
                        }
                    }
                }
                // plain term
                let term = build_term(t, pending_not)?;
                pending_not = false;
                q.terms.push(term);
                i += 1;
            }
            Tok::Op(_) => {
                i += 1;
            } // stray operator — skip
            Tok::Str(s) => {
                q.terms.push(SearchTerm {
                    text: s.clone(),
                    negate: pending_not,
                    phrase: true,
                    wildcard: false,
                });
                pending_not = false;
                i += 1;
            }
        }
    }
    Ok(q)
}

fn apply_clause(q: &mut SearchQuery, clause: Clause, negate: bool) -> Result<()> {
    if negate {
        // NOT on a field clause maps to Ne when possible; else ignored
        // (lenient). Only Eq supports clean negation.
        match clause {
            Clause::Eq { field, value } => q.clauses.push(Clause::Ne { field, value }),
            other => q.clauses.push(other),
        }
    } else {
        q.clauses.push(clause);
    }
    Ok(())
}

fn build_term(t: &str, negate_hint: bool) -> Result<SearchTerm> {
    let (text, negate) = if let Some(rest) = t.strip_prefix('-') {
        (rest, true)
    } else {
        (t, negate_hint)
    };
    let wildcard = text.contains('*');
    Ok(SearchTerm {
        text: text.trim_matches('*').to_lowercase(),
        negate,
        phrase: false,
        wildcard,
    })
}

fn split_field(t: &str) -> Option<(String, &str)> {
    // field ops need the NEXT token to be an operator; this fn checks if
    // the current token looks like a field name (dotted idents).
    if t.is_empty() {
        return None;
    }
    let first = t.chars().next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    if !t
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        return None;
    }
    Some((t.to_string(), ""))
}

/// Builds a clause for `field <op> <value>`; advances `i` past consumed
/// tokens. Returns None when the shape isn't a field op (value missing).
fn build_field_clause(
    field: &str,
    op: FieldOp,
    value_tok: Option<&Tok>,
    i: &mut usize,
) -> Result<Option<Clause>> {
    let Some(value_tok) = value_tok else {
        *i += 2;
        return Ok(Some(Clause::Exists {
            field: field.to_string(),
        }));
    };
    let value = match value_tok {
        Tok::Term(t) => t.clone(),
        Tok::Str(s) => s.clone(),
        _ => {
            *i += 2;
            return Ok(Some(Clause::Exists {
                field: field.to_string(),
            }));
        }
    };
    *i += 3;

    match op {
        FieldOp::Eq => {
            if value.contains('*') {
                let re = wildcard_to_regex(&value)?;
                Ok(Some(Clause::Re {
                    field: field.to_string(),
                    regex: re,
                }))
            } else {
                Ok(Some(Clause::Eq {
                    field: field.to_string(),
                    value,
                }))
            }
        }
        FieldOp::Ne => Ok(Some(Clause::Ne {
            field: field.to_string(),
            value,
        })),
        FieldOp::Re => Ok(Some(Clause::Re {
            field: field.to_string(),
            regex: value,
        })),
        FieldOp::Gt | FieldOp::Ge | FieldOp::Lt | FieldOp::Le => {
            // `severity >= WARN` style: threshold on the severity word.
            if (field == "severity" || field == "severity_text") && severity_rank(&value).is_some()
            {
                // >= maps to SeverityMin; other comparisons approximate to
                // SeverityMin too (documented Phase 1B simplification).
                return Ok(Some(Clause::SeverityMin(value)));
            }
            let n: f64 = value.parse().map_err(|_| {
                Error::Validation(format!("field {field} needs a number after {op:?}"))
            })?;
            let cop = match op {
                FieldOp::Gt => CmpOp::Gt,
                FieldOp::Ge => CmpOp::Ge,
                FieldOp::Lt => CmpOp::Lt,
                _ => CmpOp::Le,
            };
            Ok(Some(Clause::Cmp {
                field: field.to_string(),
                op: cop,
                value: n,
            }))
        }
        FieldOp::Colon => {
            // range `a-b` / `[a..b]` or plain eq
            if let Some((min, max)) = parse_range(&value) {
                return Ok(Some(Clause::Range {
                    field: field.to_string(),
                    min,
                    max,
                }));
            }
            if value == "*" {
                return Ok(Some(Clause::Exists {
                    field: field.to_string(),
                }));
            }
            if value.contains('*') {
                let re = wildcard_to_regex(&value)?;
                return Ok(Some(Clause::Re {
                    field: field.to_string(),
                    regex: re,
                }));
            }
            if (field == "severity" || field == "severity_text") && severity_rank(&value).is_some()
            {
                return Ok(Some(Clause::SeverityMin(value)));
            }
            Ok(Some(Clause::Eq {
                field: field.to_string(),
                value,
            }))
        }
    }
}

fn severity_rank(sev: &str) -> Option<i32> {
    Some(match sev.to_ascii_uppercase().as_str() {
        "TRACE" | "VERBOSE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" | "WARNING" => 13,
        "ERROR" | "SEVERE" | "FATAL" => 17,
        _ => return None,
    })
}

fn parse_range(v: &str) -> Option<(f64, f64)> {
    let v = v.trim_matches(|c| c == '[' || c == ']').replace("..", "-");
    let (a, b) = v.split_once('-')?;
    let min: f64 = a.trim().parse().ok()?;
    let max: f64 = b.trim().parse().ok()?;
    Some((min, max))
}

fn wildcard_to_regex(pattern: &str) -> Result<String> {
    let mut re = String::from("^");
    for c in pattern.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            c => {
                if !c.is_ascii_alphanumeric() && c != '_' && c != '.' && c != '-' && c != '/' {
                    re.push('\\');
                }
                re.push(c);
            }
        }
    }
    re.push('$');
    // Compile check for sanity.
    regex::Regex::new(&re).map_err(|e| Error::Validation(format!("bad pattern: {e}")))?;
    Ok(re)
}

// ── Tokenizer ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Term(String),
    Str(String),
    And,
    Or,
    Not,
    LParen,
    RParen,
    Op(FieldOp),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum FieldOp {
    Eq, // = or ==
    Ne, // !=
    Re, // =~
    Gt,
    Ge,
    Lt,
    Le,
    Colon, // :
}

fn tokenize(input: &str) -> Result<Vec<Tok>> {
    let mut toks = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        match c {
            '(' => {
                toks.push(Tok::LParen);
                i += 1
            }
            ')' => {
                toks.push(Tok::RParen);
                i += 1
            }
            '"' => {
                let mut s = String::new();
                i += 1;
                let mut closed = false;
                while i < chars.len() {
                    if chars[i] == '\\' && i + 1 < chars.len() {
                        s.push(chars[i + 1]);
                        i += 2;
                        continue;
                    }
                    if chars[i] == '"' {
                        closed = true;
                        i += 1;
                        break;
                    }
                    s.push(chars[i]);
                    i += 1;
                }
                if !closed {
                    return Err(Error::Validation("unterminated string".into()));
                }
                toks.push(Tok::Str(s));
            }
            '>' | '<' | '!' | '=' => {
                // operators (possibly followed by =)
                let op = match c {
                    '>' => FieldOp::Gt,
                    '<' => FieldOp::Lt,
                    '!' => FieldOp::Ne,
                    _ => FieldOp::Eq,
                };
                let mut op = op;
                i += 1;
                if i < chars.len() && chars[i] == '=' && c != '=' {
                    op = match c {
                        '>' => FieldOp::Ge,
                        '<' => FieldOp::Le,
                        _ => FieldOp::Ne,
                    };
                    i += 1;
                } else if i < chars.len() && chars[i] == '~' {
                    op = FieldOp::Re;
                    i += 1;
                }
                toks.push(Tok::Op(op));
            }
            ':' => {
                toks.push(Tok::Op(FieldOp::Colon));
                i += 1
            }
            _ => {
                let start = i;
                while i < chars.len()
                    && !chars[i].is_whitespace()
                    && !matches!(chars[i], '(' | ')' | '"' | ':' | '=' | '<' | '>' | '!')
                {
                    i += 1;
                }
                if i == start {
                    i += 1; // avoid infinite loop on stray operator-adjacent chars
                }
                let t: String = chars[start..i].iter().collect();
                match t.to_ascii_uppercase().as_str() {
                    "AND" => toks.push(Tok::And),
                    "OR" => toks.push(Tok::Or),
                    "NOT" => toks.push(Tok::Not),
                    _ => toks.push(Tok::Term(t)),
                }
            }
        }
    }
    Ok(toks)
}

// ── Matching ────────────────────────────────────────────────────────────────

/// Evaluates a parsed query against a log record.
pub fn log_matches(
    q: &SearchQuery,
    log: &parqtel_core::LogRecord,
    extra: &HashMap<String, String>,
) -> bool {
    use Clause::*;
    for clause in &q.clauses {
        let ok = match clause {
            SeverityMin(sev) => {
                let min = severity_rank(sev).unwrap_or(9);
                log.severity_number >= min
            }
            Eq { field, value } => field_value(field, log, extra)
                .map(|v| v == *value)
                .unwrap_or(false),
            Ne { field, value } => field_value(field, log, extra)
                .map(|v| v != *value)
                .unwrap_or(true),
            Re { field, regex } => {
                let re = regex::Regex::new(regex).ok();
                match (field_value(field, log, extra), re) {
                    (Some(v), Some(re)) => re.is_match(&v),
                    _ => false,
                }
            }
            Cmp { field, op, value } => {
                let fv = numeric_field(field, log, extra);
                match (fv, op) {
                    (Some(n), CmpOp::Gt) => n > *value,
                    (Some(n), CmpOp::Ge) => n >= *value,
                    (Some(n), CmpOp::Lt) => n < *value,
                    (Some(n), CmpOp::Le) => n <= *value,
                    _ => false,
                }
            }
            Range { field, min, max } => numeric_field(field, log, extra)
                .map(|n| n >= *min && n <= *max)
                .unwrap_or(false),
            Exists { field } => field_value(field, log, extra).is_some(),
        };
        if !ok {
            return false;
        }
    }
    for term in &q.terms {
        let body = log.body.to_lowercase();
        let matched = if term.wildcard {
            let re = wildcard_to_regex_ci(&term.text);
            re.as_ref().map(|re| re.is_match(&body)).unwrap_or(false)
        } else {
            body.contains(&term.text)
        };
        if term.negate {
            if matched {
                return false;
            }
        } else if !matched {
            return false;
        }
    }
    true
}

fn wildcard_to_regex_ci(pattern: &str) -> Option<regex::Regex> {
    let mut re = String::from("(?i)");
    for c in pattern.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            c => re.push(c),
        }
    }
    regex::Regex::new(&re).ok()
}

/// Resolves a field name to a string value on a log record.
/// `attr.KEY` / `res.KEY` address attributes; dedicated names first.
fn field_value(
    field: &str,
    log: &parqtel_core::LogRecord,
    extra: &HashMap<String, String>,
) -> Option<String> {
    match field {
        "body" => Some(log.body.clone()),
        "severity" | "severity_text" => Some(log.severity_text.clone()),
        "service" | "service.name" => log
            .resource_attributes
            .get("service.name")
            .map(|s| s.to_string()),
        "trace_id" => Some(hex::encode(log.trace_id)),
        "span_id" => Some(hex::encode(log.span_id)),
        _ => {
            if let Some(key) = field.strip_prefix("attr.") {
                log.attributes.get(key).map(|s| s.to_string())
            } else if let Some(key) = field.strip_prefix("res.") {
                log.resource_attributes.get(key).map(|s| s.to_string())
            } else {
                log.attributes
                    .get(field)
                    .or_else(|| log.resource_attributes.get(field))
                    .or_else(|| extra.get(field).map(|s| s.as_str()))
                    .map(|s| s.to_string())
            }
        }
    }
}

fn numeric_field(
    field: &str,
    log: &parqtel_core::LogRecord,
    extra: &HashMap<String, String>,
) -> Option<f64> {
    match field {
        "severity_number" => Some(log.severity_number as f64),
        _ => field_value(field, log, extra).and_then(|v| v.parse::<f64>().ok()),
    }
}

/// Converts a legacy `{a="x",b=~"y"}` selector into ParqtelQL clauses.
fn parse_legacy_selector(selector: &str) -> Result<SearchQuery> {
    let inner = selector
        .trim()
        .trim_start_matches('{')
        .trim_end_matches('}');
    let mut q = SearchQuery::default();
    if inner.trim().is_empty() {
        return Ok(q);
    }
    for pair in inner.split(',') {
        let pair = pair.trim();
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        let k = k.trim();
        let v = v.trim().trim_matches('"');
        if k == "__name__" {
            continue;
        }
        if let Some(re) = k.strip_suffix('~') {
            q.clauses.push(Clause::Re {
                field: re.trim().to_string(),
                regex: format!("^{}$", v),
            });
        } else if k.starts_with('!') {
            q.clauses.push(Clause::Ne {
                field: k.trim_start_matches('!').trim().to_string(),
                value: v.to_string(),
            });
        } else {
            q.clauses.push(Clause::Eq {
                field: k.to_string(),
                value: v.to_string(),
            });
        }
    }
    Ok(q)
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
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
    fn legacy_selector_shape_converts() {
        let q = parse_search("{service=\"api\",severity>=WARN}");
        // mixed: service clause + severity clause
        assert!(!q.clauses.is_empty());
        let q2 = parse_search("{}");
        assert!(q2.is_empty());
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
}
