//! Matching prepared queries against log records.

use super::ast::{Clause, CmpOp, Predicate, SearchQuery};
use super::prepared::{
    contains_ci, prefilter_clause_is_exact, prefilter_term_is_exact, ColumnPrefilter,
    PrefilterBuilder, PreparedClause, PreparedPredicate, PreparedTerm,
};
use parqtel_core::Result;
use std::collections::HashMap;

/// A flat (AND-only) search query with its per-query work already done.
#[derive(Debug, Clone)]
pub struct PreparedLogQuery {
    pub(super) clauses: Vec<PreparedClause>,
    pub(super) terms: Vec<PreparedTerm>,
}

impl PreparedLogQuery {
    /// Prepares a [`SearchQuery`]. Build once, reuse for every row.
    pub fn new(q: &SearchQuery) -> Self {
        Self {
            clauses: q.clauses.iter().map(PreparedClause::prepare).collect(),
            terms: q.terms.iter().map(PreparedTerm::prepare).collect(),
        }
    }

    /// Whether the query constrains anything.
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty() && self.terms.is_empty()
    }

    /// Number of regexes compiled while preparing this query.
    ///
    /// Exposed so a test can assert the count is a property of the *query*, not
    /// of the row count — which is the whole point of preparing.
    pub fn compiled_patterns(&self) -> usize {
        fn clause_patterns(c: &PreparedClause) -> usize {
            match c {
                PreparedClause::Re { regex, .. } => usize::from(regex.is_some()),
                PreparedClause::Not(inner) => clause_patterns(inner),
                _ => 0,
            }
        }
        fn term_patterns(t: &PreparedTerm) -> usize {
            match t {
                PreparedTerm::Wildcard { regex, .. } => usize::from(regex.is_some()),
                _ => 0,
            }
        }
        self.clauses.iter().map(clause_patterns).sum::<usize>()
            + self.terms.iter().map(term_patterns).sum::<usize>()
    }

    /// Conditions on the severity and body columns that every matching row meets,
    /// for rejecting rows before they are decoded. See [`ColumnPrefilter`].
    pub fn column_prefilter(&self) -> ColumnPrefilter {
        self.prefilter(false)
    }

    /// See [`PreparedPredicate::column_prefilter_without_extra`].
    pub fn column_prefilter_without_extra(&self) -> ColumnPrefilter {
        self.prefilter(true)
    }

    pub(super) fn prefilter(&self, bare_ok: bool) -> ColumnPrefilter {
        let mut pre = ColumnPrefilter::default();
        for clause in &self.clauses {
            pre.add_clause(clause, bare_ok);
        }
        for term in &self.terms {
            pre.add_term(term);
        }
        pre.exact = self.clauses.iter().all(prefilter_clause_is_exact)
            && self.terms.iter().all(prefilter_term_is_exact);
        pre
    }

    /// Evaluates against one log record.
    pub fn matches(&self, log: &parqtel_core::LogRecord, extra: &HashMap<String, String>) -> bool {
        let row = LogRow { log, extra };
        self.matches_row(&row)
    }

    fn matches_row(&self, row: &LogRow<'_>) -> bool {
        self.clauses.iter().all(|c| c.matches(row)) && self.terms.iter().all(|t| t.matches(row))
    }
}

impl PreparedClause {
    pub(super) fn matches(&self, row: &LogRow<'_>) -> bool {
        match self {
            PreparedClause::SeverityMin(min) => row.log.severity_number >= *min,
            PreparedClause::BodyContains {
                needle_lower,
                negate,
            } => {
                let found = contains_ci(&row.log.body, needle_lower);
                if *negate {
                    !found
                } else {
                    found
                }
            }
            PreparedClause::Eq { field, value } => row
                .field_value(field)
                .map(|v| v.as_ref() == value.as_str())
                .unwrap_or(false),
            PreparedClause::Ne { field, value } => row
                .field_value(field)
                .map(|v| v.as_ref() != value.as_str())
                .unwrap_or(true),
            PreparedClause::Re { field, regex } => match (row.field_value(field), regex) {
                (Some(v), Some(re)) => re.is_match(v.as_ref()),
                _ => false,
            },
            PreparedClause::Cmp { field, op, value } => match (row.numeric_field(field), op) {
                (Some(n), CmpOp::Gt) => n > *value,
                (Some(n), CmpOp::Ge) => n >= *value,
                (Some(n), CmpOp::Lt) => n < *value,
                (Some(n), CmpOp::Le) => n <= *value,
                _ => false,
            },
            PreparedClause::Range { field, min, max } => row
                .numeric_field(field)
                .map(|n| n >= *min && n <= *max)
                .unwrap_or(false),
            PreparedClause::Exists { field } => row.field_value(field).is_some(),
            PreparedClause::Not(inner) => !inner.matches(row),
        }
    }
}

impl PreparedTerm {
    pub(super) fn matches(&self, row: &LogRow<'_>) -> bool {
        match self {
            PreparedTerm::Contains {
                needle_lower,
                negate,
            } => {
                let found = contains_ci(&row.log.body, needle_lower);
                if *negate {
                    !found
                } else {
                    found
                }
            }
            PreparedTerm::Wildcard { regex, negate } => {
                let found = regex
                    .as_ref()
                    .map(|re| re.is_match(&row.log.body))
                    .unwrap_or(false);
                if *negate {
                    !found
                } else {
                    found
                }
            }
        }
    }
}

/// One log record, plus borrowed field access.
///
/// Field resolution returns a [`Cow`] so the common case — `body`,
/// `severity_text`, an attribute — borrows instead of allocating a `String`
/// per clause per row.
pub(super) struct LogRow<'a> {
    pub(super) log: &'a parqtel_core::LogRecord,
    pub(super) extra: &'a HashMap<String, String>,
}

impl LogRow<'_> {
    fn field_value(&self, field: &str) -> Option<std::borrow::Cow<'_, str>> {
        use std::borrow::Cow;
        match field {
            "body" => Some(Cow::Borrowed(&self.log.body)),
            "severity" | "severity_text" => Some(Cow::Borrowed(&self.log.severity_text)),
            "service" | "service.name" => self
                .log
                .resource_attributes
                .get("service.name")
                .map(Cow::Borrowed),
            // Hex encoding has to allocate; it is only reached when a query
            // actually selects on trace_id/span_id, which is rare.
            "trace_id" => Some(Cow::Owned(hex::encode(self.log.trace_id))),
            "span_id" => Some(Cow::Owned(hex::encode(self.log.span_id))),
            _ => self.resolve_other(field),
        }
    }

    /// Resolves a field name on a log record.
    /// `attr.KEY` / `res.KEY` address attributes; dedicated names first.
    ///
    /// Bare names fall back attributes -> resource attributes -> `extra`, in
    /// that order. The precedence is load-bearing: a pipeline-enriched field in
    /// `extra` must not shadow an attribute of the same name on the record.
    fn resolve_other(&self, field: &str) -> Option<std::borrow::Cow<'_, str>> {
        use std::borrow::Cow;
        if let Some(key) = field.strip_prefix("attr.") {
            return self.log.attributes.get(key).map(Cow::Borrowed);
        }
        if let Some(key) = field.strip_prefix("res.") {
            return self.log.resource_attributes.get(key).map(Cow::Borrowed);
        }
        if let Some(v) = self.log.attributes.get(field) {
            return Some(Cow::Borrowed(v));
        }
        if let Some(v) = self.log.resource_attributes.get(field) {
            return Some(Cow::Borrowed(v));
        }
        self.extra.get(field).map(|v| Cow::Borrowed(v.as_str()))
    }

    /// Numeric field. `severity_number` is a dedicated column, so it is
    /// checked before any string parse, as before.
    fn numeric_field(&self, field: &str) -> Option<f64> {
        if field == "severity_number" {
            return Some(self.log.severity_number as f64);
        }
        self.field_value(field).and_then(|v| v.parse::<f64>().ok())
    }
}

/// Evaluates a parsed query against a log record.
///
/// Prepares the query first, so calling this per row repeats the preparation.
/// Prefer [`PreparedLogQuery`] in a row loop.
pub fn log_matches(
    q: &SearchQuery,
    log: &parqtel_core::LogRecord,
    extra: &HashMap<String, String>,
) -> bool {
    PreparedLogQuery::new(q).matches(log, extra)
}

/// Evaluates a boolean predicate tree against a log record.
///
/// Prepares the predicate first, so calling this per row repeats the
/// preparation. Prefer [`PreparedPredicate`] in a row loop.
pub fn log_matches_predicate(
    pred: &Predicate,
    log: &parqtel_core::LogRecord,
    extra: &HashMap<String, String>,
) -> bool {
    PreparedPredicate::new(pred).matches(log, extra)
}

pub(super) fn wildcard_to_regex_ci(pattern: &str) -> Option<regex::Regex> {
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

/// Converts a legacy `{a="x",b=~"y"}` selector into ParqtelQL clauses.
pub(super) fn parse_legacy_selector(selector: &str) -> Result<SearchQuery> {
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
