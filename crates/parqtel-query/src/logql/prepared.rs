//! Predicates compiled once per request and applied to every row.
//!
//! Matching used to run per row: a regex was compiled, a `SearchQuery` was
//! built from a cloned clause, every field value was cloned out of its
//! `LabelSet`, and the whole body was lowercased once per search term. For a
//! 5 000-row block with a three-term query that is thousands of regex
//! compilations and allocations whose answers are identical for every row.
//! A prepared query moves all of that to parse time. It is a pure function
//! of the query string.

use super::ast::{Atom, Clause, CmpOp, Predicate, SearchTerm};
use super::logs::{wildcard_to_regex_ci, LogRow};
use super::parse::severity_rank;
use std::collections::HashMap;

/// Case-insensitive substring test without allocating a lowercased haystack.
///
/// Scans byte windows, comparing case-folded, so a 2 KB body costs no
/// allocation per term per row. The needle must already be lowercase, which
/// the prepared clauses and terms guarantee.
pub fn contains_ci(haystack: &str, needle_lower: &str) -> bool {
    parqtel_core::text::contains_ascii_ci(haystack, needle_lower)
}

/// A [`Clause`] with its per-query work already done.
#[derive(Debug, Clone)]
pub(super) enum PreparedClause {
    /// Threshold resolved from the severity name at parse time.
    SeverityMin(i32),
    /// `body:` / `body=` — a case-insensitive substring of the raw body.
    BodyContains {
        needle_lower: String,
        negate: bool,
    },
    Eq {
        field: String,
        value: String,
    },
    Ne {
        field: String,
        value: String,
    },
    /// Compiled once. `None` means the pattern was invalid, which the previous
    /// code re-derived (and swallowed) on every row.
    Re {
        field: String,
        regex: Option<regex::Regex>,
    },
    Cmp {
        field: String,
        op: CmpOp,
        value: f64,
    },
    Range {
        field: String,
        min: f64,
        max: f64,
    },
    Exists {
        field: String,
    },
    Not(Box<PreparedClause>),
}

/// A [`SearchTerm`] with its per-query work already done.
#[derive(Debug, Clone)]
pub(super) enum PreparedTerm {
    /// Substring of the raw body, case-insensitive.
    Contains { needle_lower: String, negate: bool },
    /// `(?i)`-prefixed, so it matches the raw body without lowercasing it.
    Wildcard {
        regex: Option<regex::Regex>,
        negate: bool,
    },
}

impl PreparedClause {
    pub(super) fn prepare(clause: &Clause) -> Self {
        match clause {
            Clause::SeverityMin(sev) => {
                PreparedClause::SeverityMin(severity_rank(sev).unwrap_or(9))
            }
            Clause::Eq { field, value } if field == "body" => PreparedClause::BodyContains {
                needle_lower: value.to_lowercase(),
                negate: false,
            },
            Clause::Ne { field, value } if field == "body" => PreparedClause::BodyContains {
                needle_lower: value.to_lowercase(),
                negate: true,
            },
            Clause::Eq { field, value } => PreparedClause::Eq {
                field: field.clone(),
                value: value.clone(),
            },
            Clause::Ne { field, value } => PreparedClause::Ne {
                field: field.clone(),
                value: value.clone(),
            },
            Clause::Re { field, regex } => PreparedClause::Re {
                field: field.clone(),
                regex: regex::Regex::new(regex).ok(),
            },
            Clause::Cmp { field, op, value } => PreparedClause::Cmp {
                field: field.clone(),
                op: *op,
                value: *value,
            },
            Clause::Range { field, min, max } => PreparedClause::Range {
                field: field.clone(),
                min: *min,
                max: *max,
            },
            Clause::Exists { field } => PreparedClause::Exists {
                field: field.clone(),
            },
            Clause::Not(inner) => PreparedClause::Not(Box::new(PreparedClause::prepare(inner))),
        }
    }
}

impl PreparedTerm {
    pub(super) fn prepare(term: &SearchTerm) -> Self {
        if term.wildcard {
            PreparedTerm::Wildcard {
                regex: wildcard_to_regex_ci(&term.text),
                negate: term.negate,
            }
        } else {
            PreparedTerm::Contains {
                needle_lower: term.text.to_lowercase(),
                negate: term.negate,
            }
        }
    }
}

/// A boolean predicate tree with every leaf prepared.
///
/// Mirrors [`Predicate`] exactly; `matches` walks it with the same precedence
/// the parser applied, so results are identical to
/// [`log_matches_predicate`] — which is now a thin wrapper over this.
#[derive(Debug, Clone)]
pub struct PreparedPredicate(PreparedPredicateKind);

#[derive(Debug, Clone)]
enum PreparedPredicateKind {
    And(Vec<PreparedPredicate>),
    Or(Vec<PreparedPredicate>),
    Not(Box<PreparedPredicate>),
    Clause(PreparedClause),
    Term(PreparedTerm),
}

impl PreparedPredicate {
    /// Prepares a parsed predicate tree. Build once, reuse for every row.
    pub fn new(pred: &Predicate) -> Self {
        Self(PreparedPredicateKind::new(pred))
    }

    /// Conditions on the severity and body columns that every matching row meets,
    /// for rejecting rows before they are decoded.
    ///
    /// Only conjuncts count: a row matching an `AND` meets all its parts, so each
    /// part's requirement is necessary for the whole. An `OR` or a negation
    /// contributes nothing, because a matching row need not meet any one branch.
    pub fn column_prefilter(&self) -> ColumnPrefilter {
        self.prefilter(false)
    }

    /// As [`Self::column_prefilter`], for callers that evaluate the predicate with
    /// no pipeline-enriched (`extra`) fields. A bare field name is then known to
    /// resolve to an attribute or resource attribute, so an equality on it can
    /// also narrow which granules are read.
    pub fn column_prefilter_without_extra(&self) -> ColumnPrefilter {
        self.prefilter(true)
    }

    pub(super) fn prefilter(&self, bare_ok: bool) -> ColumnPrefilter {
        let mut pre = ColumnPrefilter::default();
        self.0.collect_prefilter(&mut pre, bare_ok);
        pre.exact = self.0.is_prefilter_exact();
        pre
    }

    /// Evaluates against one log record.
    pub fn matches(&self, log: &parqtel_core::LogRecord, extra: &HashMap<String, String>) -> bool {
        let row = LogRow { log, extra };
        self.0.matches(&row)
    }
}

/// See [`PreparedPredicate::column_prefilter`].
pub type ColumnPrefilter = parqtel_core::storage::LogColumnPrefilter;

/// The name under which a record carries the value an equality on `field` looks
/// at, if the block's skip index records it.
///
/// `service`, `attr.K`, `res.K`, `trace_id` and `span_id` name their source
/// outright. A bare name is looked up in the attributes, then the resource
/// attributes, then pipeline-enriched fields; only when the caller has no such
/// fields (`bare_ok`) is the record's own pair a necessary condition.
fn indexed_pair_name(field: &str, bare_ok: bool) -> Option<String> {
    match field {
        "service" | "service.name" => Some("service.name".to_string()),
        "trace_id" | "span_id" => Some(field.to_string()),
        // Columns of their own, or not indexed.
        "body" | "severity" | "severity_text" => None,
        _ => {
            if let Some(key) = field
                .strip_prefix("attr.")
                .or_else(|| field.strip_prefix("res."))
            {
                Some(key.to_string())
            } else if bare_ok {
                Some(field.to_string())
            } else {
                None
            }
        }
    }
}

/// Clauses [`PrefilterBuilder::add_clause`] captures completely.
pub(super) fn prefilter_clause_is_exact(clause: &PreparedClause) -> bool {
    matches!(
        clause,
        PreparedClause::SeverityMin(_) | PreparedClause::BodyContains { negate: false, .. }
    )
}

/// Terms [`PrefilterBuilder::add_term`] captures completely.
pub(super) fn prefilter_term_is_exact(term: &PreparedTerm) -> bool {
    matches!(term, PreparedTerm::Contains { negate: false, .. })
}

pub(super) trait PrefilterBuilder {
    fn add_clause(&mut self, clause: &PreparedClause, bare_ok: bool);
    fn add_term(&mut self, term: &PreparedTerm);
}

impl PrefilterBuilder for ColumnPrefilter {
    fn add_clause(&mut self, clause: &PreparedClause, bare_ok: bool) {
        match clause {
            PreparedClause::SeverityMin(min) => {
                self.min_severity = Some(self.min_severity.map_or(*min, |m| m.max(*min)));
            }
            PreparedClause::BodyContains {
                needle_lower,
                negate: false,
            } => self.body_contains_lower.push(needle_lower.clone()),
            // An equality can only hold if the record carries that exact pair.
            PreparedClause::Eq { field, value } => {
                if let Some(name) = indexed_pair_name(field, bare_ok) {
                    self.attr_equals.push((name, value.clone()));
                }
            }
            _ => {}
        }
    }

    fn add_term(&mut self, term: &PreparedTerm) {
        if let PreparedTerm::Contains {
            needle_lower,
            negate: false,
        } = term
        {
            self.body_contains_lower.push(needle_lower.clone());
        }
    }
}

impl PreparedPredicateKind {
    /// Whether the predicate is nothing but conjuncts that
    /// [`Self::collect_prefilter`] turns into column conditions, so the prefilter
    /// accepts exactly the rows the predicate does.
    fn is_prefilter_exact(&self) -> bool {
        match self {
            PreparedPredicateKind::And(parts) => parts.iter().all(|p| p.0.is_prefilter_exact()),
            PreparedPredicateKind::Clause(c) => prefilter_clause_is_exact(c),
            PreparedPredicateKind::Term(t) => prefilter_term_is_exact(t),
            PreparedPredicateKind::Or(_) | PreparedPredicateKind::Not(_) => false,
        }
    }

    fn collect_prefilter(&self, pre: &mut ColumnPrefilter, bare_ok: bool) {
        match self {
            PreparedPredicateKind::And(parts) => {
                for part in parts {
                    part.0.collect_prefilter(pre, bare_ok);
                }
            }
            PreparedPredicateKind::Clause(c) => pre.add_clause(c, bare_ok),
            PreparedPredicateKind::Term(t) => pre.add_term(t),
            // An OR need not satisfy any one branch, and a NOT inverts its
            // requirement: neither yields a necessary condition.
            PreparedPredicateKind::Or(_) | PreparedPredicateKind::Not(_) => {}
        }
    }
}

impl PreparedPredicateKind {
    pub(super) fn new(pred: &Predicate) -> Self {
        match pred {
            Predicate::And(parts) => {
                PreparedPredicateKind::And(parts.iter().map(PreparedPredicate::new).collect())
            }
            Predicate::Or(parts) => {
                PreparedPredicateKind::Or(parts.iter().map(PreparedPredicate::new).collect())
            }
            Predicate::Not(inner) => {
                PreparedPredicateKind::Not(Box::new(PreparedPredicate::new(inner)))
            }
            Predicate::Atom(Atom::Clause(clause)) => {
                PreparedPredicateKind::Clause(PreparedClause::prepare(clause))
            }
            Predicate::Atom(Atom::Term(term)) => {
                PreparedPredicateKind::Term(PreparedTerm::prepare(term))
            }
        }
    }

    pub(super) fn matches(&self, row: &LogRow<'_>) -> bool {
        match self {
            // `.0` is required: `PreparedPredicate::matches` is the public
            // per-row entry point and would otherwise shadow this one.
            PreparedPredicateKind::And(parts) => parts.iter().all(|p| p.0.matches(row)),
            PreparedPredicateKind::Or(parts) => parts.iter().any(|p| p.0.matches(row)),
            PreparedPredicateKind::Not(inner) => !inner.0.matches(row),
            PreparedPredicateKind::Clause(c) => c.matches(row),
            PreparedPredicateKind::Term(t) => t.matches(row),
        }
    }
}
