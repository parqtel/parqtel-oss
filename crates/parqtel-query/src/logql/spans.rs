//! Matching prepared queries against trace spans.

use super::ast::{Clause, CmpOp, SearchQuery, SearchTerm};
use super::logs::wildcard_to_regex_ci;
use super::prepared::contains_ci;

/// A span search query with its per-query work already done.
///
/// Same reasoning as [`PreparedLogQuery`]: the previous span path compiled a
/// regex, and lowercased the operation name and *every attribute value*, once
/// per term per span.
#[derive(Debug, Clone)]
pub struct PreparedSpanQuery {
    clauses: Vec<PreparedSpanClause>,
    terms: Vec<PreparedSpanTerm>,
}

#[derive(Debug, Clone)]
enum PreparedSpanClause {
    Eq {
        field: String,
        value: String,
    },
    Ne {
        field: String,
        value: String,
    },
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
    /// Severity is not applicable to spans; kept so a shared query still matches.
    AlwaysTrue,
    Not(Box<PreparedSpanClause>),
}

#[derive(Debug, Clone)]
enum PreparedSpanTerm {
    Contains {
        needle_lower: String,
        negate: bool,
    },
    Wildcard {
        regex: Option<regex::Regex>,
        negate: bool,
    },
}

impl PreparedSpanQuery {
    /// Prepares a [`SearchQuery`] for span matching. Build once, reuse.
    pub fn new(q: &SearchQuery) -> Self {
        Self {
            clauses: q.clauses.iter().map(PreparedSpanClause::prepare).collect(),
            terms: q.terms.iter().map(PreparedSpanTerm::prepare).collect(),
        }
    }

    /// Whether the query constrains anything.
    pub fn is_empty(&self) -> bool {
        self.clauses.is_empty() && self.terms.is_empty()
    }

    /// Evaluates against one span.
    pub fn matches(&self, s: &parqtel_core::Span) -> bool {
        self.clauses.iter().all(|c| c.matches(s)) && self.terms.iter().all(|t| t.matches(s))
    }
}

impl PreparedSpanClause {
    pub(super) fn prepare(clause: &Clause) -> Self {
        match clause {
            Clause::Eq { field, value } => PreparedSpanClause::Eq {
                field: field.clone(),
                value: value.clone(),
            },
            Clause::Ne { field, value } => PreparedSpanClause::Ne {
                field: field.clone(),
                value: value.clone(),
            },
            Clause::Re { field, regex } => PreparedSpanClause::Re {
                field: field.clone(),
                regex: regex::Regex::new(regex).ok(),
            },
            Clause::Cmp { field, op, value } => PreparedSpanClause::Cmp {
                field: field.clone(),
                op: *op,
                value: *value,
            },
            Clause::Range { field, min, max } => PreparedSpanClause::Range {
                field: field.clone(),
                min: *min,
                max: *max,
            },
            Clause::Exists { field } => PreparedSpanClause::Exists {
                field: field.clone(),
            },
            Clause::SeverityMin(_) => PreparedSpanClause::AlwaysTrue,
            Clause::Not(inner) => {
                PreparedSpanClause::Not(Box::new(PreparedSpanClause::prepare(inner)))
            }
        }
    }

    pub(super) fn matches(&self, s: &parqtel_core::Span) -> bool {
        match self {
            PreparedSpanClause::Eq { field, value } => span_field(s, field)
                .map(|v| v.eq_ignore_ascii_case(value))
                .unwrap_or(false),
            PreparedSpanClause::Ne { field, value } => span_field(s, field)
                .map(|v| !v.eq_ignore_ascii_case(value))
                .unwrap_or(true),
            PreparedSpanClause::Re { field, regex } => match (span_field(s, field), regex) {
                (Some(v), Some(re)) => re.is_match(&v),
                _ => false,
            },
            PreparedSpanClause::Cmp { field, op, value } => {
                let n = if field == "duration" || field == "duration_ms" {
                    Some(s.duration_ns() as f64 / 1_000_000.0)
                } else {
                    span_field(s, field).and_then(|v| v.parse::<f64>().ok())
                };
                match n {
                    Some(n) => match op {
                        CmpOp::Gt => n > *value,
                        CmpOp::Ge => n >= *value,
                        CmpOp::Lt => n < *value,
                        CmpOp::Le => n <= *value,
                    },
                    None => false,
                }
            }
            PreparedSpanClause::Range { field, min, max } => {
                if field == "duration" || field == "duration_ms" {
                    let d = s.duration_ns() as f64 / 1_000_000.0;
                    d >= *min && d <= *max
                } else {
                    false
                }
            }
            PreparedSpanClause::Exists { field } => span_field(s, field).is_some(),
            PreparedSpanClause::AlwaysTrue => true,
            PreparedSpanClause::Not(inner) => !inner.matches(s),
        }
    }
}

impl PreparedSpanTerm {
    pub(super) fn prepare(term: &SearchTerm) -> Self {
        if term.wildcard {
            PreparedSpanTerm::Wildcard {
                regex: wildcard_to_regex_ci(&term.text),
                negate: term.negate,
            }
        } else {
            PreparedSpanTerm::Contains {
                needle_lower: term.text.to_lowercase(),
                negate: term.negate,
            }
        }
    }

    pub(super) fn matches(&self, s: &parqtel_core::Span) -> bool {
        match self {
            PreparedSpanTerm::Contains {
                needle_lower,
                negate,
            } => {
                // Case-insensitive against the raw name and attribute values —
                // no per-term lowercase allocation.
                let found = contains_ci(&s.name, needle_lower)
                    || s.attributes
                        .iter()
                        .any(|(_, v)| contains_ci(v, needle_lower));
                if *negate {
                    !found
                } else {
                    found
                }
            }
            PreparedSpanTerm::Wildcard { regex, negate } => {
                let found = match regex {
                    Some(re) => {
                        re.is_match(&s.name) || s.attributes.iter().any(|(_, v)| re.is_match(v))
                    }
                    None => false,
                };
                if *negate {
                    !found
                } else {
                    found
                }
            }
        }
    }
}

/// Applies a ParqtelQL SearchQuery to a span: service/status/duration/
/// kind/name/attr.* predicates push down into the trace scan.
///
/// Prepares first, so calling this per span repeats the preparation. Prefer
/// [`PreparedSpanQuery`] in a scan loop.
pub fn span_matches(q: &SearchQuery, s: &parqtel_core::Span) -> bool {
    PreparedSpanQuery::new(q).matches(s)
}

/// Resolves a ParqtelQL field name to a span value.
pub fn span_field(s: &parqtel_core::Span, field: &str) -> Option<String> {
    match field {
        "service" | "service.name" => s.attributes.get("service.name").map(|v| v.to_string()),
        "name" | "operation" | "operation_name" => Some(s.name.clone()),
        "status" => Some(
            match s.status.code {
                2 => "ERROR",
                1 => "OK",
                _ => "UNSET",
            }
            .to_string(),
        ),
        "kind" => Some(
            match s.kind {
                1 => "internal",
                2 => "server",
                3 => "client",
                4 => "producer",
                5 => "consumer",
                _ => "unspecified",
            }
            .to_string(),
        ),
        "trace_id" => Some(hex::encode(s.trace_id)),
        _ => {
            if let Some(key) = field.strip_prefix("attr.") {
                s.attributes.get(key).map(|v| v.to_string())
            } else {
                s.attributes.get(field).map(|v| v.to_string())
            }
        }
    }
}
