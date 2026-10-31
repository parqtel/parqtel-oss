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

/// Boolean predicate tree (Phase 2): OR of ANDs of atoms.
#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    /// Matches when ALL sub-predicates match.
    And(Vec<Predicate>),
    /// Matches when ANY sub-predicate matches.
    Or(Vec<Predicate>),
    /// Matches when the sub-predicate does NOT match.
    Not(Box<Predicate>),
    /// Leaf: a single clause or term.
    Atom(Atom),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Atom {
    Clause(Clause),
    Term(SearchTerm),
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
    /// G12: `NOT <clause>` — the inner clause must NOT match. Keeps
    /// negation exact for range/comparison/exists clauses instead of
    /// downgrading to positive matching.
    Not(Box<Clause>),
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
        if let Ok(q) = parse_legacy_selector(trimmed) {
            return q;
        }
    }
    // AND-only queries flatten into SearchQuery (backward-compatible);
    // anything with OR/NOT/grouping keeps the full tree.
    match parse_predicate(trimmed) {
        Ok(pred) => flatten_and(&pred).unwrap_or_default(),
        Err(_) => {
            // Lenient fallback: treat the whole input as body terms.
            let mut q = SearchQuery::default();
            for tok in trimmed.split_whitespace() {
                if !tok.is_empty() {
                    q.terms.push(SearchTerm {
                        text: tok.trim_matches('"').to_lowercase(),
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

/// Parses a full boolean predicate tree with precedence
/// OR < implicit-AND < NOT. Returns Err only on tokenization failure.
pub fn parse_predicate(input: &str) -> Result<Predicate> {
    let toks = tokenize(input)?;
    let mut p = TreeParser { toks, pos: 0 };
    let pred = p.parse_or()?;
    if p.pos < p.toks.len() {
        return Err(Error::Validation("trailing tokens".into()));
    }
    Ok(pred)
}

struct TreeParser {
    toks: Vec<Tok>,
    pos: usize,
}

impl TreeParser {
    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn parse_or(&mut self) -> Result<Predicate> {
        let mut branches = vec![self.parse_and()?];
        while matches!(self.peek(), Some(Tok::Or)) {
            self.pos += 1;
            branches.push(self.parse_and()?);
        }
        if branches.len() == 1 {
            Ok(branches
                .pop()
                .ok_or_else(|| Error::Validation("empty or".into()))?)
        } else {
            Ok(Predicate::Or(branches))
        }
    }

    fn parse_and(&mut self) -> Result<Predicate> {
        let mut parts = vec![self.parse_not()?];
        loop {
            match self.peek() {
                Some(Tok::And) => {
                    self.pos += 1;
                    parts.push(self.parse_not()?);
                }
                // implicit AND: a term/clause directly follows
                Some(Tok::Term(_)) | Some(Tok::Str(_)) | Some(Tok::Not) | Some(Tok::LParen) => {
                    parts.push(self.parse_not()?);
                }
                _ => break,
            }
        }
        if parts.len() == 1 {
            Ok(parts
                .pop()
                .ok_or_else(|| Error::Validation("empty and".into()))?)
        } else {
            Ok(Predicate::And(parts))
        }
    }

    fn parse_not(&mut self) -> Result<Predicate> {
        if matches!(self.peek(), Some(Tok::Not)) {
            self.pos += 1;
            return Ok(Predicate::Not(Box::new(self.parse_not()?)));
        }
        self.parse_atom()
    }

    fn parse_atom(&mut self) -> Result<Predicate> {
        match self.peek().cloned() {
            Some(Tok::LParen) => {
                self.pos += 1;
                let inner = self.parse_or()?;
                if !matches!(self.peek(), Some(Tok::RParen)) {
                    return Err(Error::Validation("missing )".into()));
                }
                self.pos += 1;
                Ok(inner)
            }
            Some(Tok::Str(s)) => {
                self.pos += 1;
                Ok(Predicate::Atom(Atom::Term(SearchTerm {
                    text: s.to_lowercase(),
                    negate: false,
                    phrase: true,
                    wildcard: false,
                })))
            }
            Some(Tok::Term(t)) => {
                // Peek the NEXT token (without consuming the Term) so
                // build_field_clause sees the same [Term, Op, Value] shape
                // the flat parser did (it advances past all three).
                if let Some(Tok::Op(op)) = self.toks.get(self.pos + 1).cloned() {
                    if let Some((field, _)) = split_field(&t) {
                        let mut consumed = self.pos; // AT the Term, like the old parser
                        if let Some(clause) = build_field_clause(
                            &field,
                            op,
                            self.toks.get(self.pos + 2),
                            &mut consumed,
                        )? {
                            self.pos = consumed;
                            return Ok(Predicate::Atom(Atom::Clause(clause)));
                        }
                    }
                }
                self.pos += 1;
                Ok(Predicate::Atom(Atom::Term(build_term(&t, false)?)))
            }
            other => Err(Error::Validation(format!(
                "unexpected token {other:?} in predicate"
            ))),
        }
    }
}

/// Flattens an AND-only tree into a SearchQuery. Returns None when the
/// tree contains OR/Not (the caller must use the tree path).
fn flatten_and(pred: &Predicate) -> Option<SearchQuery> {
    let mut q = SearchQuery::default();
    if !collect_and(pred, &mut q) {
        return None;
    }
    Some(q)
}

fn collect_and(pred: &Predicate, q: &mut SearchQuery) -> bool {
    match pred {
        Predicate::And(parts) => parts.iter().all(|p| collect_and(p, q)),
        Predicate::Atom(Atom::Clause(c)) => {
            q.clauses.push(c.clone());
            true
        }
        Predicate::Atom(Atom::Term(t)) => {
            q.terms.push(t.clone());
            true
        }
        Predicate::Or(_) => false,
        Predicate::Not(inner) => match &**inner {
            // NOT of a single clause flattens to the inverted clause.
            Predicate::Atom(Atom::Clause(cl)) => {
                q.clauses.push(Clause::Not(Box::new(cl.clone())));
                true
            }
            Predicate::Atom(Atom::Term(t)) => {
                let mut t = t.clone();
                t.negate = !t.negate;
                q.terms.push(t);
                true
            }
            // NOT of compound nodes requires the tree path.
            _ => false,
        },
    }
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

pub fn severity_rank(sev: &str) -> Option<i32> {
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
                // G14: `:` separates field from value. The value term may
                // itself contain colons (URLs, times) — scan it to the
                // next whitespace/quote without breaking on ':'.
                toks.push(Tok::Op(FieldOp::Colon));
                i += 1;
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }
                if i < chars.len() && chars[i] != '"' {
                    let start = i;
                    while i < chars.len()
                        && !chars[i].is_whitespace()
                        && !matches!(chars[i], '(' | ')' | '"')
                    {
                        i += 1;
                    }
                    if i > start {
                        let t: String = chars[start..i].iter().collect();
                        push_term_like(&mut toks, t);
                    }
                }
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

/// Keyword-aware term push (AND/OR/NOT classification).
fn push_term_like(toks: &mut Vec<Tok>, t: String) {
    match t.to_ascii_uppercase().as_str() {
        "AND" => toks.push(Tok::And),
        "OR" => toks.push(Tok::Or),
        "NOT" => toks.push(Tok::Not),
        _ => toks.push(Tok::Term(t)),
    }
}

// ── Matching ────────────────────────────────────────────────────────────────

// ---------------------------------------------------------------------------
// Prepared queries
//
// Everything below the parser used to run *per row*: a regex was compiled, a
// `SearchQuery` was built from a cloned clause, every field value was cloned
// out of its `LabelSet`, and the whole body was lowercased once per search
// term. For a 5 000-row block with a three-term query that is thousands of
// regex compilations and allocations whose answers are identical for every
// row.
//
// A prepared query moves all of that to parse time. It is a pure function of
// the query string, so it is built once per request and then applied to every
// row.
// ---------------------------------------------------------------------------

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
enum PreparedClause {
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
enum PreparedTerm {
    /// Substring of the raw body, case-insensitive.
    Contains { needle_lower: String, negate: bool },
    /// `(?i)`-prefixed, so it matches the raw body without lowercasing it.
    Wildcard {
        regex: Option<regex::Regex>,
        negate: bool,
    },
}

impl PreparedClause {
    fn prepare(clause: &Clause) -> Self {
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
    fn prepare(term: &SearchTerm) -> Self {
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

    fn prefilter(&self, bare_ok: bool) -> ColumnPrefilter {
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
fn prefilter_clause_is_exact(clause: &PreparedClause) -> bool {
    matches!(
        clause,
        PreparedClause::SeverityMin(_) | PreparedClause::BodyContains { negate: false, .. }
    )
}

/// Terms [`PrefilterBuilder::add_term`] captures completely.
fn prefilter_term_is_exact(term: &PreparedTerm) -> bool {
    matches!(term, PreparedTerm::Contains { negate: false, .. })
}

trait PrefilterBuilder {
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
    fn new(pred: &Predicate) -> Self {
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

    fn matches(&self, row: &LogRow<'_>) -> bool {
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

/// A flat (AND-only) search query with its per-query work already done.
#[derive(Debug, Clone)]
pub struct PreparedLogQuery {
    clauses: Vec<PreparedClause>,
    terms: Vec<PreparedTerm>,
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

    fn prefilter(&self, bare_ok: bool) -> ColumnPrefilter {
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
    fn matches(&self, row: &LogRow<'_>) -> bool {
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
    fn matches(&self, row: &LogRow<'_>) -> bool {
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
struct LogRow<'a> {
    log: &'a parqtel_core::LogRecord,
    extra: &'a HashMap<String, String>,
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

// --- thin wrappers, kept so existing callers and tests keep working ---------

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
    fn prepare(clause: &Clause) -> Self {
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

    fn matches(&self, s: &parqtel_core::Span) -> bool {
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
    fn prepare(term: &SearchTerm) -> Self {
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

    fn matches(&self, s: &parqtel_core::Span) -> bool {
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

#[cfg(test)]
mod tests;
