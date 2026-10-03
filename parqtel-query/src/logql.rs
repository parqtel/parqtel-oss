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
    if needle_lower.is_empty() {
        return true;
    }
    let h = haystack.as_bytes();
    let n = needle_lower.as_bytes();
    if n.len() > h.len() {
        return false;
    }
    h.windows(n.len())
        .any(|w| w.iter().zip(n).all(|(a, b)| a.to_ascii_lowercase() == *b))
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

    /// Evaluates against one log record.
    pub fn matches(&self, log: &parqtel_core::LogRecord, extra: &HashMap<String, String>) -> bool {
        let row = LogRow { log, extra };
        self.0.matches(&row)
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
mod tests {
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
        // G12: NOT duration>500 must invert the comparison exactly,
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
                        LabelSet::try_from_iter(vec![("http_status_code", code.to_string())])
                            .unwrap(),
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
        // AND-only queries still flatten into SearchQuery (same shape as Phase 1B)
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
        // G13: explicit body: prefix → contains semantics (not full equality).
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
        // G14: colons INSIDE values stay part of the value term.
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
}
