//! Query parser: tokens to [`SearchQuery`] and [`Predicate`] trees.

use super::ast::{Atom, Clause, CmpOp, Predicate, SearchQuery, SearchTerm};
use super::logs::parse_legacy_selector;
use super::tokenize::{tokenize, FieldOp, Tok};
use parqtel_core::{Error, Result};

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
                // SeverityMin too (a documented simplification).
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
