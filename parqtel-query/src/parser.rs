//! PromQL-compatible expression parser (Pratt / precedence-climbing).
//!
//! Grammar (Prometheus-compatible subset for Phase 1A):
//! ```text
//! expr        := unary | binary
//! binary      := expr OP expr                 (precedence-climbed)
//! primary     := number | selector | call | aggregation | range | paren
//! selector    := name? { matchers } [range]? [offset]?
//! call        := name ( args )
//! aggregation := (sum|avg|...)[by|without(list)] ( expr [, param] )
//! range       := primary [duration [:step]] [offset duration]
//! ```

use crate::ast::*;
use crate::matcher::parse_selector;
use parqtel_core::{Error, Result};

// ── Tokenizer ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Tok {
    Ident(String), // metric names, function names, keywords, label names
    Number(f64),
    Str(String), // quoted string
    LBrace,
    RBrace, // { }
    LBracket,
    RBracket, // [ ]
    LParen,
    RParen, // ( )
    Comma,
    Colon, // , :
    // operators
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Pow,
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,       // = != > < >= <=  (label ops + binary)
    EqStrict, // == (binary equality)
    Assign,   // = inside matchers
    MatchRe,
    NotMatchRe, // =~ !~
    And,
    Or,
    Unless,
    At, // @ modifier (parsed, rejected with clear error)
    Eof,
}

#[derive(Debug, Clone)]
pub struct Lexer<'a> {
    src: &'a [u8],
    pos: usize,
}

impl<'a> Lexer<'a> {
    fn new(src: &'a str) -> Self {
        Self {
            src: src.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.src.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_ascii_whitespace() {
                self.pos += 1;
            } else {
                break;
            }
        }
    }

    pub fn next_token(&mut self) -> Result<Tok> {
        self.skip_ws();
        let Some(c) = self.peek() else {
            return Ok(Tok::Eof);
        };
        match c {
            b'{' => {
                self.pos += 1;
                Ok(Tok::LBrace)
            }
            b'}' => {
                self.pos += 1;
                Ok(Tok::RBrace)
            }
            b'[' => {
                self.pos += 1;
                Ok(Tok::LBracket)
            }
            b']' => {
                self.pos += 1;
                Ok(Tok::RBracket)
            }
            b'(' => {
                self.pos += 1;
                Ok(Tok::LParen)
            }
            b')' => {
                self.pos += 1;
                Ok(Tok::RParen)
            }
            b',' => {
                self.pos += 1;
                Ok(Tok::Comma)
            }
            b':' => {
                self.pos += 1;
                Ok(Tok::Colon)
            }
            b'+' => {
                self.pos += 1;
                Ok(Tok::Add)
            }
            b'-' => {
                self.pos += 1;
                Ok(Tok::Sub)
            }
            b'*' => {
                self.pos += 1;
                Ok(Tok::Mul)
            }
            b'/' => {
                self.pos += 1;
                Ok(Tok::Div)
            }
            b'%' => {
                self.pos += 1;
                Ok(Tok::Mod)
            }
            b'^' => {
                self.pos += 1;
                Ok(Tok::Pow)
            }
            b'=' => {
                self.pos += 1;
                match self.peek() {
                    Some(b'=') => {
                        self.pos += 1;
                        Ok(Tok::EqStrict)
                    }
                    Some(b'~') => {
                        self.pos += 1;
                        Ok(Tok::MatchRe)
                    }
                    _ => Ok(Tok::Assign),
                }
            }
            b'!' => {
                self.pos += 1;
                match self.peek() {
                    Some(b'=') => {
                        self.pos += 1;
                        Ok(Tok::Ne)
                    }
                    Some(b'~') => {
                        self.pos += 1;
                        Ok(Tok::NotMatchRe)
                    }
                    _ => Err(Error::Validation("stray '!' in query".into())),
                }
            }
            b'>' => {
                self.pos += 1;
                match self.peek() {
                    Some(b'=') => {
                        self.pos += 1;
                        Ok(Tok::Ge)
                    }
                    _ => Ok(Tok::Gt),
                }
            }
            b'<' => {
                self.pos += 1;
                match self.peek() {
                    Some(b'=') => {
                        self.pos += 1;
                        Ok(Tok::Le)
                    }
                    _ => Ok(Tok::Lt),
                }
            }
            b'"' | b'\'' => self.lex_string(c),
            b'@' => {
                self.pos += 1;
                Ok(Tok::At)
            }
            _ if c.is_ascii_digit() => self.lex_number_or_duration(),
            b'.' if matches!(self.src.get(self.pos + 1), Some(d) if d.is_ascii_digit()) => {
                self.lex_number_or_duration()
            }
            _ if c == b'_' || c.is_ascii_alphabetic() => self.lex_ident(),
            _ => Err(Error::Validation(format!(
                "unexpected character {:?} in query",
                c as char
            ))),
        }
    }

    fn lex_string(&mut self, quote: u8) -> Result<Tok> {
        self.pos += 1; // opening quote
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'\\' {
                self.pos += 2;
                continue;
            }
            if c == quote {
                let s = String::from_utf8_lossy(&self.src[start..self.pos]).into_owned();
                self.pos += 1;
                return Ok(Tok::Str(s));
            }
            self.pos += 1;
        }
        Err(Error::Validation("unterminated string".into()))
    }

    /// Digits followed immediately by duration-unit letters (5m, 1h30m,
    /// 250ms) lex as a single Ident so duration parsing sees them whole.
    /// Plain numbers (including 1.5, 1e3) lex as Number.
    fn lex_number_or_duration(&mut self) -> Result<Tok> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.pos += 1;
        }
        let digits_end = self.pos;
        // Duration units after the digits?
        if matches!(self.peek(), Some(c) if c == b'm' || c == b'h' || c == b's' || c == b'd' || c == b'w' || c == b'y')
        {
            self.pos += 1;
            // allow ms multi-char unit
            if self.src.get(digits_end) == Some(&b'm') && self.peek() == Some(b's') {
                self.pos += 1;
            }
            // compound: 1h30m — keep consuming digit+unit pairs
            loop {
                let before = self.pos;
                while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                    self.pos += 1;
                }
                if matches!(self.peek(), Some(c) if c == b'm' || c == b'h' || c == b's' || c == b'd' || c == b'w')
                {
                    self.pos += 1;
                    if self.src.get(self.pos.wrapping_sub(2)) == Some(&b'm')
                        && self.peek() == Some(b's')
                    {
                        self.pos += 1;
                    }
                } else {
                    self.pos = before;
                    break;
                }
            }
            let text = String::from_utf8_lossy(&self.src[start..self.pos]).into_owned();
            return Ok(Tok::Ident(text));
        }
        // Not a duration: rewind fully and lex a plain number from start.
        self.pos = start;
        self.lex_number()
    }

    fn lex_number(&mut self) -> Result<Tok> {
        let start = self.pos;
        let mut seen_dot = false;
        let mut seen_exp = false;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' => self.pos += 1,
                b'.' if !seen_dot && !seen_exp => {
                    seen_dot = true;
                    self.pos += 1
                }
                b'e' | b'E' if !seen_exp && self.pos > start => {
                    seen_exp = true;
                    self.pos += 1;
                    if matches!(self.peek(), Some(b'+') | Some(b'-')) {
                        self.pos += 1;
                    }
                }
                _ => break,
            }
        }
        let text = String::from_utf8_lossy(&self.src[start..self.pos]).into_owned();
        text.parse::<f64>()
            .map(Tok::Number)
            .map_err(|_| Error::Validation(format!("invalid number {text:?}")))
    }

    fn lex_ident(&mut self) -> Result<Tok> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == b'_' || c == b'.' || c.is_ascii_alphanumeric() {
                self.pos += 1;
            } else {
                break;
            }
        }
        let text = String::from_utf8_lossy(&self.src[start..self.pos]).into_owned();
        match text.as_str() {
            "and" => Ok(Tok::And),
            "or" => Ok(Tok::Or),
            "unless" => Ok(Tok::Unless),
            _ => Ok(Tok::Ident(text)),
        }
    }
}

// ── Parser ──────────────────────────────────────────────────────────────────

pub struct Parser<'a> {
    lexer: Lexer<'a>,
    tok: Option<Tok>,
    peeked: Option<Tok>,
}

/// Parses a full expression into an AST.
pub fn parse_expr(query: &str) -> Result<Expr> {
    let mut p = Parser {
        lexer: Lexer::new(query),
        tok: None,
        peeked: None,
    };
    p.advance()?;
    let e = p.parse_binary(0)?;
    // Ensure the whole input was consumed (Eof is the only allowed token).
    if p.tok != Some(Tok::Eof) {
        return Err(Error::Validation(format!(
            "unexpected trailing tokens after expression: {:?}",
            p.tok
        )));
    }
    Ok(e)
}

impl<'a> Parser<'a> {
    fn advance(&mut self) -> Result<()> {
        if self.peeked.is_some() {
            self.tok = self.peeked.take();
        } else if self.tok != Some(Tok::Eof) {
            self.tok = Some(self.lexer.next_token()?);
        }
        Ok(())
    }

    fn expect(&mut self, t: &Tok) -> Result<()> {
        if self.tok.as_ref() == Some(t) {
            self.advance()?;
            Ok(())
        } else {
            Err(Error::Validation(format!(
                "expected {t:?}, found {:?}",
                self.tok
            )))
        }
    }

    fn ident(&mut self) -> Result<String> {
        match self.tok.clone() {
            Some(Tok::Ident(s)) => {
                self.advance()?;
                Ok(s)
            }
            other => Err(Error::Validation(format!(
                "expected identifier, found {other:?}"
            ))),
        }
    }

    // ── Binary operator precedence (PromQL) ─────────────────────────────
    fn op_precedence(t: &Tok) -> Option<(u8, BinaryOp)> {
        let (prec, op) = match t {
            Tok::Or => (1u8, BinaryOp::Or),
            Tok::And => (2, BinaryOp::And),
            Tok::Unless => (2, BinaryOp::Unless),
            Tok::EqStrict => (3, BinaryOp::Eq),
            Tok::Ne => (3, BinaryOp::Ne),
            Tok::Gt => (3, BinaryOp::Gt),
            Tok::Lt => (3, BinaryOp::Lt),
            Tok::Ge => (3, BinaryOp::Ge),
            Tok::Le => (3, BinaryOp::Le),
            Tok::Add => (4, BinaryOp::Add),
            Tok::Sub => (4, BinaryOp::Sub),
            Tok::Mul => (5, BinaryOp::Mul),
            Tok::Div => (5, BinaryOp::Div),
            Tok::Mod => (5, BinaryOp::Mod),
            Tok::Pow => (6, BinaryOp::Pow),
            _ => return None,
        };
        Some((prec, op))
    }

    /// Precedence-climbing binary expression parser.
    fn parse_binary(&mut self, min_prec: u8) -> Result<Expr> {
        let mut lhs = self.parse_unary()?;
        while let Some((prec, op)) = self.tok.as_ref().and_then(Self::op_precedence) {
            if prec < min_prec {
                break;
            }
            self.advance()?; // consume operator

            // Vector matching modifiers: on/ignoring + group_left/right
            let mut matching: Option<(VectorMatch, MatchCardinality)> = None;
            let mut return_bool = false;
            loop {
                match self.tok.clone() {
                    Some(Tok::Ident(k)) if k == "bool" && op.is_comparison() => {
                        self.advance()?;
                        return_bool = true;
                    }
                    Some(Tok::Ident(k)) if k == "on" || k == "ignoring" => {
                        self.advance()?;
                        let labels = self.parse_label_list()?;
                        let vm = if k == "on" {
                            VectorMatch::On(labels)
                        } else {
                            VectorMatch::Ignoring(labels)
                        };
                        let card = self.parse_cardinality()?;
                        matching = Some((vm, card));
                    }
                    Some(Tok::Ident(k)) if k == "group_left" || k == "group_right" => {
                        // group_left without preceding on/ignoring: invalid in
                        // PromQL; give a clear error.
                        return Err(Error::Validation(
                            "group_left/group_right requires on() or ignoring() first".into(),
                        ));
                    }
                    _ => break,
                }
            }

            let rhs = self.parse_binary(prec + 1)?;
            lhs = Expr::Binary(BinaryExpr {
                op,
                lhs: Box::new(lhs),
                rhs: Box::new(rhs),
                matching,
                return_bool,
            });
        }
        Ok(lhs)
    }

    fn parse_cardinality(&mut self) -> Result<MatchCardinality> {
        match self.tok.clone() {
            Some(Tok::Ident(k)) if k == "group_left" => {
                self.advance()?;
                let labels = self.parse_label_list()?;
                Ok(MatchCardinality::ManyToOne(labels))
            }
            Some(Tok::Ident(k)) if k == "group_right" => {
                self.advance()?;
                let labels = self.parse_label_list()?;
                Ok(MatchCardinality::OneToMany(labels))
            }
            _ => Ok(MatchCardinality::OneToOne),
        }
    }

    /// `(a, b, c)` — after on/ignoring/group_left consumed.
    fn parse_label_list(&mut self) -> Result<Vec<String>> {
        let mut labels = Vec::new();
        if self.tok != Some(Tok::LParen) {
            return Ok(labels);
        }
        self.advance()?;
        loop {
            match self.tok.clone() {
                Some(Tok::RParen) => {
                    self.advance()?;
                    break;
                }
                Some(Tok::Comma) => self.advance()?,
                Some(Tok::Ident(l)) => {
                    self.advance()?;
                    labels.push(l)
                }
                other => {
                    return Err(Error::Validation(format!(
                        "expected label name in list, found {other:?}"
                    )))
                }
            }
        }
        Ok(labels)
    }

    fn parse_unary(&mut self) -> Result<Expr> {
        match self.tok.clone() {
            Some(Tok::Sub) => {
                self.advance()?;
                let inner = self.parse_unary()?;
                Ok(Expr::Binary(BinaryExpr {
                    op: BinaryOp::Sub,
                    lhs: Box::new(Expr::Number(0.0)),
                    rhs: Box::new(inner),
                    matching: None,
                    return_bool: false,
                }))
            }
            Some(Tok::Add) => {
                self.advance()?;
                self.parse_unary()
            }
            _ => self.parse_postfix_chain(),
        }
    }

    /// primary followed by [range][offset] and/or @ (rejected).
    fn parse_postfix_chain(&mut self) -> Result<Expr> {
        let mut e = self.parse_primary()?;
        loop {
            match self.tok.clone() {
                Some(Tok::LBracket) => {
                    self.advance()?;
                    let (range_ns, step_ns) = self.parse_bracket()?;
                    let offset_ns = self.parse_offset()?;
                    e = Expr::Range(RangeExpr {
                        expr: Box::new(e),
                        range_ns,
                        step_ns,
                        offset_ns,
                    });
                }
                Some(Tok::Ident(k)) if k == "offset" => {
                    // offset without preceding [range] applies to selectors —
                    // handled inside parse_primary; reaching here means the
                    // offset came after a non-selector; treat as range offset.
                    let offset_ns = self.parse_offset()?;
                    e = Expr::Range(RangeExpr {
                        expr: Box::new(e),
                        range_ns: 0,
                        step_ns: None,
                        offset_ns,
                    });
                }
                Some(Tok::At) => {
                    return Err(Error::Validation(
                        "@ timestamp modifier is not supported yet".into(),
                    ));
                }
                _ => break,
            }
        }
        Ok(e)
    }

    /// Parses the inside of `[...]`: `5m`, `1h:30s` (subquery), or `:30s`.
    fn parse_bracket(&mut self) -> Result<(i64, Option<i64>)> {
        // Empty leading (subquery with default range handled by caller):
        let mut range_ns: Option<i64> = None;
        let mut step_ns: Option<i64> = None;
        match self.tok.clone() {
            Some(Tok::Number(0.0)) => {
                // `[:step]`? no — zero-range is invalid; read as duration 0
                self.advance()?;
                range_ns = Some(0);
            }
            Some(Tok::Ident(_)) => {
                let d = self.parse_duration()?;
                range_ns = Some(d);
            }
            Some(Tok::Colon) => {}
            other => {
                return Err(Error::Validation(format!(
                    "expected duration in [range], found {other:?}"
                )))
            }
        }
        if self.tok == Some(Tok::Colon) {
            self.advance()?; // consume ':'
                             // optional step
            if matches!(self.tok, Some(Tok::Ident(_))) {
                step_ns = Some(self.parse_duration()?);
            }
        }
        self.expect(&Tok::RBracket)?;
        let range = range_ns.ok_or_else(|| {
            // `[` alone isn't valid; require a range for plain selectors but
            // allow bare `[:step]` subqueries with unlimited range? PromQL
            // requires a range for subqueries too.
            Error::Validation("missing range duration in [range:step]".into())
        })?;
        Ok((range, step_ns))
    }

    fn parse_duration(&mut self) -> Result<i64> {
        // Durations lex as Ident ("5m") or Number+Ident ("300" then "s")?
        // Our lexer eats digits into Number; "5m" lexes as Number(5) then
        // Ident("m") only when adjacent. Simplest: parse a duration token
        // as either Ident("5m") or Number followed by Ident unit.
        match self.tok.clone() {
            Some(Tok::Ident(s)) => {
                self.advance()?;
                crate::matcher::parse_duration_str(&s)
            }
            Some(Tok::Number(n)) => {
                self.advance()?;
                // bare number = seconds
                Ok((n as i64) * 1_000_000_000)
            }
            other => Err(Error::Validation(format!(
                "expected duration, found {other:?}"
            ))),
        }
    }

    fn parse_offset(&mut self) -> Result<i64> {
        if self.tok == Some(Tok::Ident("offset".into())) {
            self.advance()?;
            match self.tok.clone() {
                Some(Tok::Ident(s)) => {
                    self.advance()?;
                    crate::matcher::parse_duration_str(&s)
                }
                Some(Tok::Number(n)) => {
                    self.advance()?;
                    Ok((n as i64) * 1_000_000_000)
                }
                other => Err(Error::Validation(format!(
                    "expected duration after offset, found {other:?}"
                ))),
            }
        } else {
            Ok(0)
        }
    }

    fn parse_primary(&mut self) -> Result<Expr> {
        match self.tok.clone() {
            Some(Tok::Number(n)) => {
                self.advance()?;
                Ok(Expr::Number(n))
            }
            Some(Tok::Str(_)) => {
                // String literals are only valid as function/aggregation
                // parameters (count_values("label"), label_replace args); in
                // expression position they evaluate like a NaN scalar
                // (PromQL has no string vectors).
                self.advance()?;
                Ok(Expr::Number(f64::NAN))
            }
            Some(Tok::LParen) => {
                self.advance()?;
                let e = self.parse_binary(0)?;
                self.expect(&Tok::RParen)?;
                Ok(Expr::Paren(Box::new(e)))
            }
            Some(Tok::Ident(_)) => self.parse_ident_start(),
            other => Err(Error::Validation(format!(
                "unexpected token {other:?} in expression"
            ))),
        }
    }

    /// An identifier begins: a function call, aggregation, or selector.
    fn parse_ident_start(&mut self) -> Result<Expr> {
        let name = self.ident()?;
        // Aggregations allow a by/without modifier BEFORE the paren:
        // `sum by (job) (...)` — check that shape first.
        if agg_op_from_name(&name).is_some() {
            if let Some(Tok::Ident(k)) = self.tok.clone() {
                if k == "by" || k == "without" {
                    self.advance()?;
                    let labels = self.parse_label_list()?;
                    self.expect(&Tok::LParen)?;
                    let agg = agg_op_from_name(&name)
                        .ok_or_else(|| Error::Validation("impossible".into()))?;
                    let grouping = if k == "by" {
                        Grouping::By(labels)
                    } else {
                        Grouping::Without(labels)
                    };
                    return self.parse_aggregation_body(agg, name, grouping);
                }
            }
        }
        match self.tok.clone() {
            // Function call or aggregation: `name(...)`
            Some(Tok::LParen) => {
                self.advance()?;
                if let Some(agg) = agg_op_from_name(&name) {
                    self.parse_aggregation(agg, name)
                } else {
                    self.parse_call(name)
                }
            }
            // Selector with braces: `name{...}` or bare `name`
            _ => {
                // Bare selector without braces — validate matchers empty.
                let (metric, matchers) = match self.tok {
                    Some(Tok::LBrace) => {
                        // Reuse the matcher parser on the brace section.
                        let brace_src = self.capture_braces()?;
                        parse_selector(&format!("{}{}", name, brace_src))?
                    }
                    _ => (Some(name.clone()), Vec::new()),
                };
                let offset_ns = self.parse_offset()?;
                let mut e = Expr::Selector(SelectorExpr {
                    metric_name: metric,
                    matchers,
                });
                if offset_ns != 0 {
                    e = Expr::Range(RangeExpr {
                        expr: Box::new(e),
                        range_ns: 0,
                        step_ns: None,
                        offset_ns,
                    });
                }
                Ok(e)
            }
        }
    }

    /// Capture `{...}` raw text and consume the tokens (for the legacy
    /// selector parser). Returns e.g. `{a="b",c=~"d"}` including braces.
    fn capture_braces(&mut self) -> Result<String> {
        self.expect(&Tok::LBrace)?;
        let mut out = String::from("{");
        let mut depth = 1;
        loop {
            match self.tok.clone() {
                Some(Tok::LBrace) => {
                    depth += 1;
                    out.push('{');
                    self.advance()?
                }
                Some(Tok::RBrace) => {
                    depth -= 1;
                    self.advance()?;
                    out.push('}');
                    if depth == 0 {
                        break;
                    }
                }
                Some(Tok::Str(s)) => {
                    out.push('"');
                    out.push_str(&s);
                    out.push('"');
                    self.advance()?
                }
                Some(Tok::Comma) => {
                    out.push(',');
                    self.advance()?
                }
                Some(Tok::Assign) => {
                    out.push('=');
                    self.advance()?
                }
                Some(Tok::EqStrict) => {
                    out.push_str("==");
                    self.advance()?
                }
                Some(Tok::Ne) => {
                    out.push_str("!=");
                    self.advance()?
                }
                Some(Tok::MatchRe) => {
                    out.push_str("=~");
                    self.advance()?
                }
                Some(Tok::NotMatchRe) => {
                    out.push_str("!~");
                    self.advance()?
                }
                Some(Tok::Ident(s)) => {
                    out.push_str(&s);
                    self.advance()?
                }
                Some(Tok::Number(n)) => {
                    out.push_str(&n.to_string());
                    self.advance()?
                }
                other => {
                    return Err(Error::Validation(format!(
                        "unexpected {other:?} inside selector braces"
                    )))
                }
            }
        }
        Ok(out)
    }

    fn parse_call(&mut self, name: String) -> Result<Expr> {
        let mut args = Vec::new();
        if self.tok == Some(Tok::RParen) {
            self.advance()?;
            return Ok(Expr::Call(CallExpr { name, args }));
        }
        loop {
            args.push(self.parse_binary(0)?);
            match self.tok.clone() {
                Some(Tok::Comma) => self.advance()?,
                Some(Tok::RParen) => {
                    self.advance()?;
                    break;
                }
                other => {
                    return Err(Error::Validation(format!(
                        "expected , or ) in call to {name}, found {other:?}"
                    )))
                }
            }
        }
        Ok(Expr::Call(CallExpr { name, args }))
    }

    fn parse_aggregation(&mut self, op: AggregationOp, name: String) -> Result<Expr> {
        self.parse_aggregation_body(op, name, Grouping::None)
    }

    /// Aggregation body after `name` [+ by/without] + `(` consumed.
    fn parse_aggregation_body(
        &mut self,
        op: AggregationOp,
        name: String,
        grouping: Grouping,
    ) -> Result<Expr> {
        // Optional param (topk N, quantile φ, count_values "label")
        let mut param: Option<Box<Expr>> = None;
        let mut args = Vec::new();
        if self.tok == Some(Tok::RParen) {
            self.advance()?;
        } else {
            loop {
                let e = self.parse_binary(0)?;
                args.push(e);
                match self.tok.clone() {
                    Some(Tok::Comma) => self.advance()?,
                    Some(Tok::RParen) => {
                        self.advance()?;
                        break;
                    }
                    other => {
                        return Err(Error::Validation(format!(
                            "expected , or ) in {name}(...), found {other:?}"
                        )))
                    }
                }
            }
        }
        // topk/bottomk/quantile/count_values take (param, expr)
        if matches!(
            op,
            AggregationOp::TopK
                | AggregationOp::BottomK
                | AggregationOp::Quantile
                | AggregationOp::CountValues
        ) {
            if args.len() != 2 {
                return Err(Error::Validation(format!(
                    "{name}() takes 2 arguments (param, expression), got {}",
                    args.len()
                )));
            }
            param = Some(Box::new(args.remove(0)));
        } else if args.len() != 1 {
            return Err(Error::Validation(format!(
                "{name}() takes exactly 1 argument, got {}",
                args.len()
            )));
        }
        let expr = Box::new(
            args.pop()
                .ok_or_else(|| Error::Validation(format!("{name}() missing expression")))?,
        );
        Ok(Expr::Aggregation(AggregationExpr {
            op,
            grouping,
            param,
            expr,
        }))
    }
}

fn agg_op_from_name(name: &str) -> Option<AggregationOp> {
    Some(match name {
        "sum" => AggregationOp::Sum,
        "avg" => AggregationOp::Avg,
        "min" => AggregationOp::Min,
        "max" => AggregationOp::Max,
        "count" => AggregationOp::Count,
        "stddev" => AggregationOp::Stddev,
        "stdvar" => AggregationOp::Stdvar,
        "count_values" => AggregationOp::CountValues,
        "topk" => AggregationOp::TopK,
        "bottomk" => AggregationOp::BottomK,
        "quantile" => AggregationOp::Quantile,
        "group" => AggregationOp::Group,
        _ => return None,
    })
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
    use super::*;

    fn assert_parses(q: &str) -> Expr {
        parse_expr(q).unwrap_or_else(|e| panic!("{q:?} failed: {e}"))
    }

    #[test]
    fn lexer_duration_tokens() {
        let mut l = Lexer::new("cpu > bool 80 offset 5m");
        let toks: Vec<Tok> = vec![
            Tok::Ident("cpu".into()),
            Tok::Gt,
            Tok::Ident("bool".into()),
            Tok::Number(80.0),
            Tok::Ident("offset".into()),
            Tok::Ident("5m".into()),
            Tok::Eof,
        ];
        for want in toks {
            let got = l.next_token().unwrap();
            assert_eq!(got, want, "token mismatch");
        }
    }

    #[test]
    fn bare_selector() {
        assert_parses("cpu_usage");
    }

    #[test]
    fn avg_over_time_parses() {
        let e = assert_parses("avg_over_time(cpu_usage[5m])");
        match &e {
            Expr::Call(c) => {
                assert_eq!(c.name, "avg_over_time");
                assert_eq!(c.args.len(), 1);
                assert!(matches!(c.args[0], Expr::Range(_)));
            }
            other => panic!("expected call, got {other:?}"),
        }
    }

    #[test]
    fn selector_with_matchers() {
        assert_parses(r#"http_requests_total{service.name="api",method=~"GET|POST"}"#);
    }

    #[test]
    fn nested_call_aggregation() {
        let e = assert_parses("sum by (job) (rate(http_requests_total[5m]))");
        assert!(matches!(e, Expr::Aggregation(_)));
    }

    #[test]
    fn binary_ratio() {
        let e = assert_parses(r#"a{f="x"} / b{f="y"}"#);
        match e {
            Expr::Binary(b) => {
                assert_eq!(b.op, BinaryOp::Div);
                assert!(b.matching.is_none());
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn vector_matching_on_group_left() {
        let e = assert_parses(r#"a * on(x, y) group_left(z) b"#);
        match e {
            Expr::Binary(b) => {
                let (vm, card) = b.matching.unwrap();
                assert!(matches!(vm, VectorMatch::On(ls) if ls == vec!["x", "y"]));
                assert!(matches!(card, MatchCardinality::ManyToOne(ls) if ls == vec!["z"]));
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn precedence_mul_over_add() {
        // 1 + 2 * 3 parses as 1 + (2*3)
        let e = assert_parses("1 + 2 * 3");
        match e {
            Expr::Binary(b) => {
                assert_eq!(b.op, BinaryOp::Add);
                assert!(matches!(*b.rhs, Expr::Binary(r) if r.op == BinaryOp::Mul));
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn paren_grouping_overrides() {
        // (1 + 2) * 3 parses as (1+2) * 3
        let e = assert_parses("(1 + 2) * 3");
        match e {
            Expr::Binary(b) => {
                assert_eq!(b.op, BinaryOp::Mul);
                assert!(matches!(*b.lhs, Expr::Paren(_)));
            }
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn subquery_range_with_step() {
        let e = assert_parses("max_over_time(rate(x[5m])[1h:30s])");
        assert!(matches!(e, Expr::Call(_)));
    }

    #[test]
    fn aggregation_without() {
        assert_parses("sum without (instance) (cpu)");
    }

    #[test]
    fn unary_minus() {
        assert_parses("-cpu_usage");
    }

    #[test]
    fn offset_on_selector() {
        assert_parses("cpu_usage offset 5m");
    }

    #[test]
    fn bool_modifier() {
        let e = assert_parses("cpu > bool 80");
        match e {
            Expr::Binary(b) => assert!(b.return_bool),
            other => panic!("expected binary, got {other:?}"),
        }
    }

    #[test]
    fn topk_two_args() {
        let e = assert_parses("topk(5, cpu_usage)");
        match e {
            Expr::Aggregation(a) => {
                assert_eq!(a.op, AggregationOp::TopK);
                assert!(a.param.is_some());
            }
            other => panic!("expected aggregation, got {other:?}"),
        }
    }

    #[test]
    fn absent_range_parses() {
        let e = assert_parses(r#"absent_over_time(nonexistent{env="prod"}[5m])"#);
        match &e {
            Expr::Call(call) => {
                assert_eq!(call.name, "absent_over_time");
                assert!(
                    matches!(call.args[0], Expr::Range(_)),
                    "arg0 is {:?}",
                    call.args[0]
                );
            }
            other => panic!("expected call, got {other:?}"),
        }
    }

    #[test]
    fn chained_composition() {
        // The canonical RED pattern
        assert_parses(
            r#"histogram_quantile(0.9, sum by (le, job) (rate(http_request_duration_seconds_bucket[5m])))"#,
        );
    }

    #[test]
    fn rejects_at_modifier() {
        assert!(parse_expr("x @ 100").is_err());
    }

    #[test]
    fn rejects_trailing_garbage() {
        assert!(parse_expr("cpu_usage )").is_err());
    }

    #[test]
    fn comparison_ops() {
        assert_parses("cpu_usage > 0.8");
        assert_parses("cpu_usage >= 0.8");
        assert_parses("cpu_usage < 0.8");
        assert_parses("cpu_usage <= 0.8");
        assert_parses("cpu_usage == 0.8");
        assert_parses("cpu_usage != 0.8");
    }

    #[test]
    fn set_operators() {
        assert_parses(r#"a{f="1"} and b"#);
        assert_parses(r#"a{f="1"} unless b"#);
        assert_parses(r#"a or b"#);
    }
}
