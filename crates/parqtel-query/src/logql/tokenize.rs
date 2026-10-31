//! Query-string tokenizer.

use parqtel_core::{Error, Result};

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Tok {
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
pub(super) enum FieldOp {
    Eq, // = or ==
    Ne, // !=
    Re, // =~
    Gt,
    Ge,
    Lt,
    Le,
    Colon, // :
}

pub(super) fn tokenize(input: &str) -> Result<Vec<Tok>> {
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
                // `:` separates field from value. The value term may
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
