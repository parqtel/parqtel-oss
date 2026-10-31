use std::collections::BTreeMap;

/// A compiled DQL condition that evaluates against signal record fields with zero allocation.
#[derive(Debug, Clone)]
pub enum CompiledCondition {
    Eq { field: String, value: String },
    Neq { field: String, value: String },
    Gt { field: String, value: f64 },
    Gte { field: String, value: f64 },
    Lt { field: String, value: f64 },
    Lte { field: String, value: f64 },
    Contains { field: String, value: String },
    Matches { field: String, re: regex::Regex },
    Exists { field: String },
    Between { field: String, low: f64, high: f64 },
    In { field: String, values: Vec<String> },
    And(Box<CompiledCondition>, Box<CompiledCondition>),
    Or(Box<CompiledCondition>, Box<CompiledCondition>),
    Not(Box<CompiledCondition>),
    True,
}

impl CompiledCondition {
    /// Evaluate against a flat field map (zero allocation at eval time).
    pub fn evaluate(&self, fields: &BTreeMap<String, String>) -> bool {
        match self {
            Self::True => true,
            Self::Eq { field, value } => fields.get(field).map(|v| v == value).unwrap_or(false),
            Self::Neq { field, value } => fields.get(field).map(|v| v != value).unwrap_or(true),
            Self::Gt { field, value } => fields
                .get(field)
                .and_then(|v| v.parse::<f64>().ok())
                .map(|v| v > *value)
                .unwrap_or(false),
            Self::Gte { field, value } => fields
                .get(field)
                .and_then(|v| v.parse::<f64>().ok())
                .map(|v| v >= *value)
                .unwrap_or(false),
            Self::Lt { field, value } => fields
                .get(field)
                .and_then(|v| v.parse::<f64>().ok())
                .map(|v| v < *value)
                .unwrap_or(false),
            Self::Lte { field, value } => fields
                .get(field)
                .and_then(|v| v.parse::<f64>().ok())
                .map(|v| v <= *value)
                .unwrap_or(false),
            Self::Contains { field, value } => fields
                .get(field)
                .map(|v| v.contains(value.as_str()))
                .unwrap_or(false),
            Self::Matches { field, re } => {
                fields.get(field).map(|v| re.is_match(v)).unwrap_or(false)
            }
            Self::Exists { field } => fields.contains_key(field),
            Self::Between { field, low, high } => fields
                .get(field)
                .and_then(|v| v.parse::<f64>().ok())
                .map(|v| v >= *low && v <= *high)
                .unwrap_or(false),
            Self::In { field, values } => fields
                .get(field)
                .map(|v| values.contains(v))
                .unwrap_or(false),
            Self::And(a, b) => a.evaluate(fields) && b.evaluate(fields),
            Self::Or(a, b) => a.evaluate(fields) || b.evaluate(fields),
            Self::Not(inner) => !inner.evaluate(fields),
        }
    }
}

/// Hand-written recursive descent parser for DQL.
pub struct DqlParser;

impl DqlParser {
    /// Parse a DQL expression string into a compiled condition.
    pub fn parse(input: &str) -> crate::Result<CompiledCondition> {
        let tokens = Lexer::tokenize(input)?;
        let mut parser = Parser::new(&tokens);
        let cond = parser.parse_expression()?;
        Ok(cond)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Ident(String),
    Str(String),
    Num(f64),
    Bool(bool),
    Op(String),
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
    And,
    Or,
    Not,
}

struct Lexer;

impl Lexer {
    fn tokenize(input: &str) -> crate::Result<Vec<Token>> {
        let mut tokens = Vec::new();
        let chars: Vec<char> = input.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            match chars[i] {
                ' ' | '\t' | '\n' | '\r' => i += 1,
                '(' => {
                    tokens.push(Token::LParen);
                    i += 1;
                }
                ')' => {
                    tokens.push(Token::RParen);
                    i += 1;
                }
                '[' => {
                    tokens.push(Token::LBracket);
                    i += 1;
                }
                ']' => {
                    tokens.push(Token::RBracket);
                    i += 1;
                }
                ',' => {
                    tokens.push(Token::Comma);
                    i += 1;
                }
                '"' => {
                    i += 1;
                    let start = i;
                    while i < chars.len() && chars[i] != '"' {
                        i += 1;
                    }
                    tokens.push(Token::Str(chars[start..i].iter().collect()));
                    i += 1; // skip closing quote
                }
                '!' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                    tokens.push(Token::Op("!=".into()));
                    i += 2;
                }
                '>' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                    tokens.push(Token::Op(">=".into()));
                    i += 2;
                }
                '<' if i + 1 < chars.len() && chars[i + 1] == '=' => {
                    tokens.push(Token::Op("<=".into()));
                    i += 2;
                }
                '=' if i + 1 < chars.len() && chars[i + 1] == '~' => {
                    tokens.push(Token::Op("=~".into()));
                    i += 2;
                }
                '=' => {
                    tokens.push(Token::Op("=".into()));
                    i += 1;
                }
                '>' => {
                    tokens.push(Token::Op(">".into()));
                    i += 1;
                }
                '<' => {
                    tokens.push(Token::Op("<".into()));
                    i += 1;
                }
                c if c.is_ascii_digit() || c == '-' => {
                    let start = i;
                    if c == '-' {
                        i += 1;
                    }
                    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                        i += 1;
                    }
                    let num_str: String = chars[start..i].iter().collect();
                    let num = num_str
                        .parse::<f64>()
                        .map_err(|_| crate::Error::Parse(format!("Invalid number: {}", num_str)))?;
                    tokens.push(Token::Num(num));
                }
                c if c.is_alphanumeric() || c == '_' || c == '.' => {
                    let start = i;
                    while i < chars.len()
                        && (chars[i].is_alphanumeric() || chars[i] == '_' || chars[i] == '.')
                    {
                        i += 1;
                    }
                    let word: String = chars[start..i].iter().collect();
                    match word.to_lowercase().as_str() {
                        "and" => tokens.push(Token::And),
                        "or" => tokens.push(Token::Or),
                        "not" => tokens.push(Token::Not),
                        "true" => tokens.push(Token::Bool(true)),
                        "false" => tokens.push(Token::Bool(false)),
                        "contains" | "matches" | "exists" | "between" | "in" => {
                            tokens.push(Token::Op(word.to_lowercase()));
                        }
                        _ => tokens.push(Token::Ident(word)),
                    }
                }
                other => {
                    return Err(crate::Error::Parse(format!(
                        "Unexpected character: '{}'",
                        other
                    )));
                }
            }
        }
        Ok(tokens)
    }
}

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token]) -> Self {
        Self { tokens, pos: 0 }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<&Token> {
        let tok = self.tokens.get(self.pos);
        self.pos += 1;
        tok
    }

    fn parse_expression(&mut self) -> crate::Result<CompiledCondition> {
        let mut left = self.parse_or()?;
        while let Some(Token::And) = self.peek() {
            self.advance();
            let right = self.parse_or()?;
            left = CompiledCondition::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_or(&mut self) -> crate::Result<CompiledCondition> {
        let mut left = self.parse_unary()?;
        while let Some(Token::Or) = self.peek() {
            self.advance();
            let right = self.parse_unary()?;
            left = CompiledCondition::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> crate::Result<CompiledCondition> {
        if let Some(Token::Not) = self.peek() {
            self.advance();
            let inner = self.parse_primary()?;
            return Ok(CompiledCondition::Not(Box::new(inner)));
        }
        self.parse_primary()
    }

    fn parse_primary(&mut self) -> crate::Result<CompiledCondition> {
        if let Some(Token::LParen) = self.peek() {
            self.advance();
            let expr = self.parse_expression()?;
            if !matches!(self.peek(), Some(Token::RParen)) {
                return Err(crate::Error::Parse("Expected ')'".into()));
            }
            self.advance();
            return Ok(expr);
        }

        // field operator value
        let field = match self.advance() {
            Some(Token::Ident(s)) => s.clone(),
            other => {
                return Err(crate::Error::Parse(format!(
                    "Expected field name, got {:?}",
                    other
                )));
            }
        };

        // Handle attributes["key"] syntax
        let field = if let Some(Token::LBracket) = self.peek() {
            self.advance();
            let key = match self.advance() {
                Some(Token::Str(s)) => s.clone(),
                other => {
                    return Err(crate::Error::Parse(format!(
                        "Expected string key, got {:?}",
                        other
                    )));
                }
            };
            if !matches!(self.peek(), Some(Token::RBracket)) {
                return Err(crate::Error::Parse("Expected ']'".into()));
            }
            self.advance();
            format!("{}.{}", field, key)
        } else {
            field
        };

        let op = match self.advance() {
            Some(Token::Op(s)) => s.clone(),
            other => {
                return Err(crate::Error::Parse(format!(
                    "Expected operator, got {:?}",
                    other
                )));
            }
        };

        match op.as_str() {
            "exists" => Ok(CompiledCondition::Exists { field }),
            "=" => {
                let value = self.parse_value_string()?;
                Ok(CompiledCondition::Eq { field, value })
            }
            "!=" => {
                let value = self.parse_value_string()?;
                Ok(CompiledCondition::Neq { field, value })
            }
            ">" => {
                let value = self.parse_value_num()?;
                Ok(CompiledCondition::Gt { field, value })
            }
            ">=" => {
                let value = self.parse_value_num()?;
                Ok(CompiledCondition::Gte { field, value })
            }
            "<" => {
                let value = self.parse_value_num()?;
                Ok(CompiledCondition::Lt { field, value })
            }
            "<=" => {
                let value = self.parse_value_num()?;
                Ok(CompiledCondition::Lte { field, value })
            }
            "contains" => {
                let value = self.parse_value_string()?;
                Ok(CompiledCondition::Contains { field, value })
            }
            "matches" | "=~" => {
                let pattern = self.parse_value_string()?;
                let re = regex::Regex::new(&pattern)
                    .map_err(|e| crate::Error::Parse(format!("Invalid regex: {}", e)))?;
                Ok(CompiledCondition::Matches { field, re })
            }
            "between" => {
                let low = self.parse_value_num()?;
                // expect "and"
                if !matches!(self.peek(), Some(Token::And)) {
                    return Err(crate::Error::Parse(
                        "Expected 'and' in between expression".into(),
                    ));
                }
                self.advance();
                let high = self.parse_value_num()?;
                Ok(CompiledCondition::Between { field, low, high })
            }
            "in" => {
                if !matches!(self.peek(), Some(Token::LParen)) {
                    return Err(crate::Error::Parse("Expected '(' after 'in'".into()));
                }
                self.advance();
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_value_string()?);
                    match self.peek() {
                        Some(Token::Comma) => {
                            self.advance();
                        }
                        Some(Token::RParen) => {
                            self.advance();
                            break;
                        }
                        _ => {
                            return Err(crate::Error::Parse(
                                "Expected ',' or ')' in 'in' list".into(),
                            ))
                        }
                    }
                }
                Ok(CompiledCondition::In { field, values })
            }
            _ => Err(crate::Error::Parse(format!("Unknown operator: {}", op))),
        }
    }

    fn parse_value_string(&mut self) -> crate::Result<String> {
        match self.advance() {
            Some(Token::Str(s)) => Ok(s.clone()),
            Some(Token::Num(n)) => Ok(n.to_string()),
            Some(Token::Ident(s)) => Ok(s.clone()),
            Some(Token::Bool(b)) => Ok(b.to_string()),
            other => Err(crate::Error::Parse(format!(
                "Expected value, got {:?}",
                other
            ))),
        }
    }

    fn parse_value_num(&mut self) -> crate::Result<f64> {
        match self.advance() {
            Some(Token::Num(n)) => Ok(*n),
            Some(Token::Str(s)) => s
                .parse()
                .map_err(|_| crate::Error::Parse(format!("Expected number, got '{}'", s))),
            other => Err(crate::Error::Parse(format!(
                "Expected number, got {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests;
