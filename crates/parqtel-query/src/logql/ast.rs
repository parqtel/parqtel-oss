//! Parsed query types.

/// A parsed ParqtelQL query: conjunction of clauses.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchQuery {
    /// Field equality/regex/range clauses — ALL must match (AND).
    pub clauses: Vec<Clause>,
    /// Free-text terms for body search (must all appear, case-insensitive).
    pub terms: Vec<SearchTerm>,
}

/// Boolean predicate tree OR of ANDs of atoms.
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
    /// `NOT <clause>` — the inner clause must NOT match. Keeps
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
