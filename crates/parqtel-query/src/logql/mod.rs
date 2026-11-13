//! ParqtelQL — unified lenient search grammar for logs and traces.
//!
//! Design (modeled on ClickStack/Elasticsearch
//! `simple_query_string` guidance):
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
//!
//! Modules:
//! - `ast` holds the parsed query types.
//! - `tokenize` and `parse` turn a query string into those types.
//! - `prepared` compiles a predicate once so it can be applied to many rows.
//! - `logs` and `spans` match prepared queries against log records and spans.

mod ast;
mod logs;
mod parse;
mod prepared;
mod spans;
mod tokenize;

pub use ast::{Atom, Clause, CmpOp, Predicate, SearchQuery, SearchTerm};
pub use logs::{log_matches, log_matches_predicate, PreparedLogQuery};
pub use parse::{parse_predicate, parse_search, severity_rank};
pub use prepared::{contains_ci, ColumnPrefilter, PreparedPredicate};
pub use spans::{span_field, span_matches, PreparedSpanQuery};

#[cfg(test)]
mod tests;
