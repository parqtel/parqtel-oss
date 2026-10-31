//! Recording rule engine and stream pipeline processor for parqtel.

pub mod config;
pub mod expr;
pub mod pipeline;
pub mod rule;
pub mod ruler;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("validation error: {0}")]
    Validation(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("storage error: {0}")]
    Storage(String),
    #[error("expression error: {0}")]
    Expression(String),
}

pub type Result<T> = std::result::Result<T, Error>;
