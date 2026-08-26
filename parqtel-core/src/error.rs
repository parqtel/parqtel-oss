use thiserror::Error;

/// Unified error type for the parqtel project.
#[derive(Debug, Error)]
pub enum Error {
    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Parquet encoding or decoding error.
    #[error("Parquet error: {0}")]
    Parquet(String),

    /// Arrow memory or schema error.
    #[error("Arrow error: {0}")]
    Arrow(String),

    /// Schema mismatch error.
    #[error("Schema mismatch: expected {expected}, found {found}")]
    SchemaMismatch {
        /// Expected schema description.
        expected: String,
        /// Found schema description.
        found: String,
    },

    /// Invalid OTLP payload.
    #[error("Invalid OTLP payload: {0}")]
    InvalidOtlp(String),

    /// Query execution error.
    #[error("Query error: {0}")]
    Query(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Validation error.
    #[error("Validation error: {0}")]
    Validation(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),

    /// Serialization/Deserialization error.
    #[error("Serde error: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Result type alias using the parqtel [Error].
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;

    #[test]
    fn test_error_variants() {
        let io_err = Error::Io(std::io::Error::other("io"));
        assert!(io_err.to_string().contains("I/O error: io"));

        let parquet_err = Error::Parquet("parquet".into());
        assert!(parquet_err.to_string().contains("Parquet error: parquet"));

        let arrow_err = Error::Arrow("arrow".into());
        assert!(arrow_err.to_string().contains("Arrow error: arrow"));

        let schema_err = Error::SchemaMismatch {
            expected: "e".into(),
            found: "f".into(),
        };
        assert!(schema_err
            .to_string()
            .contains("Schema mismatch: expected e, found f"));

        let otlp_err = Error::InvalidOtlp("otlp".into());
        assert!(otlp_err.to_string().contains("Invalid OTLP payload: otlp"));

        let query_err = Error::Query("query".into());
        assert!(query_err.to_string().contains("Query error: query"));

        let config_err = Error::Config("config".into());
        assert!(config_err
            .to_string()
            .contains("Configuration error: config"));

        let validation_err = Error::Validation("validation".into());
        assert!(validation_err
            .to_string()
            .contains("Validation error: validation"));

        let internal_err = Error::Internal("internal".into());
        assert!(internal_err
            .to_string()
            .contains("Internal error: internal"));

        let serde_err = Error::Serde(serde_json::from_str::<serde_json::Value>("{").unwrap_err());
        assert!(serde_err.to_string().contains("Serde error:"));
    }
}
