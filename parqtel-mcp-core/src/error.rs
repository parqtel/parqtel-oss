//! Error types for MCP operations

use thiserror::Error;

/// MCP-specific error codes following the JSON-RPC 2.0 spec
/// Application-level errors start at -32000
pub const ERROR_PARSE_ERROR: i64 = -32700;
pub const ERROR_INVALID_REQUEST: i64 = -32600;
pub const ERROR_METHOD_NOT_FOUND: i64 = -32601;
pub const ERROR_INTERNAL_ERROR: i64 = -32603;
pub const ERROR_APPLICATION_START: i64 = -32000;

/// Result type for MCP operations
pub type McpResult<T> = Result<T, McpError>;

/// Error type for MCP operations
#[derive(Error, Debug)]
pub enum McpError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Invalid request: {0}")]
    InvalidRequest(String),

    #[error("Method not found: {0}")]
    MethodNotFound(String),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[error("Application error: {0}")]
    ApplicationError(String),

    #[error("HTTP error: {0}")]
    HttpError(#[from] axum::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
}

impl McpError {
    /// Convert error to JSON-RPC error response
    pub fn to_json_rpc_error(&self) -> serde_json::Value {
        serde_json::json!({
            "code": self.to_code(),
            "message": self.to_string(),
            "data": self.to_data()
        })
    }

    /// Get the JSON-RPC error code
    pub fn to_code(&self) -> i64 {
        match self {
            McpError::ParseError(_) => ERROR_PARSE_ERROR,
            McpError::InvalidRequest(_) => ERROR_INVALID_REQUEST,
            McpError::MethodNotFound(_) => ERROR_METHOD_NOT_FOUND,
            McpError::InternalError(_) => ERROR_INTERNAL_ERROR,
            McpError::ApplicationError(_) => ERROR_APPLICATION_START,
            McpError::HttpError(_) => ERROR_INTERNAL_ERROR,
            McpError::SerializationError(_) => ERROR_INTERNAL_ERROR,
            McpError::IoError(_) => ERROR_INTERNAL_ERROR,
        }
    }

    /// Get additional error data
    pub fn to_data(&self) -> Option<serde_json::Value> {
        match self {
            McpError::ParseError(msg) => Some(serde_json::json!({"details": msg})),
            McpError::InvalidRequest(msg) => Some(serde_json::json!({"details": msg})),
            McpError::MethodNotFound(msg) => Some(serde_json::json!({"details": msg})),
            McpError::InternalError(msg) => Some(serde_json::json!({"details": msg})),
            McpError::ApplicationError(msg) => Some(serde_json::json!({"details": msg})),
            _ => None,
        }
    }
}
