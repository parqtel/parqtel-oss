//! Tool definitions for MCP

use serde_json::Value;

/// Represents a tool that can be called via MCP
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// Sanitize input parameters for audit logging
/// Removes sensitive fields like tokens, passwords, keys
pub fn sanitize_params(params: &Value) -> Value {
    if !params.is_object() {
        return params.clone();
    }

    let sensitive_keys = [
        "token",
        "api_key",
        "apikey",
        "password",
        "secret",
        "credential",
        "key",
    ];

    let obj = params.as_object().unwrap();
    let mut sanitized = serde_json::Map::new();

    for (key, value) in obj {
        let lower_key = key.to_lowercase();
        let is_sensitive = sensitive_keys.iter().any(|k| lower_key.contains(k));

        if is_sensitive {
            sanitized.insert(key.clone(), serde_json::json!("***REDACTED***"));
        } else if value.is_object() || value.is_array() {
            sanitized.insert(key.clone(), sanitize_params(value));
        } else {
            sanitized.insert(key.clone(), value.clone());
        }
    }

    Value::Object(sanitized)
}
