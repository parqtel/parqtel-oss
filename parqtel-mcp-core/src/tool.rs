//! Tool definitions for MCP

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use super::error::McpError;

/// Pinned, boxed future returned by a tool handler.
pub type BoxToolFuture = Pin<Box<dyn Future<Output = Result<Value, McpError>> + Send>>;

/// Asynchronous tool executor. Receives the sanitized tool params and
/// returns the tool result payload. Handlers capture their own context
/// (HTTP clients, base URLs, credentials) at registration time, so the
/// server itself stays transport-agnostic.
pub type ToolHandler = Arc<dyn Fn(Value) -> BoxToolFuture + Send + Sync>;

/// Represents a tool that can be called via MCP
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
}

/// Sanitise a value of any shape, recursing through objects *and* arrays.
///
/// This is the only correct entry point for a nested value. `sanitize_params`
/// handles a single object; handing it an array returns it unchanged, because
/// its own guard rejects non-objects. Recursing into an array with
/// `sanitize_params` therefore looked like it worked and silently did nothing —
/// a credential inside `[{...}]` reached the audit log in the clear.
fn sanitize_value(value: &Value) -> Value {
    match value {
        Value::Object(_) => sanitize_params(value),
        Value::Array(items) => Value::Array(items.iter().map(sanitize_value).collect()),
        other => other.clone(),
    }
}

/// Sanitize input parameters for audit logging
/// Removes sensitive fields like tokens, passwords, keys
pub fn sanitize_params(params: &Value) -> Value {
    if !params.is_object() {
        return params.clone();
    }

    // Matched as a substring of the lowercased key, so `api_key`, `X-Api-Key`
    // and `db_password` are all covered.
    let sensitive_keys = [
        "token",
        "api_key",
        "apikey",
        "password",
        "secret",
        "credential",
        "key",
        // The usual carrier for a bearer token, and previously not covered.
        "authorization",
        "auth",
        "session",
        "cookie",
        "private",
    ];

    let obj = params.as_object().expect("guarded above by is_object()");
    let mut sanitized = serde_json::Map::new();

    for (key, value) in obj {
        let lower_key = key.to_lowercase();
        let is_sensitive = sensitive_keys.iter().any(|k| lower_key.contains(k));

        if is_sensitive {
            sanitized.insert(key.clone(), serde_json::json!("***REDACTED***"));
        } else {
            sanitized.insert(key.clone(), sanitize_value(value));
        }
    }

    Value::Object(sanitized)
}

#[cfg(test)]
mod sanitize_regression {
    use super::*;

    /// A malformed client can send `params` as an array or a scalar. The audit
    /// sanitiser runs on that value, so it must not panic: the MCP server
    /// handles untrusted input.
    #[test]
    fn sanitize_params_handles_non_object_input() {
        for bad in [
            serde_json::json!([1, 2, 3]),
            serde_json::json!("a string"),
            serde_json::json!(42),
            serde_json::json!(null),
        ] {
            let out = sanitize_params(&bad);
            assert!(out.is_null() || out.is_array() || out.is_string() || out.is_number());
        }
    }

    #[test]
    fn sanitize_params_redacts_nested_secrets() {
        let params = serde_json::json!({
            "query": "x",
            "api_key": "secret-value",
            "nested": { "password": "hunter2", "ok": 1 },
            "list": [{ "token": "abc" }]
        });
        let out = sanitize_params(&params);
        assert_eq!(out["api_key"], "***REDACTED***");
        assert_eq!(out["nested"]["password"], "***REDACTED***");
        assert_eq!(out["nested"]["ok"], 1);
        assert_eq!(out["query"], "x", "non-sensitive fields survive");
    }

    /// Regression: a credential inside an array must be redacted too.
    ///
    /// The array branch used to recurse into `sanitize_params`, which returns
    /// non-objects unchanged — so `[{ "token": "abc" }]` was logged verbatim.
    #[test]
    fn sanitize_params_redacts_secrets_nested_in_arrays() {
        let params = serde_json::json!({
            "headers": [{ "authorization": "Bearer secret" }, { "accept": "json" }],
            "creds": [{ "password": "hunter2" }],
            "deep": [[{ "api_key": "k" }]],
            "flat": ["not", "an", "object"]
        });
        let out = sanitize_params(&params);
        assert_eq!(out["headers"][0]["authorization"], "***REDACTED***");
        assert_eq!(
            out["headers"][1]["accept"], "json",
            "non-sensitive siblings survive"
        );
        assert_eq!(out["creds"][0]["password"], "***REDACTED***");
        assert_eq!(out["deep"][0][0]["api_key"], "***REDACTED***");
        assert_eq!(out["flat"], serde_json::json!(["not", "an", "object"]));
    }
}
