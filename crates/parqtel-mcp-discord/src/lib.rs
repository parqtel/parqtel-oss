//! Discord MCP server library

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Create a Discord send alert embed tool
pub fn make_send_alert_embed_tool() -> McpTool {
    McpTool {
        name: "send_alert_embed".to_string(),
        description: "Post an embed message to a Discord channel".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "channel_id": { "type": "string" },
                "title": { "type": "string" },
                "description": { "type": "string" },
                "severity": { "type": "string" },
                "fields": { "type": "array", "items": { "type": "object" } },
                "alert_id": { "type": "string" }
            },
            "required": ["channel_id", "title", "description", "severity", "alert_id"]
        }),
    }
}

/// Create a Discord send RCA update tool
pub fn make_send_rca_update_tool() -> McpTool {
    McpTool {
        name: "send_rca_update".to_string(),
        description: "Post an RCA update to a thread".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "thread_id": { "type": "string" },
                "root_cause": { "type": "string" },
                "confidence": { "type": "number" },
                "actions": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["thread_id", "root_cause", "confidence", "actions"]
        }),
    }
}

/// Create a Discord resolve alert tool
pub fn make_resolve_alert_tool() -> McpTool {
    McpTool {
        name: "resolve_alert".to_string(),
        description: "Update the original embed with resolved status".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "message_id": { "type": "string" },
                "channel_id": { "type": "string" },
                "resolution_summary": { "type": "string" }
            },
            "required": ["message_id", "channel_id", "resolution_summary"]
        }),
    }
}
