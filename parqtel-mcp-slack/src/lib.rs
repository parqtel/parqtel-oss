//! Slack MCP server library
//!
//! This library provides the Slack integration tools for the MCP server.

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Severity colors for Slack Block Kit messages
pub fn severity_color(severity: &str) -> &'static str {
    match severity.to_lowercase().as_str() {
        "critical" => "#ff0000",
        "warning" | "warn" => "#ffa500",
        "info" | "information" => "#008080",
        _ => "#808080",
    }
}

/// Create a Slack alert message tool
pub fn make_send_alert_message_tool() -> McpTool {
    McpTool {
        name: "send_alert_message".to_string(),
        description: "Post a formatted incident alert to a Slack channel".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "channel": { "type": "string", "description": "Channel to post to" },
                "severity": { "type": "string", "description": "Alert severity level" },
                "title": { "type": "string", "description": "Alert title" },
                "summary": { "type": "string", "description": "Alert summary" },
                "runbook_url": { "type": "string", "description": "Optional runbook URL" },
                "alert_id": { "type": "string", "description": "Unique alert identifier" }
            },
            "required": ["channel", "severity", "title", "summary", "alert_id"]
        }),
    }
}

/// Create a Slack RCA update tool
pub fn make_send_rca_update_tool() -> McpTool {
    McpTool {
        name: "send_rca_update".to_string(),
        description: "Post a root cause analysis update as a thread reply".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "channel": { "type": "string" },
                "thread_ts": { "type": "string" },
                "primary_cause": { "type": "string" },
                "confidence": { "type": "number" },
                "evidence_summary": { "type": "string" },
                "recommended_actions": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["channel", "thread_ts", "primary_cause", "confidence", "evidence_summary", "recommended_actions"]
        }),
    }
}

/// Create a Slack resolve notification tool
pub fn make_resolve_notification_tool() -> McpTool {
    McpTool {
        name: "resolve_notification".to_string(),
        description: "Post a resolution notification to the original thread".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "channel": { "type": "string" },
                "thread_ts": { "type": "string" },
                "resolution_summary": { "type": "string" },
                "duration_minutes": { "type": "number" }
            },
            "required": ["channel", "thread_ts", "resolution_summary", "duration_minutes"]
        }),
    }
}

/// Create a Slack create incident channel tool
pub fn make_create_incident_channel_tool() -> McpTool {
    McpTool {
        name: "create_incident_channel".to_string(),
        description: "Create a dedicated incident channel for major incidents".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "incident_id": { "type": "string" },
                "severity": { "type": "string" },
                "affected_services": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["incident_id", "severity", "affected_services"]
        }),
    }
}
