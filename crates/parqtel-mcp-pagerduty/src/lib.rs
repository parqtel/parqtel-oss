//! PagerDuty MCP server library

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Create a PagerDuty create incident tool
pub fn make_create_incident_tool() -> McpTool {
    McpTool {
        name: "create_incident".to_string(),
        description: "Create a PagerDuty incident".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "title": { "type": "string" },
                "severity": { "type": "string" },
                "body": { "type": "string" },
                "service_id": { "type": "string" },
                "alert_id": { "type": "string" },
                "routing_key": { "type": "string" }
            },
            "required": ["title", "severity", "body", "service_id", "alert_id"]
        }),
    }
}

/// Create a PagerDuty add note tool
pub fn make_add_note_tool() -> McpTool {
    McpTool {
        name: "add_note".to_string(),
        description: "Add a note to an existing PagerDuty incident".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "incident_id": { "type": "string" },
                "note": { "type": "string" }
            },
            "required": ["incident_id", "note"]
        }),
    }
}

/// Create a PagerDuty resolve incident tool
pub fn make_resolve_incident_tool() -> McpTool {
    McpTool {
        name: "resolve_incident".to_string(),
        description: "Resolve a PagerDuty incident".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "incident_id": { "type": "string" },
                "resolution_note": { "type": "string" }
            },
            "required": ["incident_id", "resolution_note"]
        }),
    }
}

/// Create a PagerDuty get on-call tool
pub fn make_get_oncall_tool() -> McpTool {
    McpTool {
        name: "get_oncall".to_string(),
        description: "Return the current on-call engineer for a service".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "service_id": { "type": "string" }
            },
            "required": ["service_id"]
        }),
    }
}

/// Create a PagerDuty get recent incidents tool
pub fn make_get_recent_incidents_tool() -> McpTool {
    McpTool {
        name: "get_recent_incidents".to_string(),
        description: "Return incidents for a service in the last N hours".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "service_id": { "type": "string" },
                "hours": { "type": "number" }
            },
            "required": ["service_id", "hours"]
        }),
    }
}
