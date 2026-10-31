//! Notion MCP server library

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Create a Notion create incident page tool
pub fn make_create_incident_page_tool() -> McpTool {
    McpTool {
        name: "create_incident_page".to_string(),
        description: "Create an incident page from a template".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "database_id": { "type": "string" },
                "title": { "type": "string" },
                "severity": { "type": "string" },
                "affected_services": { "type": "array", "items": { "type": "string" } },
                "alert_id": { "type": "string" },
                "started_at": { "type": "string" }
            },
            "required": ["database_id", "title", "severity", "affected_services", "alert_id", "started_at"]
        }),
    }
}

/// Create a Notion update incident status tool
pub fn make_update_incident_status_tool() -> McpTool {
    McpTool {
        name: "update_incident_status".to_string(),
        description: "Update the status property on an incident page".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "page_id": { "type": "string" },
                "status": { "type": "string" }
            },
            "required": ["page_id", "status"]
        }),
    }
}

/// Create a Notion append RCA section tool
pub fn make_append_rca_section_tool() -> McpTool {
    McpTool {
        name: "append_rca_section".to_string(),
        description: "Append a root cause analysis section to an existing page".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "page_id": { "type": "string" },
                "root_cause": { "type": "string" },
                "timeline": { "type": "array", "items": { "type": "object" } },
                "recommended_actions": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["page_id", "root_cause", "timeline", "recommended_actions"]
        }),
    }
}

/// Create a Notion create postmortem page tool
pub fn make_create_postmortem_page_tool() -> McpTool {
    McpTool {
        name: "create_postmortem_page".to_string(),
        description: "Create a full postmortem document from the AI-drafted content".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "database_id": { "type": "string" },
                "incident_title": { "type": "string" },
                "postmortem_markdown": { "type": "string" },
                "action_items": { "type": "array", "items": { "type": "object" } }
            },
            "required": ["database_id", "incident_title", "postmortem_markdown", "action_items"]
        }),
    }
}
