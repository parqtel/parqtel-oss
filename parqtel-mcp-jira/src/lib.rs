//! Jira MCP server library

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Create a Jira create incident issue tool
pub fn make_create_incident_issue_tool() -> McpTool {
    McpTool {
        name: "create_incident_issue".to_string(),
        description: "Create a Jira issue for an incident".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "project_key": { "type": "string" },
                "summary": { "type": "string" },
                "description": { "type": "string" },
                "priority": { "type": "string" },
                "labels": { "type": "array", "items": { "type": "string" } },
                "alert_id": { "type": "string" }
            },
            "required": ["project_key", "summary", "description", "priority", "labels", "alert_id"]
        }),
    }
}

/// Create a Jira add RCA comment tool
pub fn make_add_rca_comment_tool() -> McpTool {
    McpTool {
        name: "add_rca_comment".to_string(),
        description: "Add RCA findings as a formatted Jira comment".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "issue_key": { "type": "string" },
                "root_cause": { "type": "string" },
                "evidence": { "type": "array", "items": { "type": "string" } },
                "recommended_actions": { "type": "array", "items": { "type": "string" } }
            },
            "required": ["issue_key", "root_cause", "evidence", "recommended_actions"]
        }),
    }
}

/// Create a Jira create action item tool
pub fn make_create_action_item_tool() -> McpTool {
    McpTool {
        name: "create_action_item".to_string(),
        description: "Create a child issue for a postmortem action item".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "parent_issue_key": { "type": "string" },
                "summary": { "type": "string" },
                "assignee_email": { "type": "string" },
                "due_date": { "type": "string" }
            },
            "required": ["parent_issue_key", "summary"]
        }),
    }
}

/// Create a Jira transition issue tool
pub fn make_transition_issue_tool() -> McpTool {
    McpTool {
        name: "transition_issue".to_string(),
        description: "Move an issue to a new status".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "issue_key": { "type": "string" },
                "transition_name": { "type": "string" }
            },
            "required": ["issue_key", "transition_name"]
        }),
    }
}
