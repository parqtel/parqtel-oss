//! Google Docs MCP server library

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Create a Google Docs create postmortem doc tool
pub fn make_create_postmortem_doc_tool() -> McpTool {
    McpTool {
        name: "create_postmortem_doc".to_string(),
        description: "Create a Google Doc from the postmortem template".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "folder_id": { "type": "string" },
                "title": { "type": "string" },
                "content_markdown": { "type": "string" },
                "action_items": { "type": "array", "items": { "type": "object" } }
            },
            "required": ["folder_id", "title", "content_markdown", "action_items"]
        }),
    }
}

/// Create a Google Docs append timeline tool
pub fn make_append_timeline_tool() -> McpTool {
    McpTool {
        name: "append_timeline".to_string(),
        description: "Append a timeline section to an existing doc".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "doc_id": { "type": "string" },
                "timeline": { "type": "array", "items": { "type": "object" } }
            },
            "required": ["doc_id", "timeline"]
        }),
    }
}

/// Create a Google Docs share document tool
pub fn make_share_document_tool() -> McpTool {
    McpTool {
        name: "share_document".to_string(),
        description: "Share the document with specific emails".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "doc_id": { "type": "string" },
                "emails": { "type": "array", "items": { "type": "string" } },
                "role": { "type": "string" }
            },
            "required": ["doc_id", "emails", "role"]
        }),
    }
}
