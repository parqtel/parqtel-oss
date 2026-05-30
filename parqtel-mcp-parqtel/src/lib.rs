//! Parqtel self-MCP server library

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

/// Create a parqtel query metrics tool
pub fn make_query_metrics_tool() -> McpTool {
    McpTool {
        name: "query_metrics".to_string(),
        description: "Execute a Prometheus range query".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" },
                "start_ns": { "type": "number" },
                "end_ns": { "type": "number" },
                "step_secs": { "type": "number" }
            },
            "required": ["query", "start_ns", "end_ns", "step_secs"]
        }),
    }
}

/// Create a parqtel query logs tool
pub fn make_query_logs_tool() -> McpTool {
    McpTool {
        name: "query_logs".to_string(),
        description: "Query log records".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "filter": { "type": "string" },
                "start_ns": { "type": "number" },
                "end_ns": { "type": "number" },
                "limit": { "type": "number" },
                "severity_min": { "type": "string" }
            },
            "required": ["filter", "start_ns", "end_ns", "limit"]
        }),
    }
}

/// Create a parqtel get alert history tool
pub fn make_get_alert_history_tool() -> McpTool {
    McpTool {
        name: "get_alert_history".to_string(),
        description: "Return alert history for a service or pod".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "service_name": { "type": "string" },
                "pod_name": { "type": "string" },
                "since_hours": { "type": "number" }
            },
            "required": []
        }),
    }
}

/// Create a parqtel get topology tool
pub fn make_get_topology_tool() -> McpTool {
    McpTool {
        name: "get_topology".to_string(),
        description: "Return Kubernetes topology for a namespace".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "namespace": { "type": "string" }
            },
            "required": ["namespace"]
        }),
    }
}

/// Create a parqtel get noise statistics tool
pub fn make_get_noise_statistics_tool() -> McpTool {
    McpTool {
        name: "get_noise_statistics".to_string(),
        description: "Return noise scoring statistics for rules".to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "rule_id": { "type": "string" }
            },
            "required": []
        }),
    }
}
