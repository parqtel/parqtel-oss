//! Parqtel self-MCP server library
//!
//! Tool definitions for AI agents to query a live Parqtel instance.
//! Schemas use epoch **seconds** (the API contract); `start_ns`/`end_ns`
//! from the legacy schema are still accepted and converted.

pub mod client;
pub mod handlers;

use serde_json::json;

use parqtel_mcp_core::tool::McpTool;

pub use client::ParqtelClient;

/// Create a parqtel query metrics tool
///
/// Three modes:
/// - **instant** — `query` only: evaluated at "now"
/// - **range** — `query` + `start` + `end` (epoch seconds) + optional `step_secs`
/// - **window** — `query` + `window_secs`: range over the trailing N seconds
pub fn make_query_metrics_tool() -> McpTool {
    McpTool {
        name: "query_metrics".to_string(),
        description: "Query Parqtel metrics with a PromQL expression. Instant evaluation by \
                      default; pass start+end (epoch seconds) or window_secs for a range query \
                      with per-bucket volume and series totals."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "PromQL expression, e.g. http_requests_total or \
                                    sum(rate(http_requests_total[5m])) by (service.name)"
                },
                "start": { "type": "number", "description": "Range start, epoch seconds" },
                "end": { "type": "number", "description": "Range end, epoch seconds" },
                "step_secs": {
                    "type": "number",
                    "description": "Range step interval in seconds (default 60, range mode only)"
                },
                "window_secs": {
                    "type": "number",
                    "description": "Convenience relative range over the trailing N seconds \
                                    (used when start/end are omitted)"
                },
                "start_ns": {
                    "type": "number",
                    "description": "Legacy alias: range start in nanoseconds (converted to seconds)"
                },
                "end_ns": {
                    "type": "number",
                    "description": "Legacy alias: range end in nanoseconds (converted to seconds)"
                }
            },
            "required": ["query"]
        }),
    }
}

/// Create a parqtel label discovery tool
pub fn make_query_metrics_labels_tool() -> McpTool {
    McpTool {
        name: "query_metrics_labels".to_string(),
        description: "Discover metric/label metadata: list all indexed label names, or the \
                      indexed values of one label (e.g. __name__ for metric names, \
                      service_name for services). Use before query_metrics to build selectors."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "label": {
                    "type": "string",
                    "description": "Label name, restricted to [a-zA-Z0-9_] \
                                    (e.g. __name__, service_name, host, method). \
                                    Omit to list all label names."
                }
            },
            "required": []
        }),
    }
}

/// Create a parqtel query logs tool
pub fn make_query_logs_tool() -> McpTool {
    McpTool {
        name: "query_logs".to_string(),
        description: "Search stored log records. Defaults to the last hour, newest first, \
                      capped at 2000 records. Returns matched records plus the true match \
                      count and a 60-bucket volume summary over the range."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Label selector expression, e.g. {} (all) or \
                                    {service_name=\"api\"}. Default: {}"
                },
                "start": { "type": "number", "description": "Range start, epoch seconds (default: now - 1h)" },
                "end": { "type": "number", "description": "Range end, epoch seconds (default: now)" },
                "limit": {
                    "type": "number",
                    "description": "Maximum records returned, capped at 2000 (default 100)"
                },
                "order": {
                    "type": "string",
                    "enum": ["asc", "desc"],
                    "description": "Result order by timestamp (default desc = newest first)"
                },
                "severity_min": {
                    "type": "string",
                    "description": "Minimum severity to include: TRACE, DEBUG, INFO, WARN, ERROR or FATAL (a numeric OTLP severity 1-24 also works)"
                }
            },
            "required": []
        }),
    }
}

/// Create a parqtel ingest rates tool
pub fn make_ingest_rates_tool() -> McpTool {
    McpTool {
        name: "ingest_rates".to_string(),
        description: "Live per-signal ingestion rates (metrics/logs/traces): current + 60s/5m/15m \
                      averages, wire bytes/sec, seconds since the last ingested item (gap), \
                      lifetime totals, and per-second event counts for the trailing window. \
                      Use to detect ingestion spikes, stalls or gaps."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "history_secs": {
                    "type": "number",
                    "description": "Length of the per-second history arrays, 1..=900 \
                                    (default 180; the server keeps a 15-minute wheel)"
                }
            },
            "required": []
        }),
    }
}

/// Create a parqtel get alert history tool
pub fn make_get_alert_history_tool() -> McpTool {
    McpTool {
        name: "get_alert_history".to_string(),
        description: "Return alert instances fired within a lookback window, optionally \
                      filtered by service or pod. Includes state, severity, labels, \
                      annotations and the noise score per alert."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "service_name": {
                    "type": "string",
                    "description": "Only alerts whose labels carry this service \
                                    (service.name / service_name / service)"
                },
                "pod_name": {
                    "type": "string",
                    "description": "Only alerts whose labels carry this pod \
                                    (pod / pod_name / k8s.pod.name)"
                },
                "since_hours": {
                    "type": "number",
                    "description": "Lookback window in hours (default 24)"
                }
            },
            "required": []
        }),
    }
}

/// Create a parqtel get topology tool
pub fn make_get_topology_tool() -> McpTool {
    McpTool {
        name: "get_topology".to_string(),
        description: "Snapshot the observed service landscape: indexed service names, storage \
                      footprint per signal (blocks/rows/bytes) and in-memory buffer counts. \
                      Gives an agent the lay of the land before diving into metrics/logs."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "namespace": {
                    "type": "string",
                    "description": "Optional filter: only services whose name contains \
                                    this string"
                }
            },
            "required": []
        }),
    }
}

/// Create a parqtel get noise statistics tool
pub fn make_get_noise_statistics_tool() -> McpTool {
    McpTool {
        name: "get_noise_statistics".to_string(),
        description: "Per-rule alert noise statistics: total/firing/resolved alert counts and \
                      noise-score aggregates from the live alert stream. Use to spot noisy \
                      rules worth tuning or silencing."
            .to_string(),
        input_schema: json!({
            "type": "object",
            "properties": {
                "rule_id": {
                    "type": "string",
                    "description": "Restrict statistics to one rule (omit for all rules)"
                }
            },
            "required": []
        }),
    }
}
