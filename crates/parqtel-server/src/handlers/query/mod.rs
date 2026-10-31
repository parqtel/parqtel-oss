//! Query API handlers.
//!
//! - [`metrics`] serves the Prometheus-compatible instant and range queries.
//! - [`labels`] lists metric names, label names and label values.
//! - [`logs`] serves log search, counts and field discovery.
//! - [`traces`] serves trace search and cross-signal correlation.
//! - [`searches`] stores saved searches.
//! - [`pipeline_search`] runs pipeline queries across signals.
//! - [`common`] holds the response envelope and the shared parsing helpers.

mod common;
mod labels;
mod logs;
mod metrics;
mod pipeline_search;
mod searches;
mod traces;

pub use labels::{list_label_names, list_label_values, list_metric_names};
pub use logs::{list_log_field_values, list_log_fields, query_logs, query_logs_count};
pub use metrics::{query_instant, query_range};
pub use pipeline_search::run_pipeline_search;
pub use searches::{create_saved_search, delete_saved_search, list_saved_searches};
pub use traces::{correlate, search_traces};
