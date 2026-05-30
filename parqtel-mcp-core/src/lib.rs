//! Core infrastructure for parqtel MCP servers
//!
//! This library provides the common infrastructure shared by all MCP servers:
//! - JSON-RPC 2.0 protocol handling
//! - Tool registration and dispatch
//! - Rate limiting
//! - Audit logging
//! - Health check endpoint

pub mod error;
pub mod server;
pub mod tool;

pub use error::{McpError, McpResult};
pub use server::{McpServer, ServerConfig};
pub use tool::{McpTool, sanitize_params};
