//! Core shared types and storage logic for parqtel.
//!
//! This crate contains the foundational data models, configuration types,
//! and storage schema definitions used across the parqtel workspace.

pub mod buffer;
pub mod config;
pub mod engine;
pub mod error;
pub mod models;
pub mod storage;

pub use buffer::MemoryBuffer;
pub use config::{BlockConfig, Config, LogBlockConfig, RetentionConfig, ServerConfig};
pub use engine::StorageEngine;
pub use error::{Error, Result};
pub use models::*;
pub use storage::{start_maintenance, BlockIndex, Scanner};
