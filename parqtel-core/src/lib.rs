//! Core shared types and storage logic for parqtel.
//!
//! This crate contains the foundational data models, configuration types,
//! and storage schema definitions used across the parqtel workspace.

pub mod config;
pub mod engine;
pub mod error;
pub mod models;
pub mod storage;

pub use config::{BlockConfig, LogBlockConfig, RetentionConfig, ServerConfig, Config};
pub use engine::StorageEngine;
pub use error::{Error, Result};
pub use models::*;
pub use storage::{BlockIndex, Scanner, start_maintenance};
