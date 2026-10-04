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
pub mod telemetry;
pub mod wal;

pub use buffer::MemoryBuffer;
pub use config::{
    compression_from_name, BlockConfig, Config, LogBlockConfig, RetentionConfig, ServerConfig,
    TailSamplingConfig,
};
pub use engine::StorageEngine;
pub use error::{Error, Result};
pub use models::*;
pub use storage::{
    start_maintenance, BlockIndex, BlockIndexStore, MaintenanceHandle, RetentionPolicy, Scanner,
};
pub use telemetry::{ContentionMetrics, FlushGuard, Histogram};
