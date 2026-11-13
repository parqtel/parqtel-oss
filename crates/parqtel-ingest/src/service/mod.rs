//! Ingestion services: decode a request, buffer it in memory, write it to the WAL and
//! rotate it into Parquet blocks.
//!
//! - `rotator` shards metric writers and flushes them into one block.
//! - `serial` is the single-writer rotator shared by logs and traces.
//! - `flush` orders flush completion: index durability, WAL commit, buffer drops.
//! - `metrics`, `logs` and `traces` are the per-signal services.

mod common;
mod flush;
mod logs;
mod metrics;
mod rotator;
mod serial;
mod traces;

pub use common::IngestionStats;
pub use logs::LogIngestionService;
pub use metrics::IngestionService;
pub use rotator::BlockRotator;
pub use serial::{LogRotator, RowWriter, SerialRotator, TraceRotator};
pub use traces::TraceIngestionService;

#[cfg(test)]
mod tests;
