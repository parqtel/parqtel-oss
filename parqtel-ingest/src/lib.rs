//! Ingestion pipeline for parqtel.
//!
//! Handles OTLP decoding, validation, and crash-safe Parquet block writing for metrics, logs, and traces.

pub mod decode;
pub mod service;
pub mod writer;

// Generated proto modules
pub mod otel {
    #![allow(clippy::all)]
    pub mod common {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.common.v1.rs"));
        }
    }
    pub mod resource {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.resource.v1.rs"));
        }
    }
    pub mod metrics {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.metrics.v1.rs"));
        }
    }
    pub mod logs {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.logs.v1.rs"));
        }
    }
    pub mod trace {
        pub mod v1 {
            include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.trace.v1.rs"));
        }
    }
    pub mod collector {
        pub mod metrics {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.collector.metrics.v1.rs"));
            }
        }
        pub mod logs {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.collector.logs.v1.rs"));
            }
        }
        pub mod trace {
            pub mod v1 {
                include!(concat!(env!("OUT_DIR"), "/opentelemetry.proto.collector.trace.v1.rs"));
            }
        }
    }
}

pub use service::{IngestionService, LogIngestionService, TraceIngestionService};
pub use writer::{BlockMetadata, BlockWriter, LogWriter, TraceWriter};
