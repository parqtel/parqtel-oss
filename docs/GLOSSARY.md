# Parqtel Glossary

This document defines key terms and concepts used within the Parqtel ecosystem.

### A
- **Alert Engine**: The subsystem responsible for evaluating YAML-based rules against ingested data and managing alert state transitions.
- **Axum**: The high-performance, asynchronous web framework used for Parqtel's HTTP server.

### B
- **Block**: A time-bounded collection of data (metrics or logs) stored as a single Parquet file.
- **Block Index**: An in-memory structure (with disk persistence) that tracks the metadata and time ranges of all Parquet blocks for fast querying.
- **Block Rotator**: The component that decides when to close a current memory buffer and write it to a new Parquet block on disk.

### C
- **Columnar Storage**: A database storage format where data is stored by column rather than row, enabling high compression and fast analytical queries.
- **Compaction**: The background process that merges multiple small Parquet blocks into larger ones to optimize storage efficiency and query performance.

### D
- **DQL (Data Query Language)**: Parqtel's internal expression language used for advanced log filtering and metric extraction.

### G
- **Grafana SimpleJSON**: A protocol used by Parqtel to serve data to Grafana dashboards without needing a complex Prometheus setup.

### I
- **Ingestion Service**: The entry point for all OTLP data, responsible for decoding, validation, and initial buffering.

### M
- **MCP (Model Context Protocol)**: An open standard that allows Parqtel to expose its data and tools directly to Large Language Models (LLMs) like Claude or GPT.

### O
- **OTLP (OpenTelemetry Protocol)**: The industry-standard protocol for transmitting metrics, logs, and traces. Parqtel supports both Protobuf and JSON variants.

### P
- **Parquet**: An open-source, column-oriented data file format designed for efficient data storage and retrieval.
- **Pipeline**: A set of stages (parsing, filtering, extraction) that data passes through during ingestion.
- **PromQL**: The Prometheus Query Language, supported by Parqtel for metric retrieval.

### R
- **Recording Rule**: A pre-calculated query that saves the result as a new metric series, reducing query-time overhead for complex dashboards.
- **Retention Policy**: The logic that determines when old data blocks should be deleted from the filesystem based on age.

### W
- **WAL (Write-Ahead Log)**: A crash-recovery mechanism that records incoming data to a persistent log before it is flushed to a Parquet block.

### Z
- **Zstd**: A high-performance compression algorithm used by Parqtel to reduce disk usage by up to 90%.
