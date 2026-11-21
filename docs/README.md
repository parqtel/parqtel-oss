# Documentation

Documentation for Parqtel, an open-source SRE telemetry store.

**Start here:** [Getting started](getting-started/getting-started.md) ·
[Configuration](operations/configuration.md) ·
[Architecture](development/architecture.md) ·
[Object storage](operations/object-storage.md)

## Getting started

| | |
|---|---|
| [Getting started](getting-started/getting-started.md) | First 15 minutes: install, ingest, query |
| [Tutorials](getting-started/tutorials.md) | Guided walk-throughs of common tasks |
| [FAQ](getting-started/faq.md) | Short answers to common questions |

## Guides

| | |
|---|---|
| [Ingestion on-ramps](guides/ingestion.md) | OTLP, generic JSON, remote_write, Kafka, pipelines, limits |
| [PQL guide](guides/pql-guide.md) | ParQL metrics, ParqtelQL log/trace search, pipelines |
| [Query functions](guides/query-functions.md) | Supported function surface and gaps |
| [Alerting](operations/alerting.md) | Evaluation, routing, grouping, inhibition, receivers |
| [MCP](guides/mcp.md) | Model Context Protocol servers and tool schemas |
| [Best practices](guides/best-practices.md) | Sizing and operating guidance |
| [Argo Rollouts](guides/argo-rollouts.md) | Progressive delivery on Kubernetes |
| [Agent on-ramps](../examples/agents/README.md) | Collector, Vector, Fluent Bit and Filebeat configurations |

## Operations

| | |
|---|---|
| [Configuration](operations/configuration.md) | Every knob, environment variable and TOML key |
| [Deployment](operations/deployment.md) | Docker, Kubernetes, Helm, systemd |
| [Object storage](operations/object-storage.md) | S3-compatible bucket, shared catalog, cache and failure modes |
| [Security](operations/security.md) | Auth, OIDC, TLS, datasets, audit, egress |
| [Alerting](operations/alerting.md) | Rules, routes, silences, receivers and delivery metrics |
| [High availability](operations/high-availability.md) | Independent two-replica mode, collector fan-out, alert dedupe and rollback |
| [Edge mode](operations/edge-mode.md) | Single-node topology, sizing and deliberate non-goals |
| [Performance and sizing](operations/performance-sizing.md) | Memory and disk for your ingest rate |
| [Head-to-head benchmark](performance/comparison.md) | Reproducible query comparison harness |
| [Troubleshooting](operations/troubleshooting.md) | Symptoms, metrics to check, fixes |
| [Known issues](operations/known-issues.md) | Caveats worth knowing before you deploy |

## Development

| | |
|---|---|
| [Architecture](development/architecture.md) | Runtime, write/read paths, storage, security and delivery |
| [Diagrams](development/diagrams.md) | Animated SVG architecture, ingest, query, storage and alert visuals |
| [Developer guide](development/developer-guide.md) | Local setup, conventions, repository layout |
| [Testing](development/testing.md) | Test strategy and how to run each layer |
| [Test plan](development/test-plan.md) | End-to-end scenarios |
| [CI/CD](development/ci-cd.md) | Pipelines, gates, release process |
| [Efficiency plan](development/efficiency-plan.md) | Measured engine performance and optimisation backlog |
| [R2b UAT result](development/r2b-result.md) | Six-hour 1 GiB footer-cache soak and decision |
| [R3 findings and Phase 2b design](development/r3-findings.md) | R3 probe evidence, memory conclusion and gated binary-footer design |
| [Memomainry backlog](development/memory-backlog.md) | Measured query and ingest memory findings |
| [HA pair plan](development/ha-pair-plan.md) | Two independent replicas with collector fan-out and alert dedupe |
| [Glossary](development/glossary.md) | Terminology |

## Reference

| | |
|---|---|
| [HTTP API](api/openapi.yaml) | OpenAPI 3 description, also served at `/oas` |
| [HTTP API reference](reference/api.md) | Every endpoint, verified against the router |
| [Performance](performance/README.md) | Measured results and how each was obtained |
| [Durability](performance/durability.md) | WAL, flush ordering, crash recovery |
| [Hot path](performance/hot-path.md) | Measured hot-path optimisations |
| [Compaction](performance/compaction.md) | Compaction behaviour and tuning |

## Scope

Parqtel is a **metrics, logs and traces store with an SRE console, alerting and
optional object storage**. It is not a general-purpose log aggregator, a
distributed consensus system, or a metrics warehouse replacement. For where it
sits relative to other tools, see the [FAQ](getting-started/faq.md).

## Accuracy

Every performance number in these docs was measured against a running build and
says so. Where a measurement showed that an optimisation was not worth doing,
that is recorded too. The
[historical labels-column dictionary experiment](performance/block-pruning.md#row-level-label-storage-historical-dictionary-experiment)
measured **0 %** benefit from Arrow dictionary encoding because Parquet already
dictionary-encodes string columns. New indexed blocks now omit that column.
