# Documentation

Documentation for Parqtel, an open-source SRE telemetry store.

## Getting started

| | |
|---|---|
| [Getting started](getting-started/getting-started.md) | First 15 minutes: install, ingest, query |
| [Tutorials](getting-started/tutorials.md) | Guided walk-throughs of common tasks |
| [FAQ](getting-started/faq.md) | Short answers to common questions |

## Guides

| | |
|---|---|
| [PQL guide](guides/pql-guide.md) | ParQL metrics, ParqtelQL log/trace search, pipelines |
| [Query functions](guides/query-functions.md) | Supported function surface, and what is not yet supported |
| [MCP](guides/mcp.md) | Model Context Protocol servers and tool schemas |
| [Best practices](guides/best-practices.md) | Sizing and operating guidance |
| [Argo Rollouts](guides/argo-rollouts.md) | Progressive delivery on Kubernetes |

## Operations

| | |
|---|---|
| [Configuration](operations/configuration.md) | Every knob, environment variable and TOML key |
| [Deployment](operations/deployment.md) | Docker, Kubernetes, Helm |
| [Performance and sizing](operations/performance-sizing.md) | How to reason about memory and disk for your ingest rate |
| [Troubleshooting](operations/troubleshooting.md) | Symptoms, metrics to check, fixes |
| [Known issues](operations/known-issues.md) | Caveats worth knowing before you deploy |

## Development

| | |
|---|---|
| [Architecture](development/architecture.md) | Crates, data flow, storage engine, concurrency model |
| [Diagrams](development/diagrams.md) | Visual architecture: context, crate graph, data flow, block lifecycle |
| [Developer guide](development/developer-guide.md) | Local setup, conventions, repository layout |
| [Testing](development/testing.md) | Test strategy and how to run each layer |
| [Test plan](development/test-plan.md) | End-to-end scenarios |
| [CI/CD](development/ci-cd.md) | Pipelines, gates, release process |
| [Glossary](development/glossary.md) | Terminology |

## Reference

| | |
|---|---|
| [HTTP API](api/openapi.yaml) | OpenAPI 3 description, also served at `/oas` |
| [Performance](performance/README.md) | Measured results and how each was obtained |

## Scope

Parqtel is a **metrics, logs and traces store with an SRE console and alerting**.
It is not a general-purpose log aggregator and does not replace a metrics
warehouse. For where it sits relative to other tools, see the
[FAQ](getting-started/faq.md).

## Accuracy

Every performance number in these docs was measured against a running build and
says so. Where a measurement showed that an optimisation was not worth doing,
that is recorded too. The
[historical labels-column dictionary experiment](performance/block-pruning.md#row-level-label-storage-historical-dictionary-experiment)
measured **0 %** benefit from Arrow dictionary encoding because Parquet already
dictionary-encodes string columns. New indexed blocks now omit that column.
