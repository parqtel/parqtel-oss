# Documentation

Documentation for Parqtel, an open-source SRE telemetry store.

## Start here

| | |
|---|---|
| [Getting Started](GETTING_STARTED.md) | First 15 minutes: install, ingest, query |
| [Tutorials](TUTORIALS.md) | Guided walk-throughs of common tasks |
| [FAQ](FAQ.md) | Short answers to common questions |

## Using Parqtel

| | |
|---|---|
| [PQL Guide](PQL_GUIDE.md) | ParQL metrics, ParqtelQL log/trace search, pipelines |
| [Query Functions](QUERY_FUNCTIONS.md) | Supported function surface, and what is not yet supported |
| [Glossary](GLOSSARY.md) | Terminology |

## Operating Parqtel

| | |
|---|---|
| [Configuration](CONFIGURATION.md) | Every knob, env var and TOML key |
| [Deployment](DEPLOYMENT.md) | Docker, Kubernetes, Helm |
| [Argo Rollouts](ARGO_ROLLOUTS.md) | Progressive delivery on Kubernetes |
| [Troubleshooting](TROUBLESHOOTING.md) | Symptoms, metrics to check, fixes |
| [Best Practices](BEST_PRACTICES.md) | Sizing and operating guidance |
| [Performance & Sizing](PERFORMANCE_SIZING.md) | How to reason about memory for your ingest rate |

## Building on Parqtel

| | |
|---|---|
| [Architecture](ARCHITECTURE.md) | Crates, data flow, storage engine, concurrency model |
| [Developer Guide](DEVELOPER_GUIDE.md) | Local setup, conventions, layout |
| [Testing](TESTING.md) | Test strategy and how to run each layer |
| [MCP](MCP.md) | Model Context Protocol server and tool schemas |
| [CI/CD](CI_CD.md) | Pipelines, gates, release process |
| [Performance notes](benchmarks/PERFORMANCE.md) | Measured results, with the caveat where each number came from |

## Scope

Parqtel is a **metrics, logs and traces store with an SRE console and alerting**.
It is not a general-purpose log aggregator and does not replace a
metrics warehouse. For where it sits relative to other tools, see the
[FAQ](FAQ.md).

## Accuracy

Every performance number in these docs was measured against a running build,
and says so. Where a claim was measured and **failed** — where an
optimisation turned out not to be worth doing — that is recorded too rather than
quietly dropped. See
[performance notes](benchmarks/PERFORMANCE.md#series-dictionary-for-the-labels-column-bloc-0304)
for the clearest example: the proposed series dictionary was measured at **0 %**
benefit, because Parquet already dictionary-encodes string columns, and was
dropped.

Internal design proposals, audit logs and build-phase findings are not kept
here; they are working documents, not user documentation.
