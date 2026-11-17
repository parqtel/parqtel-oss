# Edge / single-node mode

Parqtel's default shape is a single ~15 MB process with a local data directory.
That is a product, not a limitation to apologise for: one binary, one disk, no
external services, useful on a laptop, a single VM, or a sidecar host.

## Topology

```text
OTel SDK / Collector ──OTLP/gRPC :4317──┐
Grafana / curl ──HTTP :8080─────────────┼── parqtel (single process)
MCP client ──/mcp───────────────────────┘        │
                                                 └── data/ (local Parquet)
```

Recommended settings for an edge host:

```toml
[storage]
block_duration_secs = 300          # shorter pre-flush window, lower RAM
retention_days = 3

[ingest]
buffer_max_points = 1000000
buffer_max_bytes = 536870912       # 512 MiB estimate
series_limit_per_metric = 100000

[auth]
enabled = false                    # only on a trusted host/network
```

Put the listener behind the host firewall or a local reverse proxy when the
network is not trusted. TLS and scoped bearer tokens are available if needed.

## Deliberate non-goals

- No replication, sharding, or multi-writer operation. One process owns one data
  directory; the Helm chart rejects `replicaCount > 1`.
- No object storage. Data lives on local disk, and retention deletes it there.
- No per-tenant isolation. Use one instance per team/dataset if that matters.
- No DataFusion/SQL surface. PromQL, ParqtelQL, and the API are the contract.
- No dashboard builder. Use Grafana (Prometheus or SimpleJSON datasource).

## When to leave edge mode

Move to a multi-node deployment only after the object-storage `BlockStore` and
shared catalog land (see ADR 0001). Until then, adding a replica splits the
dataset instead of scaling it.

## Operating checks

```bash
curl -s localhost:8080/health
curl -s localhost:8080/ready
curl -s localhost:8080/metrics | grep -E 'parqtel_(buffer|scan|ingest_rejected|series_rejected)'
```

Watch buffer drops, series rejections, scan admission skips, and RSS. A flat
RSS with a rising drop counter means the ingest rate exceeds the configured
budget; raise it or shorten the block duration.
