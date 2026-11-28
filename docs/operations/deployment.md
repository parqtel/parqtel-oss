# Deployment Guide

Parqtel supports multiple deployment methods, from a single Docker container to
production Kubernetes clusters. The default mode is **single-writer per data
root**: each process owns its WAL, buffer, indexes and compaction. An optional
S3-compatible bucket shares a catalog and lets reader nodes adopt blocks, but
there is no writer fencing. Keep one writer per prefix. The Helm chart also
supports an independent two-replica HA mode with separate PVCs; see the
[high-availability guide](high-availability.md) before enabling it.

## Docker (Single Container)

### Build

```bash
docker build -t parqtel:local .
```

The Dockerfile uses a multi-stage cargo-chef build:
- **Builder**: `rust:1.88-slim` (chef → planner → builder) with pkg-config, cmake, make, g++, protobuf-compiler, and libcurl4-openssl-dev (librdkafka OAuthbearer builds need `<curl/curl.h>`)
- **Probe**: a std-only Rust `healthcheck` binary used by the image `HEALTHCHECK`
- **Runtime**: a `runtime-libs` stage collects only the glibc/libgcc and CA bundle the binary needs, then `FROM scratch` ships that minimal rootfs (no shell; current image is roughly 60 MB)

### Run

```bash
docker run -d \
  --name parqtel \
  -p 8080:8080 \
  -p 4317:4317 \
  -v parqtel_data:/var/lib/parqtel \
  -e PARQTEL__STORAGE__DATA_DIR=/var/lib/parqtel/data \
  -e PARQTEL__LOGS__DATA_DIR=/var/lib/parqtel/logs \
  -e PARQTEL__TELEMETRY__LOG_FORMAT=json \
  parqtel:local
```

Publish `4317` if you ingest over OTLP gRPC (the default; set `server.grpc_bind_address = ""` to disable it). The container runs as a non-root UID with a read-only rootfs and no shell — there is nothing to `docker exec` into.

### Health Check

The image defines its own `HEALTHCHECK` (the bundled probe hitting `/health`), so inspect the reported status instead of exec-ing a curl that the scratch image doesn't have:
```bash
docker inspect --format '{{.State.Health.Status}}' parqtel
# or, from the host:
curl -f http://localhost:8080/health
```

## Docker Compose (Full Stack)

The Compose setup includes Parqtel, Grafana, Prometheus, a load generator, and the Parqtel MCP server (`mcp-parqtel`, port 3007) enabled by default so AI agents can query the stack out of the box.

### Setup

```bash
# From project root
cp .env.example .env
docker compose -f deploy/compose/docker-compose.yml up -d
```

Or via Make (first-time onboarding):

```bash
make dev-setup
```

### Services

| Service | Port | Description |
|---------|------|-------------|
| `parqtel` | 9090 | Main observability server |
| `grafana` | 3000 | Dashboards (auto-provisioned) |
| `prometheus` | 9091 | Self-monitoring scraper |
| `load-generator` | — | Synthetic data generator |
| `mcp-parqtel` | 3007 | Parqtel MCP server (enabled by default — queries metrics/logs/alerts/topology) |

### Environment Variables (`.env`)

```bash
# MCP
MCP_PARQTEL_PORT=3007
MCP_RATE_LIMIT=600
PARQTEL_API_URL=http://parqtel:9090

# Load Generator
LOAD_TEST_MODE=false
GENERATOR_NORMAL_SERIES=1000
GENERATOR_NORMAL_RPS=167

# Grafana
GRAFANA_ADMIN_PASSWORD=parqtel-dev
```

### Local Overrides

To customise ports or mounts locally without touching the committed file, create `deploy/compose/docker-compose.override.yml` (gitignored):

```bash
# docker-compose.override.yml is gitignored — safe for local secrets/port changes
```

### Teardown

```bash
docker compose -f deploy/compose/docker-compose.yml down -v  # -v removes volumes
# or
make local-down
```

## Kubernetes (Helm)

### Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- `kubectl` configured for your cluster

### Quick Install

From the published chart (the package is public, so no registry login is
needed):

```bash
helm install parqtel oci://ghcr.io/parqtel/charts/parqtel \
  --version 0.5.0 \
  --namespace parqtel \
  --create-namespace \
  -f deploy/k8s/overlays/production/values.yaml
```

From a checkout of this repository:

```bash
helm install parqtel deploy/helm/parqtel \
  --namespace parqtel \
  --create-namespace \
  -f deploy/k8s/overlays/production/values.yaml
```

### Value Overlays

Pre-configured value files for different environments:

| File | Use Case |
|------|----------|
| `deploy/k8s/overlays/minimal/values.yaml` | Minimal resources, single replica |
| `deploy/k8s/overlays/dev/values.yaml` | Development with debug logging |
| `deploy/k8s/overlays/production/values.yaml` | Production with PDB and NetworkPolicy (single replica) |
| `deploy/k8s/overlays/load-test/values.yaml` | High-resource for load testing |
| `deploy/k8s/overlays/ci/values.yaml` | CI/CD pipeline testing |
| `deploy/k8s/overlays/orbstack/values.yaml` | OrbStack local development |

### Helm Chart Features

The chart (`deploy/helm/parqtel`) includes:

- **Deployment** with configurable resources and probes; default mode caps `replicaCount` at 1
- **Independent HA StatefulSet** — opt in with `ha.enabled: true`; see the [HA guide](high-availability.md) for per-pod ingest and operational requirements
- **Sharded topology** — `mode: sharded` renders writer StatefulSet, reader Deployment + HPA, compactor Deployment and optionally one alert-evaluator Deployment; see [Sharded writers and stateless readers](#sharded-writers-and-stateless-readers)
- **PodDisruptionBudget** — ensures availability during rollouts
- **NetworkPolicy** — restricts ingress/egress traffic
- **ServiceMonitor** — Prometheus Operator integration
- **PrometheusRule** — built-in alerting rules
- **Ingress** — optional with TLS support
- **PersistentVolumeClaim** — for data persistence
- **RBAC** — ServiceAccount, Role, RoleBinding
- **MCP Deployment** — `mcp.parqtel.enabled` renders its own Deployment + Service, not a sidecar

### Sharded writers and stateless readers

![Sharded topology: collectors fan OTLP out to a writer StatefulSet whose pods publish Parquet blocks to a shared bucket and catalog; a reader Deployment scans the bucket, fetches rows newer than its watermark from every writer, and serves queries; one compactor merges and retires blocks](../assets/diagrams/sharded-topology.svg)

`mode: sharded` renders three workloads instead of one, all reading one shared
S3-compatible bucket:

| Workload | Kind | Role | Durable state |
|----------|------|------|---------------|
| `parqtel-writer` | StatefulSet, headless Service | `writer` | one PVC per pod: WAL staging + block cache |
| `parqtel-reader` | Deployment (+ HPA) | `reader` | none: `emptyDir` cache only |
| `parqtel-compactor` | Deployment | `compactor` | none: `emptyDir` cache only |

Writers ingest, flush and publish their blocks to the shared catalog; readers
answer queries from the catalog; the single compactor merges and retires
blocks. The main Service selects the readers, so a load balancer never sends a
query to a pod that can only see its own directory.

```yaml
mode: sharded
sharding:
  objectStore:
    enabled: true
    bucket: parqtel-blocks
    region: eu-north-1
    existingSecret: parqtel-aws        # AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
  internalToken:
    value: <random-string>             # or existingSecret + secretKey
  writer:
    replicas: 2
    storageSize: 50Gi
  reader:
    minReplicas: 2
    maxReplicas: 10
```

The writers keep the newest seconds in memory, so a reader cannot see them from
the bucket. The chart gives the readers `query.writer_endpoints` (one URL per
writer pod, from the headless Service) and every pod gets
`PARQTEL__QUERY__INTERNAL_AUTH_TOKEN` from a Secret; the endpoint answers `404`
without the token. A reader with no reachable writer still answers correctly,
from the bucket alone.

Writers, readers and the compactor always run with
`server.alert_evaluator = false`: every replica of those workloads would
otherwise evaluate every rule and duplicate notifications. Enable
`sharding.alertEvaluator.enabled` to add exactly one dedicated evaluator pod
(`Recreate` strategy, never scale it above one replica); it queries the same
reader-facing storage and posts the notifications. Ingest-side effects (spans,
RED metrics) stay on the writers.

```yaml
sharding:
  alertEvaluator:
    enabled: true
    cacheSize: 2Gi   # ephemeral block cache for rule evaluation
```

Point each collector at the writers' headless Service, not the reader Service:
[otel-collector-sharded.yaml](../../examples/agents/otel-collector-sharded.yaml)
hashes every item to exactly one writer (traces by `traceID`, metrics and logs
by `service`) through the `load_balancing` exporter, with a disk-backed retry
queue per signal. Nothing is duplicated across writers.

```bash
kubectl get pods -l app.kubernetes.io/component=writer    # parqtel-writer-0/1
kubectl get pods -l app.kubernetes.io/component=reader
helm template parqtel deploy/helm/parqtel -f deploy/helm/parqtel/ci/sharded-values.yaml | kubectl apply -f -
```

`helm lint`/`helm template` fail if `mode: sharded` is set without the bucket,
without the internal token, or together with `ha.enabled`.

### Custom Values Example

```yaml
replicaCount: 1  # single-writer; >1 is rejected by values.schema.json

resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 2Gi

persistence:
  enabled: true
  size: 50Gi
  storageClass: gp3

networkPolicy:
  enabled: true

mcp:
  parqtel:
    enabled: true
    env:
      PARQTEL_API_URL: "http://parqtel:8080"
```

### Local Development with k3d

```bash
# Create cluster and deploy
bash scripts/k8s/k8s-setup.sh
# or
make local-k3d-up

# Check status
make local-k3d-status

# Run load test
bash scripts/k8s/k8s-load-test.sh

# Teardown
bash scripts/k8s/k8s-teardown.sh
# or
make local-k3d-down
```

### Uninstall

```bash
helm uninstall parqtel -n parqtel
kubectl delete namespace parqtel
```

## systemd (Bare Metal)

### Install Binary

```bash
# Build release binary
cargo build --release

# Install
sudo cp target/release/parqtel /usr/local/bin/
sudo chmod +x /usr/local/bin/parqtel

# Create directories
sudo mkdir -p /var/lib/parqtel/{data,logs}
sudo mkdir -p /etc/parqtel
sudo useradd -r -s /bin/false parqtel
sudo chown -R parqtel:parqtel /var/lib/parqtel
```

### Configuration

Parqtel runs with built-in defaults if no config file is supplied. For a
systemd deployment, write the config file referenced by `parqtel.service`
(see [Configuration](configuration.md) for all keys):

```bash
sudo tee /etc/parqtel/parqtel.toml >/dev/null <<'EOF'
[server]
bind_address = "0.0.0.0:8080"
grpc_bind_address = "0.0.0.0:4317"

[storage]
data_dir = "/var/lib/parqtel/data"

[logs]
data_dir = "/var/lib/parqtel/logs"
EOF
```

### Service File

The service file is at `deploy/systemd/parqtel.service`:

```ini
[Unit]
Description=Parqtel Observability Engine
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=parqtel
Group=parqtel
ExecStart=/usr/local/bin/parqtel --config /etc/parqtel/parqtel.toml serve
Restart=on-failure
RestartSec=5
LimitNOFILE=65536

Environment=RUST_LOG=info
Environment=PARQTEL__STORAGE__DATA_DIR=/var/lib/parqtel/data
Environment=PARQTEL__LOGS__DATA_DIR=/var/lib/parqtel/logs
# Optional: export Parqtel's own traces + SLI metrics to an OTLP collector
#Environment=PARQTEL__TELEMETRY__OTLP_ENABLED=true
#Environment=PARQTEL__TELEMETRY__OTLP_ENDPOINT=http://collector:4317
#Environment=PARQTEL__TELEMETRY__OTLP_TRACE_LEVEL=info

[Install]
WantedBy=multi-user.target
```

### Enable and Start

```bash
sudo cp deploy/systemd/parqtel.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable parqtel
sudo systemctl start parqtel

# Check status
sudo systemctl status parqtel
sudo journalctl -u parqtel -f
```

## Production Recommendations

### Storage

- Use SSDs for the data directory — Parquet benefits from fast sequential reads.
  The read cache and WAL staging area are also latency-sensitive.
- Size storage for: `(ingest_rate × retention_days) / compression_ratio`.
  Typical compression ratio with Zstd: 10-20x.
- `storage.data_dir` holds metrics and trace blocks; `logs.data_dir` is
  separate. Trace blocks live under `<data_dir>/traces`.
- For object storage, follow [Object storage](object-storage.md). One writer per
  prefix; readers may adopt blocks; compaction/retention stay on the writer.
- The cache is bounded by `object_store.cache_bytes`; size it for the working
  set of blocks a query touches, not the whole bucket.

### Memory

- Block index is held in memory — scales with number of blocks
- Each active block buffer consumes memory proportional to `max_rows_per_block`
- Recommended: 512 MB minimum, 2 GB for high-cardinality workloads
- Set `PARQTEL__INGEST__LABEL_INTERNER_CAPACITY` to at least **twice** the
  active series count (default 1 048 576). Below that, memory grows with
  points instead of series — see
  [sizing](performance-sizing.md#3-allocator-and-label-interning)
- The allocator is jemalloc, linked into the binary. The glibc arena tunables
  that older deployments set are no longer required

### Networking

- Prefer in-process TLS (`[server.tls]`) or a reverse proxy. If a proxy
  terminates TLS, do not publish `:8080` or `:4317` directly.
- Enable `[auth]` (tokens/OIDC) when the network is not fully trusted. OTLP
  gRPC has no token layer; keep it on a private listener.
- Configure `ingest.max_body_size` to match your proxy's limit.
- Use `/health` for liveness and `/ready` for readiness. `/ready` checks local
  storage, not S3 reachability.
- For object storage, allow outbound HTTPS to the bucket endpoint and mount a
  custom CA at `/etc/ssl/certs/ca-certificates.crt` if needed.

### Monitoring

- Parqtel exposes Prometheus metrics at `/metrics` and live ingestion health at `/api/v1/ingest_rates` (per-signal current + 60s/5m/15m averages, wire bytes/sec, and `gap_secs` — alert on a growing gap to catch a stalled pipeline; sparkline history via the `history_secs` param, default 180s, max 900s)
- Key metrics to watch:
  - `parqtel_batches_received_total` — batches accepted
  - `parqtel_ingested_points_total` — ingestion throughput
  - `parqtel_query_duration_seconds` — query latency
  - `parqtel_query_errors_total` — query failures
  - `parqtel_storage_blocks` / `parqtel_storage_bytes` — compaction keeping up (unbounded growth = lagging compaction)
  - `parqtel_process_rss_bytes` — memory footprint vs. your request limit
  - `parqtel_scan_slots_in_use` / `parqtel_scan_slots_limit` — block scans
    against the process-wide budget (4)
  - `parqtel_scan_low_priority_skips_total` — alert evaluations and compaction
    passes deferred because no scan slot was free
  - `parqtel_index_sidecar_bytes{signal}` — size of the block-index sidecar
- **Self-telemetry**: set `telemetry.otlp_enabled = true` (with `otlp_endpoint`, `export_interval_secs`, `otlp_trace_level`) and Parqtel exports its own traces and SLI metrics over OTLP/gRPC to your collector, exactly like the services it ingests from — useful when the `/metrics` scrape path isn't wired into your pipeline.
- **Profiling**: `telemetry.profiling_enabled = true` enables the `/debug/pprof/{profile,summary,memory}` endpoints (404 while disabled) for CPU/memory analysis during tuning. Restrict them with a NetworkPolicy in Kubernetes — they expose runtime internals.

### Backup

- Block files are immutable once written — safe to copy while running
- Back up the `index.json` sidecars (per signal directory) for fast recovery (otherwise the index rebuilds from Parquet files)
- Use filesystem snapshots or rsync for consistent backups
- A sidecar written by this version has the shape
  `{"blocks": [...], "label_values": {...}}`. An older binary cannot parse it;
  a newer one reads the legacy bare-array shape and migrates it on first start
