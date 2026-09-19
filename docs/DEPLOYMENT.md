# Deployment Guide

Parqtel supports multiple deployment methods, from a single Docker container to production Kubernetes clusters with autoscaling.

## Docker (Single Container)

### Build

```bash
docker build -t parqtel:local .
```

The Dockerfile uses a multi-stage cargo-chef build:
- **Builder**: `rust:1.87-slim` (chef → planner → builder) with pkg-config, cmake, g++, and protobuf-compiler
- **Probe**: a std-only Rust `healthcheck` binary used by the image `HEALTHCHECK`
- **Runtime**: a `runtime-libs` stage collects only the glibc/libgcc the binary needs, then `FROM scratch` ships that minimal rootfs (~15 MB final image, no shell)

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

The Compose setup includes Parqtel, Grafana, Prometheus, and a load generator. The Parqtel self-MCP server (`mcp-parqtel`, port 3007) is **enabled by default** so AI agents can query the stack out of the box; the third-party MCP servers (Slack, PagerDuty, Jira, Notion, Discord, Google Docs — ports 3001–3006) are commented out — uncomment the ones you need after setting the corresponding env vars in `.env`.

### Setup

```bash
# From project root
cp .env.example .env
# Optionally add MCP API keys in .env
docker compose up -d
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
| `mcp-parqtel` | 3007 | Parqtel self-MCP (enabled by default — queries metrics/logs/alerts/topology) |
| `mcp-{slack,pagerduty,jira,notion,discord,gdocs}` | 3001–3006 | Third-party MCP servers (opt-in — uncomment in `docker-compose.yml`) |

### Environment Variables (`.env`)

```bash
# MCP Integrations (optional)
SLACK_BOT_TOKEN=xoxb-...
PAGERDUTY_API_KEY=...
JIRA_BASE_URL=https://your-org.atlassian.net
JIRA_USER_EMAIL=...
JIRA_API_TOKEN=...
NOTION_API_KEY=secret_...
DISCORD_BOT_TOKEN=...
GOOGLE_SERVICE_ACCOUNT_JSON=...

# Load Generator
LOAD_TEST_MODE=false
GENERATOR_NORMAL_SERIES=1000
GENERATOR_NORMAL_RPS=167

# Grafana
GRAFANA_ADMIN_PASSWORD=parqtel-dev
```

### Local Overrides

To customise ports, mounts, or enable MCP servers locally without touching the committed file, create a `docker-compose.override.yml` (gitignored):

```bash
# docker-compose.override.yml is gitignored — safe for local secrets/port changes
# Example: enable the Slack MCP server
# services:
#   mcp-slack:
#     profiles: []
```

### Teardown

```bash
docker compose down -v  # -v removes volumes
# or
make local-down
```

## Kubernetes (Helm)

### Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- `kubectl` configured for your cluster

### Quick Install

```bash
helm install parqtel charts/parqtel \
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
| `deploy/k8s/overlays/production/values.yaml` | Production with HPA, PDB, NetworkPolicy |
| `deploy/k8s/overlays/load-test/values.yaml` | High-resource for load testing |
| `deploy/k8s/overlays/ci/values.yaml` | CI/CD pipeline testing |
| `deploy/k8s/overlays/orbstack/values.yaml` | OrbStack local development |

### Helm Chart Features

The chart (`charts/parqtel`) includes:

- **Deployment** with configurable replicas, resources, and probes
- **HorizontalPodAutoscaler** — scales on CPU/memory/custom metrics
- **PodDisruptionBudget** — ensures availability during rollouts
- **NetworkPolicy** — restricts ingress/egress traffic
- **ServiceMonitor** — Prometheus Operator integration
- **PrometheusRule** — built-in alerting rules
- **Ingress** — optional with TLS support
- **PersistentVolumeClaim** — for data persistence
- **RBAC** — ServiceAccount, Role, RoleBinding
- **MCP Deployments** — each enabled MCP server (`mcp.<name>.enabled`) renders as its own Deployment + Service, not a sidecar

### Custom Values Example

```yaml
replicaCount: 3

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

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

networkPolicy:
  enabled: true

mcp:
  slack:
    enabled: true
    env:
      SLACK_BOT_TOKEN: "xoxb-..."
```

### Local Development with k3d

```bash
# Create cluster and deploy
bash scripts/k8s-setup.sh
# or
make local-k3d-up

# Check status
make local-k3d-status

# Run load test
bash scripts/k8s-load-test.sh

# Teardown
bash scripts/k8s-teardown.sh
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

```bash
sudo cp config/default.toml /etc/parqtel/parqtel.toml
# Edit /etc/parqtel/parqtel.toml as needed
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

- Use SSDs for the data directory — Parquet benefits from fast sequential reads
- Size storage for: `(ingest_rate × retention_days) / compression_ratio`
- Typical compression ratio with Zstd: 10-20x

### Memory

- Block index is held in memory — scales with number of blocks
- Each active block buffer consumes memory proportional to `max_rows_per_block`
- Recommended: 512 MB minimum, 2 GB for high-cardinality workloads

### Networking

- Place behind a reverse proxy (nginx, envoy) for TLS termination
- Configure `max_body_size` to match your proxy's limit
- Use health check endpoint (`/health`) for load balancer probes

### Monitoring

- Parqtel exposes Prometheus metrics at `/metrics` and live ingestion health at `/api/v1/ingest_rates` (per-signal current + 60s/5m/15m averages, wire bytes/sec, and `gap_secs` — alert on a growing gap to catch a stalled pipeline; sparkline history via the `history_secs` param, default 180s, max 900s)
- Key metrics to watch:
  - `parqtel_batches_received_total` — batches accepted
  - `parqtel_ingested_points_total` — ingestion throughput
  - `parqtel_query_duration_seconds` — query latency
  - `parqtel_query_errors_total` — query failures
  - `parqtel_storage_blocks` / `parqtel_storage_bytes` — compaction keeping up (unbounded growth = lagging compaction)
  - `parqtel_process_rss_bytes` — memory footprint vs. your request limit
- **Self-telemetry**: set `telemetry.otlp_enabled = true` (with `otlp_endpoint`, `export_interval_secs`, `otlp_trace_level`) and Parqtel exports its own traces and SLI metrics over OTLP/gRPC to your collector, exactly like the services it ingests from — useful when the `/metrics` scrape path isn't wired into your pipeline.
- **Profiling**: `telemetry.profiling_enabled = true` enables the `/debug/pprof/{profile,summary,memory}` endpoints (404 while disabled) for CPU/memory analysis during tuning. Restrict them with a NetworkPolicy in Kubernetes — they expose runtime internals.

### Backup

- Block files are immutable once written — safe to copy while running
- Back up the `index.json` sidecars (per signal directory) for fast recovery (otherwise the index rebuilds from Parquet files)
- Use filesystem snapshots or rsync for consistent backups
