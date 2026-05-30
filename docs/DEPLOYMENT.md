# Deployment Guide

Parqtel supports multiple deployment methods, from a single Docker container to production Kubernetes clusters with autoscaling.

## Docker (Single Container)

### Build

```bash
docker build -t parqtel:local .
```

The Dockerfile uses a multi-stage build:
- **Builder**: `rust:1.85-slim` with build dependencies
- **Runtime**: `gcr.io/distroless/cc-debian12:nonroot` (~15 MB final image)

### Run

```bash
docker run -d \
  --name parqtel \
  -p 8080:8080 \
  -v parqtel_data:/var/lib/parqtel \
  -e PARQTEL__STORAGE__DATA_DIR=/var/lib/parqtel/data \
  -e PARQTEL__LOGS__DATA_DIR=/var/lib/parqtel/logs \
  -e PARQTEL__TELEMETRY__LOG_FORMAT=json \
  parqtel:local
```

### Health Check

```bash
docker exec parqtel curl -f http://localhost:8080/health
```

## Docker Compose (Full Stack)

The Compose setup includes Parqtel, Grafana, Prometheus, a load generator, and all 7 MCP servers.

### Setup

```bash
cd deploy/compose
cp .env.example .env
# Edit .env with your API keys for MCP integrations (optional)
docker-compose up -d
```

### Services

| Service | Port | Description |
|---------|------|-------------|
| `parqtel` | 9090 | Main observability server |
| `grafana` | 3000 | Dashboards (auto-provisioned) |
| `prometheus` | 9091 | Self-monitoring scraper |
| `load-generator` | — | Synthetic data generator |
| `mcp-slack` | 3001 | Slack MCP server |
| `mcp-pagerduty` | 3002 | PagerDuty MCP server |
| `mcp-jira` | 3003 | Jira MCP server |
| `mcp-notion` | 3004 | Notion MCP server |
| `mcp-discord` | 3005 | Discord MCP server |
| `mcp-gdocs` | 3006 | Google Docs MCP server |
| `mcp-parqtel` | 3007 | Parqtel query MCP server |

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

### Override File

For local development customizations, copy the override example:

```bash
cp docker-compose.override.yml.example docker-compose.override.yml
```

### Teardown

```bash
docker-compose down -v  # -v removes volumes
```

## Kubernetes (Helm)

### Prerequisites

- Kubernetes 1.25+
- Helm 3.x
- `kubectl` configured for your cluster

### Quick Install

```bash
helm install parqtel deploy/charts/parqtel \
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

### Helm Chart Features

The chart (`deploy/charts/parqtel`) includes:

- **Deployment** with configurable replicas, resources, and probes
- **HorizontalPodAutoscaler** — scales on CPU/memory/custom metrics
- **PodDisruptionBudget** — ensures availability during rollouts
- **NetworkPolicy** — restricts ingress/egress traffic
- **ServiceMonitor** — Prometheus Operator integration
- **PrometheusRule** — built-in alerting rules
- **Ingress** — optional with TLS support
- **PersistentVolumeClaim** — for data persistence
- **RBAC** — ServiceAccount, Role, RoleBinding
- **MCP Deployments** — optional sidecar MCP servers

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
bash deploy/k8s/setup.sh

# Check status
make -C deploy/k8s cluster-status

# Run load test
bash deploy/k8s/load-test.sh

# Teardown
bash deploy/k8s/teardown.sh
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

- Parqtel exposes Prometheus metrics at `/metrics`
- Key metrics to watch:
  - `parqtel_ingest_total` — ingestion throughput
  - `parqtel_query_duration_seconds` — query latency
  - `parqtel_blocks_total` — number of active blocks
  - `parqtel_compaction_duration_seconds` — compaction performance

### Backup

- Block files are immutable once written — safe to copy while running
- Back up the `index.bin` file for fast recovery (otherwise it rebuilds from Parquet files)
- Use filesystem snapshots or rsync for consistent backups
