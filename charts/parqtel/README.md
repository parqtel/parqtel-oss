# parqtel Helm Chart

parqtel is an ultra-lightweight SRE observability engine — streaming OTel metrics, logs, and traces into compressed Apache Parquet files.

## Features

- **OTLP Ingestion**: Native support for Protobuf and JSON OTLP over HTTP and gRPC (port 4317).
- **Kubernetes Custom Metrics Provider**: Direct integration with the HPA via API aggregation.
- **Resource Efficient**: Hard memory targets (<50MB RSS at idle).
- **Parquet Storage**: Columnar storage with built-in compression (zstd, snappy, lz4, none).
- **Prometheus Compatible**: Drop-in replacement for Prometheus datasources in Grafana (SimpleJSON).
- **Multi-signal**: Unified storage for metrics, logs, and traces with cross-signal correlation.
- **Alerting & Pipelines**: Built-in rule evaluation, recording rules, and stream processing.

## Installation

```bash
# From the charts directory
helm install my-parqtel ./charts/parqtel

# Or with a values overlay
helm install parqtel ./charts/parqtel \
  --namespace parqtel \
  --create-namespace \
  -f deploy/k8s/overlays/production/values.yaml
```

## Security

This chart follows strict security defaults:
- **Non-root**: Runs as UID 65534.
- **Read-only Filesystem**: The container root filesystem is read-only.
- **Capabilities**: All Linux capabilities are dropped.
- **Seccomp**: Uses the `RuntimeDefault` profile.
- **Privilege Escalation**: Explicitly disabled.

## Configuration

Refer to [values.yaml](values.yaml) for a full list of configuration options.

### Common Settings

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `image.repository` | string | `ghcr.io/parqtel/parqtel` | Container image repository |
| `replicaCount` | int | `1` | Number of replicas (Note: >1 requires shared storage) |
| `storage.size` | string | `10Gi` | Size of the persistent volume |
| `storage.storageClassName` | string | `""` | Storage class (empty = cluster default) |
| `service.port` | int | `8080` | HTTP service port |
| `ingestion.grpc.enabled` | bool | `true` | Enable OTLP gRPC ingestion on port 4317 |
| `provider.enabled` | bool | `false` | Enable the Kubernetes custom metrics provider API |
| `provider.port` | int | `6443` | Port for the provider HTTPS listener |
| `provider.tlsSecretName` | string | `parqtel-provider-tls` | Name of the Secret to store/load provider TLS certificates |
| `parqtel.telemetry.logLevel` | string | `info` | Server log level |
| `parqtel.telemetry.logFormat` | string | `json` | Log format (text or json) |

### Parqtel Engine Configuration (under `parqtel.`)

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `storage.blockDurationSecs` | int | `7200` | Metric block duration (2h) |
| `storage.maxRowsPerBlock` | int | `1000000` | Max rows per metric block |
| `storage.compression` | string | `zstd` | Parquet compression codec |
| `storage.retentionDays` | int | `7` | Metric data retention |
| `storage.compactionIntervalSecs` | int | `3600` | Compaction interval (1h) |
| `storage.rowGroupSize` | int | `100000` | Parquet row group size |
| `logs.blockDurationSecs` | int | `1800` | Log block duration (30m) |
| `logs.maxRowsPerBlock` | int | `200000` | Max rows per log block |
| `logs.compression` | string | `zstd` | Log compression codec |
| `logs.retentionDays` | int | `3` | Log data retention |
| `logs.compactionIntervalSecs` | int | `3600` | Log compaction interval |
| `logs.rowGroupSize` | int | `20000` | Log row group size |
| `ingest.maxBodySize` | int | `10485760` | Max request body (10MB) |
| `ingest.walEnabled` | bool | `false` | Enable WAL for metrics |
| `ingest.logWalEnabled` | bool | `true` | Enable WAL for logs |
| `query.maxSeries` | int | `1000` | Max series per query |
| `query.maxSamplesPerSeries` | int | `10000` | Max samples per series |
| `query.timeoutSecs` | int | `30` | Query timeout |
| `query.lookbackDeltaNs` | int | `300000000000` | Instant query lookback (5m) |
| `ui.enabled` | bool | `true` | Enable built-in UI |
| `alerts.rulesDir` | string | `/etc/parqtel/rules` | Alert rules directory |
| `alerts.noiseWindowFirings` | int | `30` | Noise analysis window |
| `alerts.refinementEnabled` | bool | `true` | Enable alert refinement |
| `alerts.noiseSuppressionThreshold` | float | `0.7` | Noise suppression threshold |

### Alert Rules

Mount alert rule YAML files into `/etc/parqtel/rules` via ConfigMap or volume to enable alerting.

```yaml
# Example values snippet to mount rules
extraVolumes:
  - name: alert-rules
    configMap:
      name: my-alert-rules
extraVolumeMounts:
  - name: alert-rules
    mountPath: /etc/parqtel/rules
    readOnly: true
```

### MCP Servers

Each MCP server can be enabled independently under `mcp.<name>.enabled`. Required secrets should be provided via `mcp.<name>.env` or `mcp.<name>.secretName`.

```yaml
mcp:
  slack:
    enabled: true
    env:
      SLACK_BOT_TOKEN: "xoxb-..."
  pagerduty:
    enabled: true
    env:
      PAGERDUTY_API_KEY: "..."
```

### Value Overlays

Pre-configured value files for different environments (in `deploy/k8s/overlays/`):

| File | Use Case |
|------|----------|
| `minimal/values.yaml` | Minimal resources, single replica |
| `dev/values.yaml` | Development with debug logging |
| `production/values.yaml` | Production with HPA, PDB, NetworkPolicy |
| `load-test/values.yaml` | High-resource for load testing |
| `ci/values.yaml` | CI/CD pipeline testing |
| `orbstack/values.yaml` | OrbStack local development |

---

*Maintained by the parqtel team.*