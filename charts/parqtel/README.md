# parqtel Helm Chart

parqtel is an ultra-lightweight SRE observability tool for streaming OTel metrics ingestion into compressed Parquet chunks.

## Features

- **OTLP Ingestion**: Native support for Protobuf and JSON OTLP.
- **Kubernetes Custom Metrics Provider**: Direct integration with the HPA via API aggregation.
- **Resource Efficient**: Hard memory targets (<50MB RSS).
- **Parquet Storage**: Columnar storage with built-in compression.
- **Prometheus Compatible**: Drop-in replacement for Prometheus datasources in Grafana.

## Installation

```bash
helm repo add parqtel https://charts.parqtel.com
helm install my-parqtel parqtel/parqtel
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

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `image.repository` | string | `ghcr.io/parqtel/parqtel` | Container image repository |
| `replicaCount` | int | `1` | Number of replicas (Note: >1 requires shared storage) |
| `storage.size` | string | `10Gi` | Size of the persistent volume |
| `provider.enabled` | bool | `false` | Enable the Kubernetes custom metrics provider API |
| `provider.port` | int | `6443` | Port for the provider HTTPS listener |
| `provider.tlsSecretName` | string | `parqtel-provider-tls` | Name of the Secret to store/load provider TLS certificates |
| `parqtel.telemetry.logLevel` | string | `info` | Server log level |

---
*Maintained by the parqtel team.*
