# Getting Started with Parqtel

Welcome to Parqtel! This guide will walk you through your first 15 minutes with Parqtel, from installation to ingesting your first metrics and logs.

## 1. Installation

The fastest way to get started is using Docker. If you don't have Docker, you can follow the [Building from Source](docs/DEPLOYMENT.md#run-from-source) guide.

```bash
docker run -d \
  --name parqtel \
  -p 8080:8080 \
  -v parqtel_data:/var/lib/parqtel \
  ghcr.io/parqtel/parqtel-oss:latest
```

Verify it's running:
```bash
curl http://localhost:8080/health
```

## 2. Your First Metric

Parqtel speaks the OpenTelemetry (OTLP) protocol over both **gRPC** (`:4317` — the OTel SDK default, no collector needed) and **HTTP** (protobuf or JSON). The examples below use `curl` against the HTTP JSON endpoints for quick testing; point your OTel SDK exporter at `http://localhost:8080` (gRPC) or `http://localhost:8080/v1/{metrics,logs,traces}` to send production traffic.

> **Note on OTLP JSON format**: `attributes` must be arrays of `{key, value}` objects (as shown below), not plain objects. This follows the OTLP JSON protobuf mapping.

### Send a Counter Metric
Let's simulate a web server receiving requests.

```bash
TS=$(date +%s)000000000

curl -X POST http://localhost:8080/v1/metrics/json \
  -H "Content-Type: application/json" \
  -d '{
    "resourceMetrics": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"stringValue": "my-web-app"}}]
      },
      "scopeMetrics": [{
        "metrics": [{
          "name": "http_requests_total",
          "sum": {
            "dataPoints": [{
              "asDouble": 1,
              "timeUnixNano": "'$TS'",
              "attributes": [{"key": "method", "value": {"stringValue": "GET"}}]
            }]
          }
        }]
      }]
    }]
  }'
```

### Query the Metric
Now, let's query it back using the Prometheus-compatible API.

```bash
curl "http://localhost:8080/api/v1/query?query=http_requests_total"
```

> Instant queries look back **1 minute**. Points older than that become visible via `/api/v1/query_range` once flushed to a Parquet block (default block duration: 2h, checked every 5s).

## 3. Your First Log

Parqtel also stores logs in Parquet for efficient searching.

### Send a Log Entry
```bash
TS=$(date +%s)000000000

curl -X POST http://localhost:8080/v1/logs/json \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"stringValue": "my-web-app"}}]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "'$TS'",
          "severityText": "INFO",
          "body": {"stringValue": "User logged in successfully"},
          "attributes": [{"key": "user_id", "value": {"stringValue": "user_123"}}]
        }]
      }]
    }]
  }'
```

### Query Logs
Log queries use a Prometheus-style selector plus a time range:

```bash
NOW=$(date +%s)
curl "http://localhost:8080/api/v1/logs?query=%7B%7D&start=$((NOW-3600))&end=$NOW&limit=50"
```

Filter by service:
```bash
curl "http://localhost:8080/api/v1/logs?query=service%3D%22my-web-app%22&start=$((NOW-3600))&end=$NOW&limit=50"
```

## 4. Visualization

Parqtel ships with a built-in zero-dependency web console for quick exploration.

1. Open your browser and go to `http://localhost:8080/ui`.
2. The **Overview** pane shows stat cards for each signal plus a 6-hour log-volume sparkline.
3. Click a card (Metrics / Logs / Traces / Alerts) to explore that signal.

The console includes:
- **Deep-linkable URLs** — the query, time range, and view are encoded in the page URL; share it to restore the exact state
- **Metrics Builder** — toggle between guided query building (metric + label filters with live PromQL preview) and raw Code mode
- **Log facets** — click "Fields" in the Logs view to browse field values and inject filters
- **Trace browse list** — grouped by trace with root service, duration, and error counts; click a trace for the waterfall
- **Alerts** — stream with severity/status filters, Evidence tab (metric chart + correlated logs), and an inline rule editor
- **Keyboard shortcuts** — press `?` in the console for the full reference

### Using Grafana
For production dashboards, we recommend Grafana.
1. Install the **SimpleJSON** datasource in Grafana.
2. Add a new datasource with URL `http://your-parqtel-ip:8080`.
3. Start building dashboards!

## 5. Next Steps

- **Query like a pro:** The full query language guide — ParQL (metrics), ParqtelQL (log/trace search), and cross-signal pipelines — lives in [docs/PQL_GUIDE.md](docs/PQL_GUIDE.md).
- **Configure Alerting:** Learn how to set up YAML-based rules in [Alerting Guide](docs/CONFIGURATION.md#alerts).
- **Architecture Deep Dive:** Understand how Parquet blocks work in the [Architecture Doc](docs/ARCHITECTURE.md).
- **Deployment:** Move to production with [Kubernetes/Helm](docs/DEPLOYMENT.md#kubernetes-helm).
- **MCP Integrations:** Connect Parqtel to Slack, PagerDuty, and more via [MCP](docs/MCP.md).

## Need Help?
Check the [Troubleshooting Guide](docs/TROUBLESHOOTING.md) or open an issue on GitHub.
