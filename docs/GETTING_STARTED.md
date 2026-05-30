# Getting Started with Parqtel

Welcome to Parqtel! This guide will walk you through your first 15 minutes with Parqtel, from installation to ingesting your first metrics and logs.

## 1. Installation

The fastest way to get started is using Docker. If you don't have Docker, you can follow the [Building from Source](docs/DEPLOYMENT.md#run-from-source) guide.

```bash
docker run -d \
  --name parqtel \
  -p 8080:8080 \
  -v parqtel_data:/var/lib/parqtel \
  parqtel/parqtel:latest
```

Verify it's running:
```bash
curl http://localhost:8080/health
```

## 2. Your First Metric

Parqtel uses the OpenTelemetry (OTLP) protocol. You can send data using `curl` to test the ingestion.

### Send a Counter Metric
Let's simulate a web server receiving requests.

```bash
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
              "timeUnixNano": "'$(date +%s%N)'",
              "attributes": [{"key": "method", "value": {"stringValue": "GET"}}, {"key": "status", "value": {"stringValue": "200"}}]
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

## 3. Your First Log

Parqtel also stores logs in Parquet for efficient searching.

### Send a Log Entry
```bash
curl -X POST http://localhost:8080/v1/logs/json \
  -H "Content-Type: application/json" \
  -d '{
    "resourceLogs": [{
      "resource": {
        "attributes": [{"key": "service.name", "value": {"stringValue": "my-web-app"}}]
      },
      "scopeLogs": [{
        "logRecords": [{
          "timeUnixNano": "'$(date +%s%N)'",
          "severityText": "INFO",
          "body": {"stringValue": "User logged in successfully"},
          "attributes": [{"key": "user_id", "value": {"stringValue": "user_123"}}]
        }]
      }]
    }]
  }'
```

### Query Logs
```bash
curl "http://localhost:8080/api/v1/logs?filter=severityText='INFO'"
```

## 4. Visualization

Parqtel ships with a built-in UI for quick exploration.

1. Open your browser and go to `http://localhost:8080/ui`.
2. You should see your `http_requests_total` metric and recent logs.

### Using Grafana
For production dashboards, we recommend Grafana.
1. Install the **SimpleJSON** datasource in Grafana.
2. Add a new datasource with URL `http://your-parqtel-ip:8080`.
3. Start building dashboards!

## 5. Next Steps

- **Configure Alerting:** Learn how to set up YAML-based rules in [Alerting Guide](docs/CONFIGURATION.md#alerts).
- **Architecture Deep Dive:** Understand how Parquet blocks work in the [Architecture Doc](docs/ARCHITECTURE.md).
- **Deployment:** Move to production with [Kubernetes/Helm](docs/DEPLOYMENT.md#kubernetes-helm).
- **MCP Integrations:** Connect Parqtel to Slack, PagerDuty, and more via [MCP](docs/MCP.md).

## Need Help?
Check the [Troubleshooting Guide](docs/TROUBLESHOOTING.md) or open an issue on GitHub.
