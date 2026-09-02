# Parqtel Tutorials

This guide provides step-by-step tutorials for common SRE and DevOps scenarios using Parqtel.

## 1. Monitoring Nginx Logs

In this scenario, we will ingest Nginx access logs into Parqtel and extract metrics (Request Rate and Error Rate).

### Step 1: Define the Pipeline
Create a file named `rules/pipelines/nginx.yaml`:

```yaml
name: nginx-access-logs
enabled: true
stages:
  - name: parse
    type: preprocessor
    config:
      # Common Nginx Log Format
      pattern: '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent'
  - name: extract_metrics
    type: metric_extractor
    config:
      metrics:
        - name: nginx_requests_total
          type: counter
          value_field: status
        - name: nginx_errors_total
          type: counter
          value_field: status
          # Only count statuses starting with 4 or 5
          filter: "status >= 400"
```

### Step 2: Stream Logs to Parqtel
Use an OTLP-compatible collector (like OpenTelemetry Collector) to tail Nginx logs and send them to `http://parqtel:8080/v1/logs`.

### Step 3: Visualize in the UI
Go to the Parqtel UI and search for the `nginx_requests_total` metric. You can now build a Grafana dashboard using these extracted metrics.

---

## 2. High-Latency Alerting with Slack Notifications

In this scenario, we will set up an alert for high HTTP latency and receive notifications in Slack via MCP.

### Step 1: Set up the Slack MCP Server
Ensure your `mcp-slack` server is running and has a valid bot token.

```bash
# In .env (project root)
SLACK_BOT_TOKEN=xoxb-your-token
```

### Step 2: Define the Alert Rule
Create `rules/latency-high.yaml`:

```yaml
name: api-latency-high
severity: critical
interval_secs: 60
for_secs: 120
expression: "avg(http_request_duration_seconds[5m]) > 0.5"
labels:
  team: backend
  channel: #ops-alerts
annotations:
  summary: "API Latency is > 500ms"
  description: "Average latency for the last 5 minutes is {{ $value }}s."
```

### Step 3: Verify the Alert
Once latency exceeds 500ms for 2 minutes, Parqtel will transition the alert to `Firing`. If configured, the MCP server will pick up this state change and post a message to the Slack channel specified in the labels.

---

## 3. High-Cardinality Analysis

Parqtel excels at high-cardinality data because of its columnar Parquet storage.

### Scenario: Per-User Latency Tracking
If you have 100,000 users and want to track latency per user:

1. Send metrics with a `user_id` label.
2. In Prometheus/TSDB, this would cause a "cardinality explosion".
3. In Parqtel, this is just another column in a Parquet file.
4. Querying a specific user is extremely fast:
   ```bash
   curl "http://localhost:8080/api/v1/query" --get --data-urlencode 'query=http_request_duration_seconds{user_id="user_999"}' 
   ```

---

## Next Steps
Have a specific scenario you want to see? Open an issue!
