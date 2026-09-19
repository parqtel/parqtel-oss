# Parqtel Tutorials

This guide provides step-by-step tutorials for common SRE and DevOps scenarios using Parqtel.

## 1. Monitoring Nginx Logs

In this scenario, we will ingest Nginx access logs into Parqtel and extract metrics (Request Rate and Error Rate).

### Step 1: Define the Pipeline
Create a file named `rules/pipelines/nginx-access-logs.yaml` (the repo ships a complete example at that path — adapt it rather than writing from scratch):

```yaml
pipelines:
  - name: nginx-access-logs
    description: "Parse Nginx access logs, extract request counter, mask client IPs"
    match:
      signal: logs
      conditions:
        - field: service_name
          op: "="
          value: "nginx"

    stages:
      # Stage 1: Extract fields from the Nginx combined log format
      - type: processor
        name: parse_nginx_log
        processor: regex_extract
        source_field: body
        pattern: |
          (?P<client_ip>\d+\.\d+\.\d+\.\d+) - (?P<user>[^ ]*) \[(?P<timestamp>[^\]]+)\] "(?P<method>[A-Z]+) (?P<path>[^ ]*) HTTP/[\d.]+" (?P<status>\d+) (?P<bytes>\d+) "[^"]*" "[^"]*" (?P<duration_ms>[\d.]+)
        target_fields:
          http.method: method
          http.status_code: status
          http.duration_ms: duration_ms
        on_parse_failure: keep_original

      # Stage 2: Extract a request counter metric
      - type: metric_extract
        name: extract_request_count
        metric_name: nginx_requests_total
        metric_type: counter
        value_field: "constant:1"
        dimensions: [http.method, http.status_code, service_name]

      # Stage 3: Mask client IP addresses before storage
      - type: masker
        name: mask_client_ips
        rules:
          - field: body
            pattern: '\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}'
            replacement: "[IP_MASKED]"

      # Stage 4: Drop health-check noise
      - type: router
        name: drop_health_checks
        conditions:
          - field: http.target
            op: "=~"
            value: "^/(health|ready|live)"
        action: drop
```

Valid stage `type` values are `preprocessor`, `processor`, `metric_extract`, `masker`, and `router`; metric stages take `metric_name`/`metric_type`/`value_field`/`dimensions`/`condition`, and filtering uses `condition:` (`field`/`op`/`value`) — not a `filter:` expression. See [docs/CONFIGURATION.md](CONFIGURATION.md#pipeline-yaml-schema) for the full schema.

### Step 2: Stream Logs to Parqtel
Use an OTLP-compatible collector (like the OpenTelemetry Collector) to tail Nginx logs and send them to `http://parqtel:9090/v1/logs` (port 9090 in the compose stack; 8080 for a bare `parqtel serve`).

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
Create `rules/latency-high.yaml`. The rule schema splits the PromQL selector (`query`) from the threshold (`condition`) — there is no single `expression` field:

```yaml
id: api-latency-high
name: API Latency High
signal: metrics
query: 'http_request_duration_seconds'
condition:
  type: threshold
  operator: ">"
  value: 0.5
  for_duration_secs: 120
severity: critical
labels:
  team: backend
annotations:
  summary: "API Latency is > 500ms"
  description: "Average latency for the last 5 minutes exceeded the threshold."
enabled: true
```

Rules are evaluated by the global 15s loop (there is no per-rule `interval_secs`); `for_duration_secs` is the sustain window before the alert fires.

### Step 3: Route the Notification
Alert-to-channel routing is defined in separate route files under `rules/notifications/`, not by labels on the rule. A route matches on severity/labels and targets an MCP tool:

```yaml
# rules/notifications/latency-slack.yaml
name: latency-to-slack
match:
  severity: critical
  labels:
    team: backend
channels:
  - type: slack
    server: parqtel-mcp-slack
    tool: send_alert_message
    params:
      channel: "#ops-alerts"
```

Once latency exceeds 500ms for 2 minutes, Parqtel transitions the alert to `Firing` and the route dispatches it to Slack via the MCP server.

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
