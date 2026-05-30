# Configuration Reference

Parqtel uses [Figment](https://github.com/SergioBenitez/Figment) for layered configuration. Settings are resolved in this priority order (highest wins):

1. **CLI flags** — `--bind`, `--data-dir`, `--config`, `--log-level`
2. **Environment variables** — prefixed with `PARQTEL__`, using `__` as section separator
3. **TOML config file** — specified via `--config` or auto-detected at `config/default.toml`
4. **Built-in defaults** — hardcoded in the source

## TOML Configuration File

```toml
[server]
bind_address = "0.0.0.0:8080"
max_connections = 1024
shutdown_timeout_secs = 30

[storage]
backend = "parquet"
data_dir = "data"
block_duration_secs = 7200        # 2 hours
max_rows_per_block = 1000000
compression = "zstd"              # zstd | snappy | lz4 | none
retention_days = 7
compaction_interval_secs = 3600   # 1 hour
row_group_size = 100000

[logs]
data_dir = "data/logs"
block_duration_secs = 1800        # 30 minutes
max_rows_per_block = 200000
compression = "zstd"
retention_days = 3
compaction_interval_secs = 3600
row_group_size = 20000

[ingest]
max_body_size = 10485760          # 10 MB
wal_enabled = false
log_wal_enabled = true

[query]
max_series = 1000
max_samples_per_series = 10000
timeout_secs = 30

[ui]
enabled = true

[telemetry]
log_level = "info"
log_format = "text"               # text | json

[alerts]
rules_dir = "rules"
noise_window_firings = 30
refinement_enabled = true

[k8s_provider]
enabled = false
bind_address = "0.0.0.0:6443"
cache_expiry_secs = 30
```

## Section Reference

### `[server]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `bind_address` | String | `"0.0.0.0:8080"` | TCP address to listen on |
| `max_connections` | Integer | `1024` | Maximum simultaneous TCP connections |
| `shutdown_timeout_secs` | Integer | `30` | Seconds to wait for in-flight requests during graceful shutdown |

### `[storage]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `backend` | String | `"parquet"` | Storage backend (currently only `"parquet"`) |
| `data_dir` | Path | `"data"` | Directory for metric block files |
| `block_duration_secs` | Integer | `7200` | Time span of a single block (seconds) |
| `max_rows_per_block` | Integer | `1000000` | Maximum rows before forced rotation |
| `compression` | String | `"zstd"` | Parquet compression: `zstd`, `snappy`, `lz4`, `none` |
| `retention_days` | Integer | `7` | Days to retain metric data |
| `compaction_interval_secs` | Integer | `3600` | Interval between compaction passes |
| `row_group_size` | Integer | `100000` | Rows per Parquet row group |

### `[logs]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `data_dir` | Path | `"data/logs"` | Directory for log block files |
| `block_duration_secs` | Integer | `1800` | Time span of a single log block |
| `max_rows_per_block` | Integer | `200000` | Maximum rows before forced rotation |
| `compression` | String | `"zstd"` | Parquet compression codec |
| `retention_days` | Integer | `3` | Days to retain log data |
| `compaction_interval_secs` | Integer | `3600` | Interval between compaction passes |
| `row_group_size` | Integer | `20000` | Rows per Parquet row group |

### `[ingest]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_body_size` | Integer | `10485760` | Maximum request body size in bytes (10 MB) |
| `wal_enabled` | Boolean | `false` | Enable write-ahead log for metrics |
| `log_wal_enabled` | Boolean | `true` | Enable write-ahead log for logs |

### `[query]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `max_series` | Integer | `1000` | Maximum time series returned per query |
| `max_samples_per_series` | Integer | `10000` | Maximum samples per series |
| `timeout_secs` | Integer | `30` | Query execution timeout |

### `[ui]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `true` | Enable the built-in web UI at `/ui` |

### `[telemetry]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `log_level` | String | `"info"` | Log level filter (trace, debug, info, warn, error) |
| `log_format` | String | `"text"` | Log output format: `text` or `json` |

### `[alerts]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `rules_dir` | String | `"rules"` | Directory containing alert rule YAML files |
| `noise_window_firings` | Integer | `30` | Number of firings in window to calculate noise score |
| `refinement_enabled` | Boolean | `true` | Enable automatic alert refinement |

### `[k8s_provider]`

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `enabled` | Boolean | `false` | Enable Kubernetes custom metrics API server |
| `bind_address` | String | `"0.0.0.0:6443"` | Address for the K8s API service |
| `cache_expiry_secs` | Integer | `30` | Cache TTL for metric values |

## Environment Variables

Environment variables use the `PARQTEL__` prefix with `__` as the section separator. Examples:

```bash
# Server
export PARQTEL__SERVER__BIND_ADDRESS="0.0.0.0:9090"
export PARQTEL__SERVER__MAX_CONNECTIONS=2048

# Storage
export PARQTEL__STORAGE__DATA_DIR="/var/lib/parqtel/data"
export PARQTEL__STORAGE__COMPRESSION="snappy"
export PARQTEL__STORAGE__RETENTION_DAYS=14
export PARQTEL__STORAGE__BLOCK_DURATION_SECS=3600

# Logs
export PARQTEL__LOGS__DATA_DIR="/var/lib/parqtel/logs"
export PARQTEL__LOGS__RETENTION_DAYS=7

# Ingest
export PARQTEL__INGEST__MAX_BODY_SIZE=20971520

# Query
export PARQTEL__QUERY__TIMEOUT_SECS=60
export PARQTEL__QUERY__MAX_SERIES=5000

# Telemetry
export PARQTEL__TELEMETRY__LOG_LEVEL="debug"
export PARQTEL__TELEMETRY__LOG_FORMAT="json"

# Alerts
export PARQTEL__ALERTS__RULES_DIR="/etc/parqtel/rules"
```

## CLI Flags

```bash
parqtel [OPTIONS] [COMMAND]

Options:
  -c, --config <PATH>      Path to TOML configuration file [env: PARQTEL_CONFIG]
  -b, --bind <ADDRESS>     TCP bind address [env: PARQTEL_BIND]
  -d, --data-dir <PATH>    Data directory [env: PARQTEL_DATA_DIR]
      --log-level <LEVEL>  Log level override [env: RUST_LOG]
  -h, --help               Print help
  -V, --version            Print version

Commands:
  serve    Start the HTTP server (default)
  compact  Run one compaction pass and exit
  inspect  Print storage summary as JSON
  export   Export metric data to CSV
```

## Alert Rule YAML Schema

Alert rules are defined in YAML files under the `rules_dir`:

```yaml
name: http-error-rate
type: static                    # static | anomaly
severity: warning               # critical | warning | info
interval_secs: 60
for_secs: 300                   # Duration before transitioning to Firing
expression: "rate(http_errors_total[5m]) / rate(http_requests_total[5m])"
threshold:
  operator: ">"
  value: 0.05
labels:
  team: platform
annotations:
  summary: "HTTP error rate above 5%"
  runbook: "https://wiki.example.com/runbooks/http-errors"
```

## Recording Rule YAML Schema

```yaml
name: http:requests:rate5m
expression: "rate(http_requests_total[5m])"
interval_secs: 60
labels:
  aggregation: "recording_rule"
```

## Pipeline YAML Schema

```yaml
name: nginx-access-logs
enabled: true
stages:
  - name: parse
    type: preprocessor
    config:
      pattern: '$remote_addr - $remote_user [$time_local] "$request" $status $body_bytes_sent'
  - name: extract_metrics
    type: metric_extractor
    config:
      metrics:
        - name: nginx_requests_total
          type: counter
          value_field: status
        - name: nginx_bytes_total
          type: counter
          value_field: body_bytes_sent
```

## Validation

Configuration is validated at startup. Invalid configurations produce clear error messages:

```
Error: server.bind_address cannot be empty; storage.compression must be one of: zstd, snappy, lz4, none
```
