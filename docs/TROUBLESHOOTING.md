# Troubleshooting Guide

This guide helps you diagnose and resolve common issues encountered when running Parqtel.

## 1. Data Not Appearing

If you send data but queries return empty results, check the following:

### Check Ingestion Metrics
Visit `http://localhost:8080/metrics` and look for:
- `parqtel_batches_received_total`: Is the count increasing?
- `parqtel_ingested_points_total`: Are data points being counted?
- `parqtel_ingest_errors_total`: Are there any ingestion errors?

### Log Level
Set `RUST_LOG=debug` to see detailed ingestion logs.
```bash
PARQTEL__TELEMETRY__LOG_LEVEL=debug parqtel serve
```

### Time Range and Flush Behaviour
- **Instant queries** (`/api/v1/query`) look back **5 minutes** (the Prometheus default, via `query.lookback_delta_ns`). Older buffered data won't appear there — query it via `/api/v1/query_range` instead.
- **Metrics, logs, and traces** are all queryable immediately after ingest via the in-memory buffer (buffer drains on flush — no double-counting).
- **Live ingestion health**: `curl localhost:8080/api/v1/ingest_rates` shows per-signal current + 60s/5m/15m averages and a `gap_secs` field that counts how long since the last sample — a growing gap means data stopped arriving (see the UI's rate cards for the same data as a sparkline).
- If the `timeUnixNano` in your OTLP payload is outside the query range (or in the distant past/future), the data will be ignored.
- **Tip:** Ensure your system clocks are synchronized via NTP.

### WAL Recovery
If Parqtel crashed, it might be recovering from the Write-Ahead Log (WAL). Check logs for "Recovering from WAL...".

## 2. Query Failures

### Timeout Errors
If a query times out (default 30s), it might be scanning too many blocks.
- **Solution:** Increase `PARQTEL__QUERY__TIMEOUT_SECS` or narrow your time range.

### "No such metric"
Verify the metric exists in the index:
```bash
curl http://localhost:8080/api/v1/label/__name__/values
```

### High Memory Usage during Query
Queries involving high cardinality (thousands of series) or large time ranges can consume significant memory.
- **Solution:** Use recording rules to pre-aggregate data for frequently used dashboards.

## 3. Storage & Disk Issues

### Disk Full
Parqtel will fail to rotate blocks if the disk is full.
- **Monitor:** Check `df -h` on the data directory.
- **Solution:** Decrease `PARQTEL__STORAGE__RETENTION_DAYS` to purge old data faster.

### Slow Compaction
If the compactor cannot keep up with the ingestion rate, you will have many small Parquet files, slowing down queries.
- **Diagnostic:** Check `parqtel_storage_blocks` / `parqtel_storage_bytes` / `parqtel_storage_rows` on `/metrics` for block accumulation, and watch server logs for `Compaction failed` errors.
- **Solution:** Decrease `PARQTEL__STORAGE__COMPACTION_INTERVAL_SECS` (more frequent passes) or provide more CPU/IOPS.

## 3a. Ingest latency (contention)

Every signal is ingested under one mutex, and a block flush runs while that
mutex is held. These series make that contention measurable instead of
requiring a profiler:

| Metric | What it tells you |
|--------|-------------------|
| `parqtel_ingest_rotator_shards` | Writer shards in the metrics rotator. Reports 0 when an embedder never set it. |
| `parqtel_ingest_lock_wait_seconds{signal}` | How long a request waited for the ingest mutex or one of its writer shards. This is the best single predictor of ingest p99 — a p99 approaching the flush duration means requests are queueing behind a flush. |
| `parqtel_flush_duration_seconds{signal}` | Wall time of each flush that actually wrote rows (encode + compress + fsync), including capacity-triggered flushes inside a request, which the 5-second tick does not see. |
| `parqtel_flush_inflight{signal}` | Flushes currently running. Normally 0 or 1. |
| `parqtel_flush_rows_total{signal}` | Rows written to blocks since start — tells you how much work each flush is doing. |
| `parqtel_index_lock_wait_seconds` | How long the block-index writer waited for the write lock. Every query handler reads that lock, so this is why a query tail latency tracks the index. |
| `parqtel_index_sidecar_bytes{signal}` | Size of the block-index sidecar on disk. This is the number that predicts "disk full in N days", and it grows with the retention window. |
| `parqtel_index_pending_writes{signal}` | 1 when the in-memory index has changes not yet written to the sidecar. Persist runs on a debounce (`server.index_persist_interval_secs`); persistently stuck at 1 means writes are failing — check the logs for `failed to persist block index`. |

```promql
# p99 time an ingest request spent waiting for the mutex
histogram_quantile(0.99, sum by (le, signal) (rate(parqtel_ingest_lock_wait_seconds_bucket[5m])))

# flush pressure: how long each block write takes
histogram_quantile(0.99, sum by (le, signal) (rate(parqtel_flush_duration_seconds_bucket[15m])))
```

All signals are emitted from boot, including zero-valued series, so a
dashboard query does not have to handle series appearing and disappearing.

**If `parqtel_ingest_lock_wait_seconds` p99 is high:** the blocks are too
large or rotate too rarely, so each flush holds the lock for a long time.
Two levers, in order of preference:

1. Raise `PARQTEL__INGEST__ROTATOR_SHARDS` (default 4). Concurrent pushes
   then continue into fresh shard buffers while a flush encodes, so only the
   flush's *own* shard is blocked. Block count and durability are unchanged —
   the shards are merged into a single file on flush.
2. Lower `PARQTEL__STORAGE__MAX_ROWS_PER_BLOCK` (and `PARQTEL__LOGS__...` for
   logs) so each flush does less work.

Note that even fully sharded, a request that triggers a flush still waits for
that flush: it is not acknowledged until its data is on disk. Removing that
wait is BL-01-14 and is deferred until the WAL can recover an unacknowledged
flush.

**If `parqtel_index_lock_wait_seconds` is high:** the index is being
re-serialised and rewritten while holding the write lock, so its cost scales
with the total index size — that is, with your retention window. Index writes
are debounced and happen off the lock, so sustained contention means either a
very large index or contending background maintenance (compaction/retention).
Raise `PARQTEL__SERVER__INDEX_PERSIST_INTERVAL_SECS` to write less often, and
`PARQTEL__SERVER__RETENTION_INTERVAL_SECS` to sweep less often.

### "Invalid timestamp column" / arrow2-era blocks
After the arrow2 → arrow 59 migration, Parquet blocks written by older builds are unreadable: compaction and scans log `Arrow error: Invalid timestamp column`. **Wipe the data directory** (`data/`, `data/logs/`, `data/traces/`) when upgrading across that boundary — old blocks cannot be converted in place.

### Stale Docker image
If the compose stack behaves oddly after a source change (panics referencing `arrow2`, missing endpoints), the image is stale. Run `make local-rebuild` — `make local-up` alone reuses the previously built image.

## 4. Self-Observability

### Traces / SLI metrics not exported
`telemetry.otlp_enabled = true` exports Parqtel's own traces and SLI metrics over OTLP/gRPC to `telemetry.otlp_endpoint`.
- **Check startup logs** — if the OTLP SDK failed to initialise, the server logs `telemetry.otlp_enabled=true but the OTLP SDK failed to initialise — continuing with console logs only` and continues with console logs only.
- **Trace level**: `otlp_trace_level` is independent of the console `log_level`, so trace export still works when console logs are raised to `warn`.
- **SLI push interval**: `export_interval_secs` (default 30).

### Profiling endpoints 404
`/debug/pprof/{profile,summary,memory}` return 404 unless `telemetry.profiling_enabled = true`. Enable it, then capture a profile and view the flamegraph:
```bash
curl -o profile.pb.gz http://localhost:8080/debug/pprof/profile?seconds=30
go tool pprof -http=:0 profile.pb.gz
```
Restrict these endpoints with a NetworkPolicy in Kubernetes — they expose runtime internals.

## 4. MCP Connectivity

### "Connection Refused"
Ensure the MCP server is running and bound to the correct address.
- **Check:** `curl http://localhost:3001/health` (for Slack MCP).

### "Rate Limit Exceeded"
MCP servers have built-in rate limiting.
- **Solution:** Increase `MCP_RATE_LIMIT` in the environment variables of the MCP server.

## 5. Built-in UI Issues

### UI Not Loading
Ensure `PARQTEL__UI__ENABLED=true` (default is true).

### Missing Graphs
The UI depends on the `/api/v1/*` endpoints. If those are blocked by a firewall or proxy, the UI will not show data.

## Getting More Help

1. **GitHub Issues:** Search existing issues or open a new one with your `RUST_LOG=debug` output.
2. **Community:** Join our Slack/Discord (if available) for real-time support.
3. **Logs:** Always include the last 100 lines of logs when reporting an issue.
