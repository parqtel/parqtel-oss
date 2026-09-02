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
- **Instant queries** (`/api/v1/query`) look back only **1 minute**. Older buffered data won't appear there — query it via `/api/v1/query_range` instead.
- **Metrics, logs, and traces** are all queryable immediately after ingest via the in-memory buffer (buffer drains on flush — no double-counting).
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

### "Invalid timestamp column" / arrow2-era blocks
After the arrow2 → arrow 59 migration, Parquet blocks written by older builds are unreadable: compaction and scans log `Arrow error: Invalid timestamp column`. **Wipe the data directory** (`data/`, `data/logs/`, `data/traces/`) when upgrading across that boundary — old blocks cannot be converted in place.

### Stale Docker image
If the compose stack behaves oddly after a source change (panics referencing `arrow2`, missing endpoints), the image is stale. Run `make local-rebuild` — `make local-up` alone reuses the previously built image.

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
