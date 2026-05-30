# Troubleshooting Guide

This guide helps you diagnose and resolve common issues encountered when running Parqtel.

## 1. Data Not Appearing

If you send data but queries return empty results, check the following:

### Check Ingestion Metrics
Visit `http://localhost:8080/metrics` and look for:
- `parqtel_ingest_total`: Is the count increasing?
- `parqtel_ingest_errors_total`: Are there any ingestion errors?

### Log Level
Set `RUST_LOG=debug` to see detailed ingestion logs.
```bash
PARQTEL__TELEMETRY__LOG_LEVEL=debug parqtel serve
```

### Time Range
Parquet blocks are time-bounded. If the `timeUnixNano` in your OTLP payload is outside the query range (or in the distant past/future), the data will be ignored or not appear in recent queries.
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
- **Diagnostic:** Check `parqtel_compaction_duration_seconds`.
- **Solution:** Increase `PARQTEL__STORAGE__COMPACTION_INTERVAL_SECS` or provide more CPU/IOPS.

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
