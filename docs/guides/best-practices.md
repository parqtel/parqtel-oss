# Best Practices & Production Scaling

This guide outlines how to run Parqtel at scale in production environments.

## 1. Resource Tuning

### Memory Management
Parqtel's memory usage is primarily driven by:
1. **Active Buffers**: Configured by `max_rows_per_block` and the block duration.
2. **The Block Index**: Grows with the number of Parquet files on disk.
3. **The label interner**: Every point of a series shares one copy of its labels
   while the interner has room. Set `ingest.label_interner_capacity` to at
   least **twice** the active series count — each series is interned twice,
   once as received and once with `service.name` for the memory buffer.

**Recommendation:** leave `max_rows_per_block` at its default (1 M). A bigger
block is not cheaper: the per-block series table grows with the *series count*
in the block, not with the row count, so a block that holds more rows also
holds a bigger index. The 5-minute L1 rollup and the 2-hour L2 rollup are
what keep the number of blocks a query touches bounded.

### CPU & I/O
- **Compaction**: This is a CPU and Disk I/O intensive process. It runs every
  `storage.compaction_interval_secs` (default 300) and every pass is bounded
  (8 sources, 2 groups, 16 M rows), so it cannot monopolise the disk. If your
  queries are slow, check whether the compactor is lagging.
- **Zstd Level**: Parqtel uses Zstd for compression. If CPU usage is too high, you can switch to `lz4`.

## 2. Storage Strategy

### SSDs are Mandatory
Parquet is a columnar format that benefits from fast sequential and random reads. **Do not use HDD** for production data directories; the performance degradation will be significant.

### Retention vs. Compaction
- Keep `retention_days` realistic for your disk size.
- Ensure `compaction_interval_secs` is frequent enough to merge small blocks created during low-traffic periods. The default (300 s) matches the L1 window; lower it only if the rollup lags.
- Metric block files are named by level (`m_l0_`, `m_n_`, `m_l1_`, `m_l2_`), so `ls` on the data directory shows at a glance whether each rollup is keeping up.

## 3. Security Hardening

### TLS Termination
Parqtel supports in-process TLS for HTTP and OTLP gRPC via `[server.tls]`. A
reverse proxy or service mesh is still useful for certificate automation,
request logging, rate limiting and a single public entry point. If TLS
terminates at a proxy, keep `:8080` and `:4317` private.

### Authentication
HTTP authentication is optional and off by default. Enable scoped bearer tokens
and/or OIDC when the network is not fully trusted:

```toml
[auth]
enabled = true
# [[auth.tokens]] ...
```

`read` covers queries, `write` adds ingest, and `admin` adds mutations. OTLP gRPC
has no token layer — protect `:4317` with a proxy, mesh or private listener.
Keep `alerts.allow_runtime_updates = false` unless the admin surface is
authenticated. Alert delivery is fail-closed for private destinations; opt in
with `[alerting.routing.egress].allow_hosts` rather than `allow_private = true`
where possible.

### Network Isolation
In Kubernetes, use **NetworkPolicies** to restrict access:
- Allow ingress from your applications/collectors to port 8080.
- Allow ingress from Grafana to port 8080.
- Deny all other traffic.

## 4. Monitoring Parqtel

Don't let your monitoring tool go unmonitored!
- Use the `/metrics` endpoint to scrape Parqtel with another instance or a separate Prometheus.
- **Alert on:**
    - `parqtel_ingest_errors_total > 0`
    - `parqtel_query_errors_total > 0`
    - `parqtel_storage_blocks` / `parqtel_storage_bytes` (unbounded growth means compaction is lagging)
    - `parqtel_process_rss_bytes` (memory creep)
    - `parqtel_scan_slots_in_use` against `parqtel_scan_slots_limit` (4) — the
      block-scan budget; sustained saturation means queries are queued
    - `parqtel_scan_low_priority_skips_total` — alert evaluations deferred
      because no scan slot was free
    - Disk usage > 85%

## 5. High Availability (HA)

Parqtel is a single-writer engine. Object storage shares a catalog across
nodes, but there is no writer fencing; do not point two writers at one prefix.

For HA:
1. **Mirror ingest**: Send the same OTLP stream to two independent instances
   with separate data roots, or use the collector's queued retry and run one
   writer with fast restart.
2. **Persistence**: Use a PersistentVolumeClaim with `ReadWriteOnce` and let
   Kubernetes reschedule the pod. WAL replay recovers acknowledged writes.
3. **Read replicas**: A second node can adopt blocks from the same S3 prefix
   and serve queries, but keep compaction/retention on the writer.
4. **Failure detection**: Alert on `/ready`, `parqtel_flush_*`,
   `parqtel_process_rss_bytes`, catalog/upload warnings and disk pressure.

For true multi-writer scale-out, use separate prefixes and aggregate at query
time or with recording rules; do not rely on concurrent compaction.
