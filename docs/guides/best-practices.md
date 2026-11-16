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
holds a bigger index. The 5-minute L1 rollup is what keeps the number of
blocks a query touches bounded.

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
- Metric block files are named by level (`m_l0_`, `m_n_`, `m_l1_`), so `ls` on the data directory shows at a glance whether the rollup is keeping up.

## 3. Security Hardening

### TLS Termination
Parqtel does not natively support TLS. **Always** place Parqtel behind a reverse proxy (Nginx, Envoy, or HAProxy) or use a Service Mesh (Istio, Linkerd) to terminate TLS.

### Authentication
Parqtel endpoints are open by default. Use your reverse proxy to implement:
- **Basic Auth** or **OAuth2** for the Query API and UI.
- **IP Allow-listing** for the OTLP Ingestion API.

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

Parqtel is currently designed as a single-node engine. For HA:
1. **Replication**: Run two identical Parqtel instances and have your OTLP collector "load balance" or "mirror" data to both.
2. **Persistence**: Use a Persistent Volume Claim (PVC) in Kubernetes with `ReadWriteOnce` and rely on K8s to restart the pod on a healthy node if one fails.
