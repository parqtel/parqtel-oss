# Best Practices & Production Scaling

This guide outlines how to run Parqtel at scale in production environments.

## 1. Resource Tuning

### Memory Management
Parqtel's memory usage is primarily driven by:
1. **The Block Index**: Grows with the number of Parquet files on disk.
2. **Active Buffers**: Configured by `max_rows_per_block`.

**Recommendation:** For high-throughput environments, increase `max_rows_per_block` to 2M or 5M to reduce the number of small Parquet files, but ensure you have at least 2GB of RAM.

### CPU & I/O
- **Compaction**: This is a CPU and Disk I/O intensive process. If your queries are slow, check if the compactor is lagging.
- **Zstd Level**: Parqtel uses Zstd for compression. If CPU usage is too high, you can lower the compression level (on the roadmap) or switch to `lz4`.

## 2. Storage Strategy

### SSDs are Mandatory
Parquet is a columnar format that benefits from fast sequential and random reads. **Do not use HDD** for production data directories; the performance degradation will be significant.

### Retention vs. Compaction
- Keep `retention_days` realistic for your disk size.
- Ensure `compaction_interval_secs` is frequent enough to merge small blocks created during low-traffic periods.

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
    - `parqtel_compaction_duration_seconds` (sudden spikes)
    - Disk usage > 85%

## 5. High Availability (HA)

Parqtel is currently designed as a single-node engine. For HA:
1. **Replication**: Run two identical Parqtel instances and have your OTLP collector "load balance" or "mirror" data to both.
2. **Persistence**: Use a Persistent Volume Claim (PVC) in Kubernetes with `ReadWriteOnce` and rely on K8s to restart the pod on a healthy node if one fails.
