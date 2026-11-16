# Parqtel Performance & Memory Sizing

> **Status: partially superseded.** The per-point string cloning diagnosed below
> was fixed — label sets are now shared behind an `Arc` in the query evaluator
> and block index, and the write-ahead log removes the pre-flush buffering cliff.
> See [the performance notes](../performance/README.md) for current,
> re-measured numbers.
>
> The sizing **method** below is still the right way to reason about a
> deployment, and §5 has been refreshed. The absolute figures were measured on
> one staging cluster (RKE2, 1 CPU / 3 GiB cgroup) with the built-in
> self-telemetry (`parqtel_process_rss_bytes`) and the embedded CPU profiler
> (`/debug/pprof/summary`); re-measure rather than reusing them.

## 1. Root cause of the >2 GiB RSS (measured, not guessed)

Parqtel holds every ingested point in the in-memory `MemoryBuffer` until the
block flush. Buffer entry cost is dominated by label/name/UTF-8 strings that
are **cloned per point** — the CPU profile confirms it:

| Sample | Window | Finding |
|---|---|---|
| Steady state (99 Hz) | 12 s | **100 % of CPU samples in `__clone` (memcpy)** |
| Threads | — | 4 (Tokio sized to the 1-CPU cgroup — not a thread explosion) |

So memory and CPU shared one root cause: per-point string cloning. That is
fixed — `LabelSet` is shared behind an `Arc` throughout the evaluator, so a
series' labels are cloned once per block rather than once per step
(`ast::SharedLabels`).

The structural point still holds: RAM scales with **buffered points**, and
buffered points scale with **ingest rate × block duration**. Two consequences
that were *not* addressed and remain the live risk:

- The memory buffer is still unbounded — no max size, no byte accounting and no
  eviction. See the sizing rule below before raising the ingest rate.
- `storage.block_duration_secs` (default **2 h**) sets how long points sit
  buffered. Shortening it trades query freshness (points become visible on
  flush) against resident memory.

The WAL added for durability does not add per-point cost: one record per metric,
and every append is flushed to the OS (an `fsync` happens on
`ingest.wal_sync_interval_ms`).

### Measured data points (oman-stg, ~840 pts/s metrics ingest)

| Config (block duration / allocator) | Peak buffer | RSS floor | Flush peak | Post-flush |
|---|---|---|---|---|
| 300 s, glibc defaults | ~251 k pts | 2309 MiB | 2435 MiB (HWM) | **never returns** |
| 120 s, glibc defaults | ~98 k pts | 1272 MiB | 1355 MiB | returns (mostly) |
| 120 s, `MALLOC_ARENA_MAX=2` + 128 MiB trim | ~98 k pts | **1174 MiB** | 1350 MiB | returns every cycle |

Two compounding effects made the old deployment look “leaky”:

1. **Linear per-point cost** — ~6.3 KiB/point buffered (derived below).
2. **glibc arena retention** — with default arenas, RSS pinned at the
   high-water mark (2309 MiB) even after the buffer drained from 251 k → 240
   points. With `MALLOC_ARENA_MAX=2` + `MALLOC_TRIM_THRESHOLD_=134217728`, RSS
   falls back after every flush — the sawtooth tracks live load.

### Linear model

```text
RSS ≈ BASELINE + (KiB_PER_POINT × peak_buffered_points) + FLUSH_TRANSIENT

BASELINE          ≈ 1.15 GiB   (runtime, OTel SDK, query engine, block index,
                                allocator slack — measured, includes page cache
                                attribution to the cgroup)
KiB_PER_POINT     ≈ 6.3 KiB    (slope between the 251k-pt and 98k-pt configs:
                                (2309−1174) MiB / (251k−98k) pts ≈ 6.3 KiB)
FLUSH_TRANSIENT   ≈ 80–140 MiB (Arrow record batches + Parquet encode of one
                                block; see parqtel_flush_rows histogram for
                                the block size driving it)
peak_buffered_points ≈ ingest_rate_pts_per_sec × block_duration_secs
```

Cross-check: 1.15 GiB + 6.3 KiB × 251 k + 0.1 GiB ≈ 2.8 GiB — bounds the
observed 2.4 GiB HWM (the model intentionally over-counts because the buffer
also drains into the encode path during the window).

## 2. Sizing formula (metrics-to-memory ratio)

For a target ingest rate R (pts/s) and block duration D (s):

```text
peak_buffer = R × D                          points
buffer_mem  = peak_buffer × 6.3 KiB
rss_peak    = 1.15 GiB + buffer_mem + 0.15 GiB
memory_limit ≥ 1.5 × rss_peak                (OOM safety factor)
```

Worked examples (with the allocator tuning from §3 applied):

| Ingest rate | Block duration | Peak buffer | rss_peak (est.) | Recommended limit |
|---|---|---|---|---|
| 0.5 k pts/s | 120 s | 60 k | 1.7 GiB | **2.5 GiB** |
| 1 k pts/s | 120 s | 120 k | 2.0 GiB | **3 GiB** |
| 2 k pts/s | 120 s | 240 k | 2.8 GiB | **4.5 GiB** |
| 1 k pts/s | 300 s | 300 k | 3.3 GiB | **5 GiB** |
| 5 k pts/s | 60 s | 300 k | 3.3 GiB | **5 GiB** |

**Rule of thumb:** every extra **1 k pts/s of ingest costs ~6.3 MiB of RAM per
second of block duration** (6.3 KiB × 1000). Adding 60 s to the block duration
at 1 k pts/s costs ~380 MiB. Logs are ~10× cheaper per record (strings arrive
pre-owned, one record = one row); traces are ~2× a metric point.

### OOM-avoidance rules

1. **Size from the formula, verify with the SLIs** — after deploy, watch
   `parqtel_process_rss_bytes` across ≥ 3 flush cycles (10 min at D=120); the
   flush peak must stay ≤ 65 % of the limit.
2. **Never raise D to fix query latency** — the 5-min instant-query lookback
   (`query.lookback_delta_ns`) reads the buffer anyway; D only controls flush granularity.
3. **Alert before the OOM kill**:
   `parqtel_process_rss_bytes / on() kube_pod_container_resource_limits{resource="memory"} > 0.65`.
4. **Buffer occupancy is your leading indicator** —
   `parqtel_buffer_points` rising monotonically across a flush means the flush
   is failing (disk full? PVC stuck?) and OOM is minutes away.
5. **CPU follows memory here** — a pod near its CPU limit with 100 % `__clone`
   samples is about to also stall flushes (encode is CPU-bound); keep the CPU
   request ≥ 0.5 core at 1 k pts/s.

## 3. Allocator and label interning

**The allocator is built in.** The server links `tikv-jemallocator` as its
global allocator, so the glibc tunables that used to be required are no longer
needed:

```yaml
# No longer required — jemalloc is linked into the binary.
# MALLOC_ARENA_MAX / MALLOC_TRIM_THRESHOLD_ only affect glibc builds.
```

Measured end to end with the real binaries (578 K series, 2.3 M points over
HTTP, `label_interner_capacity=2097152`), idle RSS 40 s after ingest:

| Allocator | Idle RSS | Peak RSS |
|---|---|---|
| glibc | 1.5–1.8 GiB | 2.0–2.1 GiB |
| jemalloc (shipped) | 0.9–1.0 GiB | 1.5–1.7 GiB |

**Set the label interner capacity.** The interner lets every point of a series
share one copy of its labels. Below the working set, that sharing is lost and
memory grows with *points* instead of *series*:

```yaml
env:
  - name: PARQTEL__INGEST__LABEL_INTERNER_CAPACITY
    value: "2097152"   # at least 2x the active series count
```

Measured with 578 K series and 2.3 M points through the live ingest path:

| Capacity | Peak heap | Steady RSS |
|---|---|---|
| 262 144 (old default) | 4613 MiB | 5839 MiB |
| 1 048 576 (current default) | 4120 MiB | 5740 MiB |
| 2 097 152 (2× series, above) | 2876 MiB | 3011 MiB |

Each series is interned twice — once as received, once with `service.name`
for the memory buffer — so the capacity should be at least twice the active
series count. The table only holds label sets that were actually seen, so a
high value costs nothing on a small deployment.

## 4. Profiling playbook (how these numbers were obtained)

```bash
kubectl -n monitoring port-forward deploy/parqtel 19092:9090 &

# Memory decomposition (live buffer vs RSS vs high-water mark)
curl -s localhost:19092/debug/pprof/memory | jq

# CPU hot spots — expect `__clone` dominance until interning ships
curl -s 'localhost:19092/debug/pprof/summary?seconds=15&top=10' | jq

# Full protobuf profile for `go tool pprof` (flamegraph in the browser)
go tool pprof -http=:0 'http://localhost:19092/debug/pprof/profile?seconds=30'
```

Profiling is gated by `parqtel.telemetry.profiling_enabled` (default **false**;
oman-stg enables it) and serialized by a process-wide lock — a second capture
while one runs returns 429.

## 5. Limits and caveats

* **The memory buffer is unbounded.** `MemoryBuffer` has no cap, no byte
  accounting and no eviction, so resident memory is a function of ingest rate x
  block duration with no ceiling. Size the data directory and the container limit
  from the formula in §2 and alert on `parqtel_process_rss_bytes`.
* **`KiB_PER_POINT` is conservative.** The 6.3 KiB/point slope was measured
  before label sets were shared behind an `Arc`, before the label interner was
  made configurable, and before the write-ahead log, so real usage is lower. Measure resident memory at your own ingest rate before
  tightening a limit; `crates/parqtel-server/examples/perf_bench.rs` reproduces
  the measurement.
* **`parqtel_flush_duration_seconds` and `parqtel_flush_inflight`** are rendered
  in `/metrics`; `parqtel_flush_rows_total` is the cheaper per-signal counter
  for dashboards. See [Troubleshooting](troubleshooting.md#4-ingest-latency-contention).
