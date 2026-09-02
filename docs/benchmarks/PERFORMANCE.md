# Hot-Path Performance Optimizations

Branch: `perf/hot-path-optimizations` (merged; optimizations are part of current `main`)
Benchmark harness: `parqtel-server/examples/perf_bench.rs` (untracked-by-default; run with `cargo run --release -p parqtel-server --example perf_bench`)
Raw results: `baseline_before.txt`, `results_after.txt` (3 runs each, median-of-5 per metric)

## What changed

| # | Change | File(s) | Rationale |
|---|--------|---------|-----------|
| 1 | **Non-blocking flushes** — Parquet encode + zstd compress + disk I/O moved to `tokio::task::spawn_blocking`. The writer is swapped out (`mem::replace`) so the rotator can accept new pushes immediately. Flush is now idempotent on an empty buffer (fixes spurious `"Cannot flush empty buffer"` errors on idle systems). | `parqtel-ingest/src/service.rs` | Previously the entire compression + `fs::write` ran synchronously on a tokio worker thread while holding the ingest mutex — stalling all concurrent ingest batches and blocking a runtime worker. |
| 2 | **Capacity pre-check instead of error-string control flow** — rotator checks `writer.len() + incoming > capacity` and flushes *before* pushing, instead of matching `e.to_string().contains("buffer is full")` per point. Also fixes silent data loss: the old code discarded points of the batch that tripped capacity mid-push. Push returns whether it flushed so callers drain the memory buffer at the right time. | `service.rs` | Typed, lossless, one check per batch instead of string formatting per failed push. |
| 3 | **Scanner runs on blocking pool with pre-acquired semaphore** — `tokio::spawn` (async worker) replaced by `spawn_blocking`; semaphore permit acquired *before* spawning so concurrency is bounded without oversubscribing. | `parqtel-core/src/storage/scanner.rs` | Blocking file I/O inside `tokio::spawn` starves async workers under load. |
| 4 | **Row-group statistics pruning** — before decoding a block, row groups whose column-0 timestamp min/max cannot overlap `[start_ns, end_ns]` are skipped using parquet2 statistics (which were already written but never read). Falls back to scanning when stats are absent. | `scanner.rs` | Enables time-range pruning *within* block files. |
| 5 | **Multi-row-group block files** — flushes now split each block into ~25K-row Parquet row groups instead of one giant group. | `parqtel-ingest/src/writer.rs` | Prerequisite for #4: a single-row-group file has nothing to prune. Also bounds decode memory spikes. |
| 6 | **Per-chunk label caching in scanner** — parsed `LabelSet`s are cached by their raw JSON text within each chunk (keys borrow from the chunk arrays). Metric scan additionally skips resource-attribute parsing entirely (the scanner never used it), skips kind/correlation extraction, filters timestamp + metric name *before* any allocation, and compares metric names by borrowed `&str` instead of `.to_string()` per row. Log rows get cached attribute/resource label parsing via `row_to_log` (signature now takes caller-scoped caches). | `scanner.rs`, `models/storage/reader.rs` | Label JSON repeats once per series across thousands of rows; parsing it once per unique string removes ~1 serde round-trip per point. |
| 7 | **Log query: filter before sort, allocation-free search** — severity/search/matcher filtering now happens before the timestamp sort, and case-insensitive search uses byte-window ASCII folding instead of allocating `body.to_lowercase()` per record per query. ponytail: non-ASCII case folding is not handled. | `parqtel-query/src/executor.rs` | Fewer allocations and a smaller sort input. |
| 8 | **OTLP protobuf ingest fixed** — `/v1/{metrics,logs,traces}` were routed to the *JSON* handlers, so every protobuf request failed with 400. Added content-type dispatch (`application/x-protobuf` → proto handler, else JSON) per the OTLP spec. | `parqtel-server/src/router.rs`, `handlers/ingest.rs` | Pre-existing bug surfaced by the existing failing test; proto handlers existed but were unrouted. |
| 9 | Minor: compact index sidecar serialization (no pretty-print); clippy fix in prometheus handler. | `storage/index.rs`, `handlers/prometheus.rs` | |

## Benchmark methodology

Single machine, release build (`cargo run --release -p parqtel-server --example perf_bench`).

Dataset: 12 seeded Parquet blocks × 50K points × 100 series (2 row groups/block after change #5), plus live batches through the real OTLP JSON decode path.

Each number = median of 5 iterations after warmup; tables below show medians of 3 process runs.

## Results

Throughput in points/sec, higher is better:

| Benchmark | Before (main) | After (branch) | Δ |
|---|---|---|---|
| ingest (decode + buffer push) | ~570 K pts/s | ~550 K pts/s | ≈ flat (±3% noise) |
| flush (50K pts → Parquet) | ~840 K pts/s | ~830 K pts/s | ≈ flat (compression-bound) |
| **scan (full range, 600K pts)** | ~4.58 M pts/s | **~6.40 M pts/s** | **+39%** |
| query (full range, executor) | ~1.99 M pts/s | ~1.95 M pts/s | ≈ flat |
| **scan narrow** (pruned to 25K pts) | n/a* | 15–16 ms/op | new capability |
| **query narrow** (pruned) | n/a* | ~26 ms/op (~11.9 M pts/s effective) | new capability |
| svc-ingest (8 concurrent workers, flush contention) | ~2110 ms/op | ~2125 ms/op | parity, see notes |

\* Narrow benchmarks were added mid-work; they only become meaningful together with changes #4+#5, which is exactly their point: a query touching half of one block out of twelve now decodes ~half of ~one file instead of everything.

### Notes on the numbers

- **The full-range query is unchanged** because executor-side grouping/downsampling dominates once scanning is fast. Scanning itself improved 39%.
- **svc-ingest is at parity by design.** The old code held the ingest mutex across the whole batch loop including synchronous Parquet writes — terrible for latency (workers fully stall behind each other's flushes) but not throughput, since work was serialized anyway. The win from change #1/#3 is *isolation*: no tokio worker thread ever blocks on disk I/O, so event-loop tasks (health checks, queries, other signals) don't stall behind flushes. Under co-located load this prevents p99 spikes rather than raising mean throughput.
- The svc-ingest bench also validated a regression we caught and fixed during development: without propagating the flush signal back (#2), the memory buffer never drained and alloc pressure cost ~7%.

### Reproducing

```bash
cargo run --release -p parqtel-server --example perf_bench
```

Baseline comparison requires checking out `main` (the harness only uses APIs present on both branches).

## Known follow-ups

- Shard rotators per metric-name hash if single-mutex ingest contention ever shows up in profiles.
- Expose `ROW_GROUP_ROWS` (currently 25K, hardcoded) via `BlockConfig`.
- Non-ASCII case folding for log search (currently ASCII-only).
- `get_log_field_values` / `list_label_values` re-scan blocks per call; add a short-TTL cache if UI traffic makes them hot.
