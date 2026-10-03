# BL-04 — Runtime, Concurrency Limits, Caching & CI Gates

**Domain:** `parqtel-server` (router, state, grpc, metrics, telemetry), `parqtel-core::config`, workspace CI
**Goal:** predictable overload behaviour, no misapplied limits, and performance regressions caught before release.

Severity: **C** Critical, **H** High, **M** Medium, **L** Low. Effort: S ≤ 2d, M ≤ 1w, L > 1w.

---

## BL-04-01 (H) — No lock-wait, flush-queue or blocking-pool metrics; contention is invisible

**Evidence** — the existing SLI surface covers request latency and buffer occupancy (`parqtel-server/src/otel_sli.rs`, `main.rs:396-410` flush tick publishing saturation gauges) but there is **no** measurement of: time spent waiting on the ingest mutex, time waiting on the index write lock, flush queue depth, in-flight flush count, or tokio blocking-pool saturation. `Scanner::MAX_CONCURRENT = 16` (`parqtel-core/src/storage/scanner.rs:15`) and the blocking pool default are likewise unobserved.

**Gap.** Every contention finding in BL-01-01, BL-03-01, BL-03-02 and BL-04-04 is currently only provable from source or pprof. Without lock-wait histograms the fixes in Phase 1 cannot be validated in production, and a regression re-introducing a global stall would not page anyone.

**Resolution.**
- Instrument `rotator.lock().await` with a `Instant`-measured wait histogram, tagged by signal.
- Instrument the index write-lock acquisition in the index task.
- Export `parqtel_ingest_lock_wait_seconds{signal}`, `parqtel_index_lock_wait_seconds`, `parqtel_flush_inflight`, `parqtel_flush_queue_depth`, `parqtel_blocking_tasks_active`.
- Use a `try_lock` fast path in the hot ingest path where safe, to avoid queueing behind a flush at all.

**Acceptance.** A deliberate flush-stall injection produces a visible `parqtel_ingest_lock_wait_seconds` spike; the Phase 1 exit criterion ("max global stall < 100 ms") is verifiable from `/metrics`.

**Effort** S · **Risk** Low

---

## BL-04-02 (M) — Query timeout and body-size limits are applied to ingest and static routes

**Evidence** — `parqtel-server/src/router.rs:157-163` layers the **whole** router with:
```rust
.layer(TraceLayer::new_for_http())
.layer(TimeoutLayer::new(Duration::from_secs(query_config.timeout_secs)))  // 30 s
.layer(RequestBodyLimitLayer::new(ingest_config.max_body_size))            // 10 MiB
```
`query_config.timeout_secs` defaults to 30 (`parqtel-core/src/config/query.rs:28`) and `ingest_config.max_body_size` to 10 MiB (`config/ingest.rs:57`).

**Gap.** (a) A slow ingest path manifests as a query-timeout error rather than as backpressure — wrong signal for the operator and wrong remedy. (b) The 10 MiB body limit also applies to `/ui` and `/oas`, which is meaningless but harmless; the real problem is that ingest has **no** limit of its own and queries have no limit of theirs. (c) There is no concurrency limit anywhere, so overload queues instead of shedding.

**Resolution.**
- Scope `TimeoutLayer` to the query routes; give ingest its own (much larger or absent) timeout.
- Add `ConcurrencyLimitLayer` + `LoadShedLayer` on ingest routes, returning 429/503 with `Retry-After` when saturated (pairs with BL-01-01's flush semaphore and BL-03-12's WAL).
- Apply `RequestBodyLimitLayer` only where a body is expected.

**Acceptance.** Sustained overload produces 429/503 within one RTT rather than a 30 s timeout wall; query routes still bounded by `query.timeout_secs`.

**Effort** S · **Risk** Low

---

## BL-04-03 (M) — gRPC server has no message-size, concurrency or timeout configuration

**Evidence** — `parqtel-server/src/grpc.rs:46-52`:
```rust
tonic::transport::Server::builder()
    .add_service(MetricsServiceServer::new(svc.clone()))
    .add_service(LogsServiceServer::new(svc.clone()))
    .add_service(TraceServiceServer::new(svc))
    .serve(addr).await
```
No `max_decoding_message_size` (tonic default 4 MiB — inconsistent with the HTTP 10 MiB limit), no `max_encoding_message_size`, no `concurrency_limit_per_connection`, no `timeout`, no load shedding. All three services share one `AppState` and therefore the same three ingest mutexes as HTTP.

**Gap.** A 6 MiB OTLP batch succeeds over HTTP and fails over gRPC with an opaque `ResourceExhausted`, which is a confusing operational failure on the default production port. Unbounded per-connection concurrency means one client can monopolise the shared ingest mutex.

**Resolution.** Set `max_decoding_message_size`/`max_encoding_message_size` to match `ingest.max_body_size`; add `concurrency_limit_per_connection` and a per-method timeout; share a single ingest admission semaphore between HTTP and gRPC so neither path can starve the other. Also enable TCP keepalive and consider `http2_keepalive_interval` for long-lived exporters.

**Acceptance.** A 9 MiB batch is accepted identically over gRPC and HTTP; a single connection cannot exceed the configured concurrency limit.

**Effort** S · **Risk** Low

---

## BL-04-04 (M) — Scanner concurrency is bounded per query, not globally

**Evidence** — `parqtel-core/src/storage/scanner.rs:15` `const MAX_CONCURRENT: usize = 16;` with a **new** `Semaphore` created inside each scan call (`:60`, `:263`, `:409`). `MAX_BLOCKS = 128` (`:17`) caps work per query.

**Gap.** N concurrent queries admit 16N blocking tasks. Tokio's default blocking pool is 512 threads, so ~32 concurrent queries exhaust it — and an exhausted blocking pool also stalls the ingest `spawn_blocking` flushes (`parqtel-ingest/src/service.rs:73`), coupling query load to ingest stalls. The per-query semaphore also means a single query can occupy all 16 slots, so 15 other queries queue behind it rather than sharing.

**Resolution.** Hoist the semaphore to a process-wide `static`, sized from config, and use a **lower** separate budget for ingest flushes (flushes must never be starved by queries). Additionally consider a weighted/fair queue so one query cannot monopolise the pool.

**Acceptance.** With 30 concurrent range queries, ingest flush p99 latency is unaffected; blocking-pool active count stays below the configured limit.

**Effort** S · **Risk** Low

---

## BL-04-05 (L) — Synchronous `/proc` reads on the async runtime

**Evidence** — `parqtel-server/src/metrics.rs:477-494` (`get_rss`) is called from `ServerMetrics::render` (`:412`) inside the `/metrics` handler; `otel_sli.rs:219-232` (`read_vm_rss_hwm`) runs every 5 s from `record_gauges` on the flush task.

**Gap.** Small, but blocking file I/O on a runtime worker, executed on every Prometheus scrape. Under a 1 s scrape interval with a handful of instances this is avoidable syscall + parse work on the hot path.

**Resolution.** Cache RSS/VM values, refreshed in the existing 5 s background tick (`main.rs:396`), and have both `/metrics` render and the gauge recorder read the cached atomics.

**Acceptance.** `/metrics` handler performs zero file I/O (assert in a test using a `#[test]` that renders with an unwritable `/proc`).

**Effort** S · **Risk** Low

---

## BL-04-06 (L) — Duplicate per-request telemetry work and allocations

**Evidence**
- `parqtel-server/src/otel_sli.rs:143-152` — `record_http` allocates `method.to_string()`, `route.to_string()`, `status.as_u16().to_string()` and then **clones all three `KeyValue`s again** for the histogram. `MatchedPath` → `String` clone at `:254-258`.
- `router.rs:159` adds `TraceLayer::new_for_http()` on top of the SLI middleware (`router.rs:165-167`), producing a second per-request span and latency record.
- `metrics.rs:394` takes a `std::sync::Mutex<Histogram>` to render `query_duration_ms` — correct (render-only), noted so it is not moved onto the ingest path later.

**Gap.** 6+ allocations per HTTP request including ingest. Cardinality is well controlled (matched route is used, `otel_sli.rs:245-248`), so this is cost, not risk.

**Resolution.** Cache static `KeyValue`s (`method`, matched route) in a `OnceLock`/`DashMap`; reuse a single owned attribute slice; drop `TraceLayer` in favour of the SLI middleware (which already records count + latency for the whole request including inner layers).

**Acceptance.** Zero heap allocation in `record_http` for cache-hit routes; one span per request instead of two.

**Effort** S · **Risk** Low

---

## BL-04-07 (M) — Tokio runtime is not sized or tuned

**Evidence** — `#[tokio::main]` at `parqtel-server/src/main.rs:80` with no builder configuration; `tokio = { features = ["full"] }` (`Cargo.toml:29`). No `worker_threads`, no `max_blocking_threads`, no `global_queue_interval`, no `event_interval`. Bind is a bare `TcpListener` (`main.rs:555`) with `axum::serve` (`:594`) and no `http1_keepalive`/`http2`/TCP-nodelay tuning.

**Gap.** On a container with a CPU quota smaller than the host core count, the default worker count (host CPUs) oversubscribes the quota and increases context-switch latency. `max_blocking_threads` is left at 512, which interacts badly with BL-04-04.

**Resolution.** Build the runtime explicitly: `worker_threads = max(2, available_parallelism())`, `max_blocking_threads` from config, `global_queue_interval` tuned if profiling shows a need. Add TCP_NODELAY (axum sets it by default — verify) and explicit keepalive. Document the reasoning in `docs/PERFORMANCE_SIZING.md`.

**Acceptance.** Documented sizing guidance; no measurable regression on the 2-core CI runner.

**Effort** S · **Risk** Low

---

## BL-04-08 (M) — No result cache and no query parse cache

**Evidence** — every request re-parses the PromQL expression (twice, per BL-02-19h) and re-executes the full scan+evaluate pipeline. `docs/benchmarks/PERFORMANCE.md` already lists "add a short-TTL cache if UI traffic makes them hot" for `get_log_field_values`/`list_label_values`.

**Gap.** Dashboards re-issue identical queries every refresh interval; the UI polls autocomplete endpoints. With Phase 2's work in place a repeat query becomes cheap, but a short-TTL cache still removes duplicate work and smooths bursts.

**Resolution.**
- LRU + short TTL (default 1–5 s) keyed by `(signal, canonical query, start, end, step)` for `/api/v1/query*`, invalidated on flush.
- Separate, longer-lived cache (30–60 s) for `/api/v1/label/:name/values`, `/api/v1/labels`, `/v1/logs/field_values`.
- Parse cache for the AST (`Expr`) keyed by query string, eliminating the double parse and repeated `Regex::new` for `=~`.
- Make all TTLs configurable and add hit/miss gauges.

**Acceptance.** Repeated identical dashboard queries ≥ 90 % hit rate at a 15 s refresh with a 5 s TTL; parse-cache miss count per request ≤ 1.

**Effort** M · **Risk** Medium (staleness semantics must be documented)

---

## BL-04-09 (M) — No ingest backpressure or load shedding

**Evidence** — every ingest handler accepts unconditionally; there is no concurrency limit, no queue-depth guard, and the only backpressure is the implicit one from the ingest mutex (BL-01-01), which manifests as latency rather than as rejection. `ingest.max_body_size` (`config/ingest.rs:57`) is the only limit.

**Gap.** Under overload the system degrades by stalling every request (including health checks and queries) rather than by shedding load. Clients have no signal to back off, and the OOM risk from BL-01-03 has no early warning.

**Resolution.** Add an ingest admission semaphore with `try_acquire`; return 429/503 with `Retry-After` when full (with BL-04-02). Force a flush (rather than reject) when the buffer is near its bound, and reject only when the flush cannot keep up. Expose queue depth, rejection count and buffer fill ratio on `/metrics` and as SLI saturation gauges.

**Acceptance.** Overload test (2× capacity for 60 s) shows: ingest rejections > 0, query p99 unchanged, RSS flat, recovery to full acceptance within 10 s of load removal.

**Effort** M · **Risk** Low

---

## BL-04-10 (M) — No performance regression gate in CI

**Evidence** — CI (`.github/`) runs fmt, clippy, tests, MSRV, `cargo-audit`, Trivy, Helm lint, Docker build + smoke test (health + `/metrics`). The benchmark harness exists (`parqtel-server/examples/perf_bench.rs`, `parqtel-query/examples/bench_label_values.rs`) and its results are recorded in `docs/benchmarks/PERFORMANCE.md` (baseline_before.txt / results_after.txt), but **nothing runs it automatically**.

**Gap.** Every item in this backlog assumes before/after numbers. Without a gate, a future change can silently reintroduce a per-row regex compile or a lock held across I/O and nothing catches it.

**Resolution.**
- Add a `make perf-gate` target running the harness in release on a fixed seeded dataset, with generous thresholds (e.g. fail at > 25 % regression on scan/query/ingest medians) to avoid flaky CI on shared runners.
- Add micro-regression unit tests that assert *structural* properties rather than timings: no `Regex::new` inside a row loop (counting wrapper), `row_group_size <= max_rows_per_block` config validation, no blocking syscall in the index save path, `spawn_blocking` used by compaction.
- Record the run in the PR as a comment.

**Acceptance.** A deliberately reintroduced regression (e.g. moving `Regex::new` back into `log_matches`) fails CI.

**Effort** M · **Risk** Low (threshold tuning may take a few iterations on shared runners)

---

## BL-04-11 (L) — `total_series` counts points, not series

**Evidence** — `parqtel-query/src/executor.rs:262` (and the legacy equivalents) increments `total_series += 1` **inside the per-point loop**, and `matched_series_fps.insert(fp)` (`:384`, `:475`) is likewise per point. The value is reported in the `tracing::debug!` and the `max_series` guard.

**Gap.** `query.max_series` (`config/query.rs:25`, default 1000) is therefore enforced against a point count, so a single series with many samples triggers the limit while many distinct series do not. It also makes the emitted series-count metric/trace field wrong by an order of magnitude.

**Resolution.** Count distinct fingerprints (the `series_map` already keys on them — count insertions, not points); remove the redundant `matched_series_fps` set (BL-02-05). Verify the truncation semantics still bound memory.

**Acceptance.** A 1-series/10 000-point query is not rejected by `max_series`; a 2 000-series query is. Covered by a unit test.

**Effort** S · **Risk** Low (behaviour change in the guard — announce it)

---

## BL-04-12 (M) — Trace index path hardcoded in `QueryExecutor` constructors

**Evidence** — three `QueryExecutor` constructors hardcode `/tmp/parqtel-traces`:
```rust
let trace_index = Arc::new(RwLock::new(BlockIndex::new(std::path::Path::new("/tmp/parqtel-traces"))));
```
(`parqtel-query/src/executor.rs:32-34`, `:54-56`, `:92-94`; four constructors with overlapping parameters). `main.rs:201` uses `config.storage.data_dir.join("traces")`.

**Gap.** Trace index data is written to ephemeral `/tmp` by any embedder that constructs a `QueryExecutor` directly, so trace block metadata does not survive a restart and is not configurable — while the server binary uses the correct data dir. This is the same class of defect as the trace-path issue already recorded in the root `ARCHITECTURE_ANALYSIS_REPORT.md` (Issue 1); it is restated here because it is still open in `main`.

**Resolution.** Consolidate to a single constructor taking a config struct (or `&Config`) and derive every path from it; delete the duplicated constructors.

**Acceptance.** No literal path outside config; an embedder's trace index survives restart.

**Effort** S · **Risk** Low

---

## BL-04-13 (L) — Alert and pipeline evaluation windows and intervals are hardcoded

**Evidence** — `parqtel-server/src/main.rs:285-286` uses a fixed 300 s lookback for alert evaluation (`now_ns - 300_000_000_000`); the alert evaluation interval is a hard-coded 15 s (`main.rs:487`); the background flush tick is a hard-coded 5 s (`main.rs:396`); the retention sweep interval is a hard-coded 3 600 s (`parqtel-core/src/storage/retention.rs:14`).

**Gap.** Rules that need a longer or shorter window cannot be expressed, and the two intervals that most affect latency and cost (flush cadence, retention sweep) are not operator-tunable — which directly blocks the Phase 1 exit criterion of moving to short blocks (BL-01-04).

**Resolution.** Add `evaluation_window_secs` to the alert rule/config, and expose `flush_interval_secs`, `alert_interval_secs`, `retention_interval_secs` in config with the current values as defaults.

**Acceptance.** All four intervals settable via env/TOML; documented in `docs/CONFIGURATION.md`.

**Effort** S · **Risk** Low

---

## BL-04-14 (L) — Dead / unused config surface

**Evidence**
- `server.max_connections` (default 1024) and `server.shutdown_timeout_secs` (default 30) at `parqtel-core/src/config/server.rs:13`, `:15`, `:27-28` are never read anywhere in the workspace (only asserted in `config/mod.rs:250-251`).
- `parqtel-core/src/storage/mod.rs` / `query/plan.rs` / `query/pipeline.rs` retain partially-unused abstractions; `ServerExtension` (`parqtel-server/src/router.rs:18-21`) is defined but unused; `RetentionConfig` (`config/storage.rs:93-94`) is a retained-for-compatibility stub.

**Gap.** Operators set these and get no effect — a silent misconfiguration class. Dead abstractions mislead contributors about extension points.

**Resolution.** Either implement (`max_connections` → a connection-counting acceptor / `ConcurrencyLimitLayer`; `shutdown_timeout_secs` → bound the graceful-shutdown await at `main.rs`) or delete them, and fail config validation on unknown keys so typos surface at startup. Remove `ServerExtension` and `RetentionConfig`, or document the intended use.

**Acceptance.** `cargo run -- parqtel serve --config bad.toml` fails on an unknown key; no config field in the workspace is unread.

**Effort** M · **Risk** Low (unknown-key strictness is a breaking config change — gate behind a flag for one release)

---

## Cross-cutting: measurement plan

The items above cannot be validated individually without a stable measurement setup. Sequence it first (Phase 0):

1. **Fix the harness seed and shape.** `perf_bench.rs` currently uses 12 blocks × 50k points × 100 series (`docs/benchmarks/PERFORMANCE.md`). Add a second, larger shape — 200 series × 24 h at 15 s — plus a high-cardinality shape (10k distinct label values) and a concurrent-load shape (8 workers + 4 queries). Report ingest/scan/query medians **and** peak RSS.
2. **Make the harness emit a machine-readable baseline** (`results.json`) that CI can diff, and append before/after numbers to `docs/benchmarks/PERFORMANCE.md` as the backlog's Definition of Done requires.
3. **Stand up the pprof workflow** in the docs — the dependency is already wired (`Cargo.toml`, `pprof` with `prost-codec`, no `protoc` needed); document the exact `go tool pprof` invocation for CPU, heap, and block profiles.
4. **Add a load-test profile that runs each shape for ≥ 10 minutes** so that compaction and retention cycles (hourly by default) are actually exercised — short benchmarks will miss every BL-03-02 class of stall.
