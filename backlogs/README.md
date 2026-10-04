# Parqtel Performance & Storage Optimization Backlog

**Owner:** Principal Architect
**Date:** 2026-10-03
**Scope:** `parqtel-core`, `parqtel-ingest`, `parqtel-query`, `parqtel-server`
**Goal:** sub-second ingest-to-query latency, predictable tail latency at 100k+ points/sec, and materially better storage efficiency.

---

## 1. Backlog documents

| Doc | Domain | Items | Theme |
|-----|--------|-------|-------|
| [BL-01-ingest-throughput.md](./BL-01-ingest-throughput.md) | Ingestion hot path | 15 | Lock serialization, allocator pressure, batching, backpressure |
| [BL-02-query-latency.md](./BL-02-query-latency.md) | Query execution | 21 | Label cloning, per-step work, single-thread evaluation, pushdown |
| [BL-03-storage-optimization.md](./BL-03-storage-optimization.md) | Storage format & lifecycle | 15 | Parquet layout, bloom filters, series encoding, index, compaction |
| [BL-04-runtime-observability.md](./BL-04-runtime-observability.md) | Runtime, middleware, config | 14 | Scheduler hygiene, limits, caching, CI perf gates |

**Total: 65 items** — 13 Critical/High, 30 Medium, 22 Low.

---

## 2. Executive summary

Parqtel already contains a solid round of hot-path work (non-blocking flush, row-group statistics pruning, per-chunk label caching, content-negotiated OTLP routing — see `docs/benchmarks/PERFORMANCE.md`). What remains is best described by three structural findings:

### Theme A — The system is allocation-bound, not compute-bound
`LabelSet` is a `BTreeMap<String, String>` (`parqtel-core/src/models/labels.rs:11-15`) and it is **deep-cloned at every stage boundary**:

```
decode (LabelSet::try_from_iter)  →  buffer injection (labels.clone + merge)  →
memory buffer (clone)             →  rotator (move)                          →
flush grouping (BTreeMap key clone + dp.clone)  →  Arrow row (to_json per row) →
scan (LabelSet::from_json)        →  executor grouping (labels.clone)        →
evaluator (labels.clone per step) →  group key (2 String per label)          →
response (serde_json::Map per series + String per sample)
```

A single-entry `BTreeMap` costs roughly one leaf-node allocation (~500 B) plus two `String` allocations. A modest `sum by (x) (rate(m[5m]))` over 1 000 series × 1 000 steps spends **tens of millions** of allocations on label copies alone. Resolving this once (interned, `Arc`-shared, immutable label sets + fingerprint-keyed grouping) fixes the largest share of items across BL-01, BL-02 and BL-03.

### Theme B — Blocking work sits on the async runtime, and locks are held across it
`spawn_blocking` is used correctly in `BlockRotator::flush` (`parqtel-ingest/src/service.rs:73`) and in the scanner (`parqtel-core/src/storage/scanner.rs:69`). It is **missing everywhere else in the storage subsystem**:

- `BlockIndex::add`/`remove` re-serialize the whole index and `fs::write` + `rename` **inside the shared write lock** (`parqtel-core/src/storage/index.rs:33-52`), invoked from `main.rs:197-216` with no `spawn_blocking`.
- The entire compactor read/merge/write/delete cycle is synchronous (`storage/compactor.rs:78`, `:94`, `:105`, `:332-370`).
- Retention holds the index write lock across `idx.save()` and every `fs::remove_file` (`storage/retention.rs:27-54`).
- OTLP decode runs inline on a worker thread (`parqtel-ingest/src/service.rs:180-205`), and the JSON path first materialises a full `serde_json::Value` DOM.

Result: multi-second periodic stalls that hit every endpoint, not just the one that triggered them.

### Theme C — Query cost scales with (steps × series × labels), and the whole result set is resident
`eval_steps` retains every step's `InstantVector` (`parqtel-query/src/eval.rs:93-114`) before conversion (`executor.rs:308-326`); each instant vector owns a cloned `LabelSet` per series. Aggregations build a `Vec<(String, String)>` group key per series per step and clone it twice more (`eval.rs:682-702`). Matchers are evaluated **per data point** rather than per series (`executor.rs:249`). The entire evaluator is single-threaded and runs on an axum worker.

The realistic cost model for the current code at 1 000 series × 1 000 steps is **multiple seconds and ~600 MB live memory for one 4-hour panel** — which is exactly why sub-second latency is not currently reachable. BL-02 is ordered to move that from O(steps × series × labels) allocations to O(series) with parallel execution.

---

## 3. Target service levels

These are the acceptance anchors used by the acceptance criteria in each item.

| Metric | Current (observed/derived) | Target |
|---|---|---|
| Ingest p99 latency (`/v1/metrics`, 10k-point batch) | lock-queued behind flush; ~flush duration at boundary | **< 250 ms** |
| Ingest throughput, single stream | ~550k pts/s decode (bench, no flush wait) | **> 200k pts/s sustained including flush** |
| Max global ingest stall (any endpoint) | seconds (flush, compaction, retention, index save) | **< 100 ms** |
| Instant query p99, 1 000 series × 1 000 steps | seconds (derived) | **< 1 s** |
| Range query p99, 7-day range, narrow metric | tens of ms narrow / seconds wide | **< 1 s** |
| Logs query p99, 50k rows, 3-term search | tens of ms per block (regex per row) | **< 300 ms** |
| Query peak RSS, 4-hour wide panel | ~600 MB estimated (now ~110 MB after `BL-02-01`/`03`) | **< 150 MB** |
| On-disk size vs uncompressed Parquet+JSON labels | baseline | **≤ 60 % (2–5× smaller)** |
| Crash data loss window | up to a full block (no WAL) | **< 5 s** |

Measurement method: `parqtel-server/examples/perf_bench.rs` (release, median of 5, median of 3 process runs) plus `go tool pprof` against the embedded pprof endpoint (already wired, see `Cargo.toml` `pprof` dependency). Any item that claims a speedup must ship with a before/after number in `docs/benchmarks/PERFORMANCE.md`.

---

## 4. Sequencing plan

### Phase 0 — Make the stalls visible and remove the cheap ones (1 week)
Establish that the current pain is where we claim it is, and land zero-risk wins.

- Add lock-wait, flush-duration, index-save-duration and blocking-pool saturation metrics (BL-04-01).
- Land BL-04-11 (`total_series` counting points as series), BL-03-05 (compression level), BL-02-18 (double `min` fold), BL-04-05 (`/proc` caching).
- Add a CI perf gate on the existing benchmark harness with generous thresholds (BL-04-10).

**Exit criteria:** pprof shows the ingest mutex and the index write lock as the top two contention sources.

### Phase 1 — Unblock the async runtime (2–3 weeks)
Removes the seconds-long global stalls. Largely mechanical, low risk, high payoff.

- **BL-03-01** index persistence off the write lock (debounced, off-lock I/O) — *landed, #44*
- **BL-03-02** compaction/retention fully on `spawn_blocking` — *landed, #44*
- **BL-01-05** decode on a dedicated blocking pool; typed JSON instead of `Value`
- **BL-01-11** split batches across the block boundary (no partial push + client error)
- **BL-01-01a** shard the rotator's locks by metric-name hash, merging on flush
  so block count is unchanged — flush stays synchronous
- **BL-02-04a** move the query CPU half onto the blocking pool
- **BL-02-01/02** share label sets behind `Arc`; fingerprint-keyed, cached
  aggregation groups — **6.9×** on the reference workload
- **BL-02-03** stream steps into the result instead of retaining them all —
  168 MB → 110 MB on a 1 000-series × 3 599-step query
- BL-02-04 move the query CPU half into one `spawn_blocking`
- BL-04-07 explicit tokio worker sizing; BL-04-04 global blocking semaphore

**Durability note.** BL-01-01 was deliberately split. The sharded form
(BL-01-01a) removes cross-metric contention while keeping a flush
acknowledged only once it is on disk. The stronger form — releasing the lock
before the encode, so even the flushing bucket does not block — **acknowledges
a request before its data is durable** and is therefore deferred to **BL-01-14,
gated on the WAL (BL-03-12)**. Taking that trade silently would be the wrong
call while `wal_enabled` defaults to `false`.

**Exit criteria:** max global stall < 100 ms; ingest p99 < 250 ms; p99/p50 ingest ratio < 3.

### Phase 2 — Query latency (3–4 weeks)
The largest single latency win available. Requires a type change with a wide blast radius, so it lands as its own sequence.

1. BL-02-01 interned `Arc<LabelSet>` through the evaluator (`InstantVector`/`RangeVector` carry `Arc`)
2. BL-02-02 fingerprint-keyed group keys; single-pass `LabelSet` builders (BL-02-02 also retires the `merge`-per-label pattern everywhere)
3. BL-02-05 matchers evaluated once per series, not per point
4. BL-02-03 streaming step evaluation into the result builder (peak memory O(series))
5. BL-02-04b shard `eval_steps` by series across the blocking pool
6. BL-02-06 precompiled regexes in LogQL and pipeline predicates; BL-02-07/08 allocation-free log matching

**Exit criteria:** instant query p99 < 1 s at 1 000 × 1 000; query RSS < 150 MB; logs query p99 < 300 ms.

### Phase 3 — Storage efficiency (3–4 weeks)
**Note:** the "2–5× smaller files" goal below was an estimate, and measurement
has now disproved the largest item behind it (`BL-03-04`). What Phase 3 actually
delivered is *read* performance — pruning, projection and compaction coverage —
not size. Treat remaining size claims as unverified until measured.

- BL-03-04 series-ID dictionary encoding for `labels` (kills per-row JSON)
- BL-03-03 bloom filters + column/page index + data page limits
- BL-03-07 sort row groups by `(metric_name, service_name, timestamp_ns)`; retune row-group sizes
- BL-03-06 index slimming — **measured**: the sidecar is 23 % of the data it
  describes and grows ~0.3 GB/day, so the premise holds. The fix is a sidecar
  format change worth ~13 % of storage, so it is recorded as a sized proposal
  rather than an instruction.
- BL-03-08 column projection at scan time
- BL-03-09/10 real tiered compaction incl. traces

**Exit criteria:** ≥2× smaller blocks, narrow-query scan cost independent of `row_group_size`, index.json < 20 MB at 30 days retention.

### Phase 4 — Durability, caching, long tail (2 weeks)

- BL-03-12 WAL — **prerequisite for BL-01-14**, so pick this first if the
  async flush worker is wanted sooner
- BL-01-14 async flush worker (only once the WAL can recover an unacknowledged flush)
- BL-04-08 short-TTL result cache + query parse cache
- BL-03-05 (tier-aware codec/level policy: fast codec hot, high level cold)
- BL-04-09 backpressure and load shedding on ingest

**Exit criteria:** crash loss < 5 s; dashboard panel repeat queries < 100 ms.

### Explicitly deferred
- Distributed/remote block storage (S3) — no evidence of need; `StorageEngineRegistry` already provides the seam.
- LSM-style leveled manifests — the current append-then-compact model is adequate at the observed block counts; revisit only if Phase 3 shows more than ~10k blocks in the index.
- Replacing Parquet — not justified; the fixes above recover the required performance inside Parquet.

---

## 5. Risk register

| Risk | Impact | Mitigation |
|---|---|---|
| `Arc<LabelSet>` refactor touches every operator | High | Land behind a feature flag; keep `InstantVector` constructors; conformance suite (`parqtel-query/src/conformance.rs`) gates each step |
| Schema change (series dictionary) breaks existing data dirs | High | Version the schema in `BlockMetadata`; support reading v1 and writing v2; document wipe-or-upgrade |
| Async flush loses crash-safety (response returns before durability) | Medium | Deferred to BL-01-14 behind the WAL (BL-03-12); flushes stay synchronous until then |
| Off-thread drops (BL-01-09) can reorder drain/flush | Low | Drain ordering is already best-effort post-flush; verify no double-count regression in ingest tests |
| Parallel evaluation changes float summation order | Low | Aggregations are per-group; only per-series concat order changes. Assert against conformance fixtures |

---

## 6. Definition of done (per item)

1. Code merged with `make lint` clean (fmt + clippy `-D warnings`, no `unwrap`/`expect`/`panic`).
2. `make test` green; new unit test for the specific pathology (e.g. no regex compiled inside a row loop).
3. Before/after benchmark numbers appended to `docs/benchmarks/PERFORMANCE.md`.
4. Any new config knob documented in `docs/CONFIGURATION.md` and `openapi.yaml` where user-visible.
5. Acceptance criteria in the item met, with the measurement command recorded in the PR description.
