# Storage Plan: Small-Block Flush + 30-Minute Background Compaction

**Status:** Proposed · **Owner:** Platform/SRE · **Scope:** `parqtel-core`, `parqtel-ingest`, `parqtel-server`, charts
**Related incident:** oman-stg `parqtel` pod OOMKilled loop (5 restarts in 30 min) while ingesting ~350 pts/sec from the OpenTelemetry Collector.

---

## 1. Problem statement

Parqtel holds every ingested metric point in the in-process `MemoryBuffer` until a
**time-based** flush fires. With the default `block_duration_secs: 7200`, a cluster-wide
OTLP feed (k8s_cluster + kubeletstats + prometheus SD + app OTLP ≈ 350 pts/sec)
accumulates **~2.5M points (~950 MiB estimated at ~4 KiB/point) in RAM before the first
flush ever runs** — a 1 GiB-limited pod can never get there, so it OOM-loops and drops
ingest windows.

The obvious hotfix — flush every 5 minutes — bounds memory but creates **12 blocks/hour
× 24h = 288 files/day per signal**, which degrades scanner open-file cost, index size,
and compaction churn.

**Goal:** flush fast (memory-optimised) and compact small blocks in the background into
~30-minute blocks (object-count-optimised), with the whole pipeline bounded by
configuration rather than by ingest rate.

## 2. Current-state analysis (as-built, with file references)

| Component | File | Behaviour today | Gap |
|---|---|---|---|
| MemoryBuffer | `parqtel-core/src/buffer.rs` | Holds metrics/logs/spans until drained on flush; `DataPoint` carries a full `LabelSet` per point (serialised to JSON at write time in `models/storage/writer.rs:57`) | No size accounting, no pressure trigger; ~4 KiB/point observed |
| Flush trigger | `parqtel-ingest/src/service.rs` (`BlockRotator::check_and_flush`, `IngestionService::check_and_flush`) + 5 s ticker in `parqtel-server/src/main.rs:303-318` | **Time-only**: fires when `elapsed >= block_duration_secs` | No row-count/byte high-water mark; `max_rows_per_block` is never consulted at flush time |
| Flush defaults | `parqtel-core/src/config/storage.rs:35` | `block_duration_secs: 7200`, `max_rows_per_block: 1_000_000` | 2 h horizon incompatible with 1 GiB-class deployments |
| Compactor | `parqtel-core/src/storage/compactor.rs` (`Compactor::run_loop`, `compact_once`, `compact_tiered`) | Wired via `storage::start_maintenance` (`storage/mod.rs:16-19`, called from `main.rs:229-230`) for metrics + logs | ① `compact_once` hardcodes `row_count < 10_000` and fan-in ≤ 8 — **5-min blocks (~100k rows) never qualify, so no small-block compaction happens at scale**; ② warm/cold tiering only kicks in at >6 h / >24 h — no hot 30-min tier; ③ `read_source_blocks` decodes **all** selected blocks into RAM before writing (fine at 8×10k rows, unsafe for 6×100k-row blocks) |
| Traces | `parqtel-server/src/main.rs:206-214` | Trace `BlockIndex` created and its write-back task spawned, but `start_maintenance` is **not** called for the trace index | Trace blocks never compacted or retention-cycled |
| Observability | `handlers/misc.rs` `/api/v1/stats` | Exposes buffer counts (points/logs/spans) and storage footprint | No estimated buffer bytes, no compaction lag, no block-count-per-signal — the OOM loop was only visible via pod restarts |

## 3. Design

### 3.1 Tiered block lifecycle (target state)

```
 ingest ──► MemoryBuffer ──flush (time OR pressure)──► L0 blocks (≤5 min span)
                                                        │
                 compaction hot tier (every pass) ──────┤  merge adjacent, combined span ≤ 30 min
                                                        ▼
                                                  L1 blocks (~30 min span)
                                                        │  age > 6 h  (existing warm tier)
                                                        ▼
                                                  L2 blocks (~6 h span)
                                                        │  age > 24 h (existing cold tier)
                                                        ▼
                                                  L3 blocks (~24 h span) ──► retention delete
```

Steady state at 350 pts/sec: 12 L0 files/hour are folded into **2 L1 files/hour**;
older tiers collapse further. With `retention_days: 7` the block inventory stays in the
low hundreds — comparable to today, with a bounded memory footprint.

### 3.2 Feature A — pressure-based flush (memory bound)

* **Config** (new fields on `BlockConfig`/`LogBlockConfig`, serde defaults keep TOML back-compat):
  * `flush_max_points: usize` (metrics default **100_000**) — flush when buffered metric points ≥ this.
  * `flush_max_rows: usize` (alias of `max_rows_per_block`, actually enforced at flush time).
  * `flush_max_bytes: u64` (default **256 MiB**) — flush when `estimate_bytes() ≥ this`.
  * `block_duration_secs` stays as the *minimum* horizon (default lowered **7200 → 300**).
* **Implementation**
  1. `MemoryBuffer` gains `point_count()` (cheap: sum of `Vec::len` under one read lock) and
     `estimate_bytes()` (sum of `DataPoint` shallow size + label string lens; called at most
     once per 5 s tick, not per point).
  2. `IngestionService::check_and_flush` (`parqtel-ingest/src/service.rs:249-253`) becomes:
     `flushed = elapsed ≥ block_duration_secs || buffer.point_count() ≥ flush_max_points
      || buffer.estimate_bytes() ≥ flush_max_bytes`.
  3. Row-count enforcement moves into the flush: if the drained slice exceeds
     `max_rows_per_block`, write **multiple Parquet files per flush pass** (existing
     `StorageModel::write` supports chunked writer calls) so a single file never exceeds the row cap.
* **Effect:** memory ceiling becomes `flush_max_bytes` + encode overhead, independent of
  `block_duration_secs`; the 7200→300 default change alone already cuts the oman-stg peak
  from ~950 MiB to ~400 MiB, and the pressure trigger caps it regardless of burstiness.

### 3.3 Feature B — duration-aware hot compaction (30-min blocks)

Rework `compact_once` into `compact_hot(index, config)`:

* **Selection** — same-signal, adjacent (non-overlapping, gap ≤ `merge_max_gap_secs`, default 120 s),
  only blocks whose `end_timestamp_ns` is at least `compaction_min_age_secs` (default 120 s)
  old (never touch a block a concurrent flush may still append to), sliding window:
  keep adding next block while `combined_span ≤ target_block_duration_secs` (new config,
  default **1800**) AND `combined_rows ≤ max_rows_per_block`.
* **Fan-in cap**: at most `max_merge_fan_in` (default 16) source blocks per pass; more remains
  for the next tick — bounds the compaction working set.
* **Trigger**: run when ≥ `min_blocks_to_merge` (default 4) candidates exist, i.e. roughly every
  4×5-min flushes fold into one 30-min block. With `compaction_interval_secs: 300` (lowered from
  3600) this reaches steady state within one hour of startup.
* **De-dup safety**: if the same series+timestamp appears in overlapping blocks (retry/replay),
  last-write-wins by ingestion order as today; no behavioural change to reads.
* Replace the hardcoded `10_000` / `8` constants in `compact_once` with the config knobs above;
  keep the function name as an alias for the existing tests.

### 3.4 Feature C — streaming merge (constant-memory compaction)

`read_source_blocks` (compactor.rs:185) decodes every selected point into
`Vec<(String, MetricKind, LabelSet, DataPoint)>`. For 6 × 100k-row sources that is
~600k points decoded + re-encoded in one pass — the same RAM problem we are fixing at ingest.

* Merge via **Arrow record batches**: open each source with `ParquetRecordBatchReaderBuilder`
  (already imported), project to the canonical schema (`schema.rs`), sort-merge by
  `timestamp_ns` per row group, and feed `ArrowWriter` incrementally.
* Peak memory ≈ one row group per source (`row_group_size`, default 100k rows ≈ ~40 MiB with
  dictionary columns) + writer buffers — flat, not proportional to total merged rows.
* `write_merged` (compactor.rs:232) gains a streaming variant; the point-decoding path is kept
  for logs (small rows) and as a fallback behind `compaction_stream_merge: bool` (default true).
* Cross-check with AGENTS.md constraint: arrow/parquet 59 is already the stack; no new deps.

### 3.5 Feature D — traces maintenance parity

Call `start_maintenance(trace_index.clone(), config.traces.clone().into())` in
`main.rs` after the trace index is built (mirroring metrics/logs at :229-230). Also extend
`LogBlockConfig→BlockConfig` conversion (`config/storage.rs:78-91`) so it preserves the new
fields (add them there too).

### 3.6 Feature E — compaction-aware observability

Extend `/api/v1/stats` (`handlers/misc.rs`) and the embedded UI storage panel with:

* `buffer.points`, `buffer.estimated_bytes` (Feature A accounting),
* per-signal `blocks.count`, `blocks.smallest_rows`, `blocks.l0_pending` (count below hot-tier target),
* `compaction.last_run_at`, `compaction.last_merged_blocks`, `compaction.last_error`,
* alert-ready gauge `parqtel_buffer_estimated_bytes` emitted through Parqtel's own `/metrics`
  endpoint (self-instrumentation), so the oman-stg failure mode becomes a firing alert
  (`buffer_bytes > 0.7 × flush_max_bytes for 5m`) instead of a CrashLoopBackOff discovery.

## 4. Configuration surface (proposed defaults)

```toml
[storage]
block_duration_secs        = 300          # was 7200 — flush at least every 5 min
flush_max_points           = 100_000      # pressure trigger #1
flush_max_bytes            = 268_435_456  # 256 MiB — pressure trigger #2
target_block_duration_secs = 1800         # hot-tier compaction target
compaction_interval_secs   = 300          # was 3600
compaction_min_age_secs    = 120
merge_max_gap_secs         = 120
max_merge_fan_in           = 16
min_blocks_to_merge        = 4
compaction_stream_merge    = true

[logs]
block_duration_secs        = 300          # was 1800
target_block_duration_secs = 1800
```

Environment overrides follow the existing Figment mapping
(`PARQTEL__STORAGE__BLOCKDURATIONSECS=300`, etc.), and the Helm chart exposes them under
`parqtel.storage.*` (`charts/parqtel/values.yaml`, templated into the ConfigMap consumed as
`PARQTEL_CONFIG`).

**Sizing note (oman-stg geometry):** at 350 pts/sec, a 5-min L0 block is ~105k rows; a
30-min L1 block is ~630k rows (< `max_rows_per_block` 1M, one row group). Peak RSS budget:
~256 MiB buffer ceiling + ~64 MiB compaction working set + ~120 MiB engine baseline ≈ **440 MiB**,
comfortably inside the 1 GiB limit. Live measurements superseding this estimate — including
the measured metrics-to-memory ratio and the OOM-avoidance sizing formula — are maintained in
[`docs/PERFORMANCE_SIZING.md`](PERFORMANCE_SIZING.md).

## 5. Implementation plan (ordered, testable increments)

| Phase | Deliverable | Files | Tests | Risk |
|---|---|---|---|---|
| 0 — hotfix (no code) | Set `block_duration_secs=300` (metrics + logs) in `deploy/k8s/overlays/oman-stg/values.yaml` + helm upgrade on oman-stg | values only | live: buffer drains every ≤5 min; pod RSS plateaus < 600 MiB | Low — pure config |
| 1 — pressure flush | `point_count`/`estimate_bytes` on `MemoryBuffer`; trigger logic in `IngestionService`; multi-file flush per pass | `buffer.rs`, `ingest/service.rs`, `config/storage.rs` | unit: trigger matrix (time/points/bytes); integration: 120k-point burst → 2 files, neither > max_rows | Low |
| 2 — hot compaction | `compact_hot` with config knobs; delete hardcoded constants | `storage/compactor.rs`, `config/storage.rs` | unit: adjacency/span/fan-in selection table cases; integration: 24 L0 blocks → 2 L1 blocks, `index.json` consistent, source files deleted | Medium — selection math |
| 3 — streaming merge | RecordBatch sort-merge path behind `compaction_stream_merge` | `storage/compactor.rs`, `models/storage/schema.rs` | property: merged output row set == union of inputs (sorted, deduped); memory assertion via allocator counter in a bench | Medium |
| 4 — traces + UI | `start_maintenance` for trace index; stats/UI fields; self-metric gauge | `server/main.rs`, `handlers/misc.rs`, `ui.html` | e2e: ingest traces for 2×interval → trace block compacted | Low |
| 5 — soak & rollout | 30-min soak at 350 pts/sec in CI (docker compose load-generator); chart defaults; docs | `charts/`, `docker-compose.yml`, `docs/CONFIGURATION.md`, `docs/ARCHITECTURE.md` | RSS stays within budget; block inventory < 150 files after 6 simulated hours (accelerated clock) | Low |

Phases 1–3 are independent enough to ship sequentially; each keeps the previous behaviour as
the fallback (config defaults flip only in Phase 5 charts/docs).

## 6. Query-correctness gates (must not regress)

* **Drain-on-flush invariant**: buffer is emptied exactly when a block is written and indexed —
  no double counting (existing invariant, covered by `service.rs` tests; re-run for multi-file flush).
* **Instant query window**: `/api/v1/query` uses a 5-minute lookback (`query.lookback_delta_ns`). With a 300 s flush
  horizon, a series that stops sending at T is visible from the buffer until T+300 s and from
  L0/L1 blocks after the drain — add a regression test that an instant query at T+6 min still
  resolves a series via flushed blocks.
* **Range queries across tiers**: the scanner already reads all blocks in the index + buffer;
  add a test spanning L0→L1→L2 boundaries (no gaps, no duplicates).
* **Crash safety**: the current flush path is tmp+rename + atomic `index.save()`; compaction keeps
  the same ordering (write merged → index swap → unlink sources). Add a test that a compaction
  crash between swap and unlink leaves a valid index (orphan file cleaned by the next pass).

## 7. Rollout

1. **oman-stg** (now): Phase 0 config hotfix + collector interval tuning already applied.
2. Feature branch `feat/small-block-compaction`, phases 1→5, `make lint && make test` per phase.
3. Enable on **oman-stg** with the new config keys; soak 24 h; watch
   `parqtel_buffer_estimated_bytes` and the block inventory.
4. Fold defaults into `charts/parqtel` values + `docs/CONFIGURATION.md`; release as part of the
   next minor (chart version bump; note in CHANGELOG that `block_duration_secs` default changed).
5. Ops note for upgraders: existing data files remain readable (arrow 59 stack unchanged by this
   plan); the first compaction pass after upgrade will fold any legacy small blocks into the
   hot tier automatically.

## 8. Alternatives considered

* **Ingest-side batching at the collector** (what we did live on oman-stg): reduces burstiness
  but cannot bound a 2 h in-memory horizon — necessary, not sufficient.
* **Disk-backed buffer / WAL** (spill to a log file, replay on flush): strictly better memory
  behaviour but a much larger change to the ingest path; the flush-to-small-blocks design gets
  the same bound with existing Parquet machinery.
* **Huge single-block flush with external sort** (e.g. 30-min buffer horizon): worst memory
  profile of all options — rejected.
* **Row-count-only flush**: ignores label-heavy low-row-count workloads; byte estimate covers it.
