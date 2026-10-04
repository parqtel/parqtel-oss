# BL-03 — Storage Optimization

**Domain:** `parqtel-core/src/storage/*`, `parqtel-core/src/models/storage/*`, `parqtel-ingest/src/writer.rs`
**Goal:** 2–5× smaller blocks, write amplification and read amplification both down, no index/query cost that scales with the retention window, bounded crash-loss window.

Severity: **C** Critical, **H** High, **M** Medium, **L** Low. Effort: S ≤ 2d, M ≤ 1w, L > 1w.

---

## BL-03-01 (C) — Whole index re-serialised and rewritten **inside the shared write lock** on every flush

**Evidence**
- `parqtel-core/src/storage/index.rs:33-39` — `save()` = `serde_json::to_string(&self.blocks)` → `fs::write(tmp)` → `fs::rename`. Called by `add()` (`:42-46`) and `remove()` (`:49-52`).
- `parqtel-server/src/main.rs:197-204` (metrics), `:209-216` (logs), `:248-255` (traces): `while let Some(meta) = rx.recv().await { let mut idx = idx_clone.write().await; idx.add(meta) }` — **no `spawn_blocking`**.
- `BlockIndex::add` also re-sorts the whole `blocks` vector on every insert (`index.rs:44`).
- The payload includes `label_values: BTreeMap<String, BTreeSet<String>>` capped at `MAX_VALUES_PER_FIELD = 10_000` per field (`parqtel-ingest/src/writer.rs:86-90`, populated at `:91-118`).

**Gap.** After **every** block flush the entire index — all blocks, all metric names, all label names, up to 10 000 values per label — is re-serialised to a JSON `String` in memory, written synchronously, and renamed, all while holding the `RwLock` that **every query handler** needs for `index.read()`. That is O(total_index_size) CPU + a blocking syscall pair per flush, on a tokio worker, under the write lock. As block count grows this is quadratic in the retention window, and every concurrent `/api/v1/query*` blocks for the duration.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Mark the index dirty and persist on a **debounce** (1–2 s) or on shutdown, doing the `to_string` + write inside `spawn_blocking`.
- Better: replace the JSON sidecar with an **append-only log** (one `serde_json` line per block add/remove), compacted periodically — O(1) per flush.
- Never hold the index write lock across persistence: take the lock only to mutate the in-memory `Vec`, clone out what needs writing, release, then write.
- Only sort on load or when the vector is known unsorted (or keep a sorted invariant by appending + merge).

**Acceptance.** Query tail latency no longer scales with index size; index persistence is O(1) per flush; `index.json` write duration < 10 ms at 10 000 blocks.

**Effort** M · **Risk** Medium (crash-consistency of the index format — needs a versioned format and a load-time repair path)

---

## BL-03-02 (C) — Compaction and retention run synchronous Parquet/filesystem work on tokio workers, holding the index write lock

**Evidence**
- `parqtel-core/src/storage/mod.rs:16-19` — `start_maintenance` spawns `Compactor::run_loop` and `RetentionPolicy::run_loop`.
- `compactor.rs:34-53` — `run_loop` is `async` but `compact_once` (`:78`) and `compact_tiered` (`:184`, `:189`) call **sync** `read_source_blocks` (`:209-231`: `File::open` + `ParquetRecordBatchReaderBuilder` + full row decode via `StorageModel::row_to_point`) and **sync** `write_merged` (`:256-370`: `create_dir_all`, `File::create`, `ArrowWriter::write/close`, `rename`, `metadata`).
- `compactor.rs:96-102` and `:191-197` — index **write** lock taken, then `idx.save()` inside it; `fs::remove_file` loop at `:104-106` / `:199-201`.
- `retention.rs:27-54` — index write lock held across `idx.save()` (`:50`) **and** every `fs::remove_file` (`:52-54`).

**Gap.** Tokio's multi-threaded runtime has no preemption for synchronous work. A worker stuck in `ArrowWriter::close()` cannot poll any other task assigned to it, so **every** request multiplexed on that worker stalls — not just storage endpoints. On the default `compaction_interval_secs = 3600` (`config/storage.rs:39`) the cycle decodes and re-encodes up to 8 small blocks (or 12 in the tiered pass), potentially many seconds, while holding the write lock that every query needs.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Wrap `read_source_blocks`, `write_merged` and the delete batches in `spawn_blocking` (mirroring `parqtel-ingest/src/service.rs:73`).
- Snapshot the candidate metadata under the **read** lock, release it, do all I/O lock-free, then take the write lock only to swap the `Vec`.
- Add a compaction concurrency limit and make the cycle resumable so an interrupted compaction does not leave orphans.

**Acceptance.** No endpoint observes a stall > 100 ms attributable to compaction or retention; index write-lock hold time < 1 ms.

**Effort** M · **Risk** Medium

---

## BL-03-03 (H, partly landed) — No bloom filters, no column/page index, no page-size limits in the Parquet writer

**Evidence** — `parqtel-ingest/src/writer.rs:400-409` is the whole `WriterProperties` setup for metrics, logs and traces:
```rust
WriterProperties::builder()
    .set_compression(/* codec only */)          // :401-406
    .set_writer_version(WriterVersion::PARQUET_2_0)
    .set_max_row_group_row_count(Some(row_group_size.max(1)))
    .build();
```
No `set_bloom_filter_enabled` on `metric_name` / `service_name` / `timestamp_ns`; no `set_column_index_truncate_length`; no `EnabledStatistics::Page` tuning; no `set_data_page_size_limit`. `parqtel-core/src/storage/scanner.rs` reads **row-group** statistics (the only level written) and nothing finer.

**Gap.** Without bloom filters, every metric-name query must open and decode block footers + row-group metadata for all candidate blocks; the index's `metric_names` filter (`index.rs:67-71`) is the only coarse filter. Without a column index, a narrow row-group read still decodes whole pages. Note the writer also has `parquet = { features = [… "encryption"] }` (`Cargo.toml:31`) — dead weight for this engine that inflates build and binary size.

**Resolution taken.** Bloom filters on `metric_name` and `service_name`
(per-column, not all columns, so nothing pays for a filter nothing reads),
page-level statistics, a 64-byte `column_index_truncate_length` and a 512 KB
`data_page_size_limit` so the column index has pages finer than a row group.
The reader keeps `ArrowReaderMetadata` rather than just the builder, and prunes
row groups by metric name before decoding any page — time statistics alone
cannot help, because a block interleaves every metric it holds.

`PageIndexPolicy::Optional`, not `Required`: blocks written before this change
have no page index and must still be readable.

Measured through the production `Scanner::scan` path, 200 metrics × 500 points
in one block, one metric of 200 queried:

| | without bloom | with bloom | change |
|---|---|---|---|
| block size | 1.47 MB | 1.49 MB | **+1.5 %** |
| block write | 44 ms | 59 ms | **+33 %** |
| single-metric query | 31.98 ms | **2.83 ms** | **11.3×** |

Both paths return the same 500 points. An absent metric decodes nothing at all.

**Still open in this item:** page/column-index-based pruning *within* a row
group. The index is now written and loaded, but nothing reads it yet; the
column-level prune is left for a follow-up because it must stay **sound** — same
rule as the existing row-group prune: keep rather than skip when a statistic is
absent. Also `service_name` is not used as a bloom conjunct yet, because once
`BL-03-07` orders rows by `(metric_name, service_name, timestamp_ns)` the
row-group statistics prune on service exactly and for free, which is strictly
better than a probabilistic filter.

**Acceptance.** Narrow metric queries open ≤ 1 row group per candidate block;
bloom filters confirmed present on both key columns. Met.

**Effort** M · **Risk** Medium

---

## BL-03-04 (H) — `labels` stored as a per-row JSON string column instead of a series-keyed dictionary

**Evidence**
- `parqtel-core/src/models/storage/schema.rs:88` — metrics `labels` is `DataType::Utf8`; logs `attributes` and `resource_attributes` are `Utf8` (`:155-156`); traces `attributes`/`resource_attributes`/`events`/`links` are `Utf8` (`:227-230`).
- By contrast `metric_name`, `service_name`, `service_version`, the k8s columns and `resource_attributes` (metrics) **are** dictionary-encoded (`schema.rs:44`, `:50`, `:84`).
- Every row serialises its labels independently: `parqtel-core/src/models/storage/writer.rs:57` — `labels.append_value(&dp.labels.to_json()?)`; scanner caches by JSON text because "label JSON repeats once per series across thousands of rows" (`scanner.rs:132`, `:198-219`).

**Gap (as originally stated).** The largest repeated column is a near-identical JSON blob per row — the archetypal Parquet anti-pattern — supposedly costing file size because "zstd must re-compress the same text per row", plus write CPU and scan bandwidth.

**MEASURED: the premise is wrong. Do not do this.**

`cargo run --release -p parqtel-core --example probe_labels_size`, 100 000 rows,
200 series, 5 000 rows/row-group:

| configuration | `Utf8` (current) | `Dictionary(Int32, Utf8)` | change |
|---|---|---|---|
| zstd (production) | 810 614 B | 810 678 B | **0.0 %** |
| uncompressed | 1 784 431 B | 1 784 495 B | **0.0 %** |
| write time (zstd) | 0.034 s | 0.035 s | **wash** |

Reading the encodings back out of a block written with the **plain `Utf8`
column** gives:

```
"labels"  ["PLAIN", "RLE", "RLE_DICTIONARY"]
```

**Parquet dictionary-encodes string columns by default.** The Arrow-level type
only decides whether *Arrow* does the encoding; Parquet applies the same
`RLE_DICTIONARY` either way. So a series dictionary would add an encoding that
is already present, and zstd then compresses what remains — 9 MB of raw labels
JSON becomes 810 KB.

An on-disk format migration is expensive, risky, and version-sensitive. This
says it buys nothing, so it should not be done. The probe is kept as the
evidence and as a guard if anyone revisits the assumption.

**What survives from the original analysis:** the per-row
`labels.to_json()` on the *write* path and the per-series `from_json()` on the
*read* path are real CPU costs. The read side is already addressed by the
per-chunk label cache in `perf(storage)`. The write side is one serialisation
per row and was not measured separately here — `parqtel_flush_duration_seconds`
is the metric to watch if anyone wants to quantify it.

Note also that `metric_kind` is plain `Utf8` for the same reason, and the same
conclusion applies: Parquet already dictionary-encodes it.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Introduce a **series dictionary**: intern each distinct label set to a `u32 series_id` (from the same interner as BL-01-02), store `series_id` as `DataType::UInt32` (or `Dictionary(Int32, UInt32)`), and keep a per-block side table mapping `series_id → labels JSON`.
- Equivalently, make `labels` a `Dictionary(Int32, Utf8)` column so Parquet dictionary-encodes it — cheaper to implement, smaller win, no side table.
- Prefer the full series-id design; fall back to dictionary-encoding if the side-table complexity is not worth it in one step.
- Same treatment for logs `attributes`/`resource_attributes` and traces `attributes`.

**Acceptance.** NOT MET — and the target itself was wrong. Closed as measured-not-worth-doing. No schema version bump, no migration, no `schema_version` field: none is needed when the format does not change.

**Effort** XL (avoided) · **Risk** High (avoided)

---

## BL-03-05 (H) — Compression level is not configurable; codec choice is stringly-typed and duplicated

**Evidence** — `writer.rs:401-406` and `compactor.rs:344-349` both match on `config.compression` as a string:
```rust
"zstd" => Compression::ZSTD(Default::default()),  // library default level
"snappy" => Compression::SNAPPY,
"lz4" => Compression::LZ4_RAW,
_ => Compression::UNCOMPRESSED,
```
Validation lives separately in `parqtel-core/src/config/mod.rs:50-62` against `["zstd","snappy","lz4","none"]`, so the mapping is duplicated in two places; `"none"` relies on the `_` arm. There is no level control and no distinction between `LZ4_RAW` (write) and its read-side equivalent.

**Gap.** zstd at the library default is a size-first choice applied to the hot write path. Level 1 is typically 3–5× faster to encode with a modest size penalty — the right default for blocks written every 30–300 s (BL-01-04). Conversely, compacted cold data (24 h tier, `compactor.rs:120`) is written with the same setting even though it is read rarely and benefits from a higher level. The duplication also means a config typo validated in one place can behave differently in the other.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Add `compression_level: Option<i32>` to `BlockConfig`/`LogBlockConfig`; single shared `fn compression_from_config(&str, Option<i32>) -> Compression` used by both the writer and the compactor.
- Tier-aware policy: fast codec/level for fresh blocks, high level for compacted tiers (pairs with BL-03-09).
- Make codec an enum rather than a string; keep string parsing at the config boundary only.

**Acceptance.** `compression_level` documented in `docs/CONFIGURATION.md`; flush CPU for the default config drops ≥ 30 % at equal or better compressed size for the compacted tier.

**Effort** S · **Risk** Low

---

## BL-03-06 (H) — Block index size is O(blocks × fields × values) and label-value lookups re-merge per block

**Evidence** — `BlockMetadata` (`schema.rs:7-23`) carries, per block: `metric_names: HashSet<String>`, `label_names: HashSet<String>`, `label_values: BTreeMap<String, BTreeSet<String>>`. These are rebuilt per row at flush (`writer.rs:91-118` metrics, `:232-257` logs) and serialised wholesale on every save (`index.rs:34`). Lookups merge across blocks per call (`executor.rs:1297`, `:1324`, `:1513`). `metric_names` and `label_names` are **unbounded per block**.

**Gap.** A 30-day retention at, say, 300 blocks/signal with 50 label fields at 10 000 values each produces an index measured in hundreds of MB — serialised in full on **every flush** (BL-03-01). Per-block `HashSet<String>` of metric names also duplicates information that is trivially derivable from the block's Parquet dictionary page.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Replace per-block `metric_names`/`label_names` with a compact **series-dictionary side table**: one global `series_id → {metric, labels}` map plus a per-block `Vec<u32>` of the series it contains (falls out of BL-03-04).
- Keep `label_values` only in a **separate**, independently-loaded autocomplete structure with its own cap and eviction, not in the hot `index.json`.
- Persist the autocomplete structure lazily (it is rebuilt from blocks if lost) — `/api/v1/label/:name/values` already merges with the memory buffer (`buffer.rs:196-207`).
- Add an index size guard: warn and prune `label_values` when the serialised index exceeds a configured threshold.

**Acceptance.** `index.json` < 20 MB at 30 days retention with 10 000 blocks; index save duration independent of label cardinality.

**Effort** L · **Risk** Medium

---

## BL-03-07 (H) — Row groups are time-sorted only; no `(metric_name, service_name, timestamp)` ordering, and default row-group sizes are coarse

**Evidence**
- `writer.rs:72` — `self.buffer.sort_by_key(|ctx| ctx.dp.timestamp_ns)`; compaction sorts by timestamp too (`compactor.rs:264`, `:306`).
- `scanner.rs:520-532` documents the resulting invariant: pruning is only *useful* when rows are roughly time-ordered, and relies on flush/compaction sorting.
- Defaults: metrics `row_group_size: 100_000` (`config/storage.rs:40`), logs `20_000` (`:73`), with `max_rows_per_block` 1 000 000 / 200 000 (`:36`, `:70`).
- `row_groups_in_range` prunes on the timestamp column only (`scanner.rs:533-574`), and the metrics scan then filters by metric name row by row (`scanner.rs:187-190`).

**Gap.** With a single global time ordering, a query for one metric in one service must decode a row group that also holds every other metric and every other service. At 100k rows per group, that is up to 100 000 rows decoded (and their label JSON parsed) to serve a handful of points. Metric names are already dictionary columns and service names are dictionary columns — sorting by them first costs nothing at write time and would make row-group pruning two-dimensional. The 100 000-row default is also coarse: it is 100 000 rows of decode work per group before pruning can help.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Sort rows at flush by `(metric_name, service_name, timestamp_ns)` — for metrics; `(service_name, severity, timestamp_ns)` or just `(service_name, timestamp_ns)` for logs; traces already keyed by `start_time_ns`.
- Extend `row_groups_in_range` into a `row_groups_matching(metadata, ts_column, start, end, name_column, name)` that intersects **both** the timestamp and the metric/service dictionary statistics.
- Retune `row_group_size` defaults downward (25k–50k metrics, 5k–10k logs) once ordering is in place; document the trade-off (more row groups → more footer/metadata bytes).
- Note `ROW_GROUP_ROWS`/`row_group_size` was already noted as a follow-up in `docs/benchmarks/PERFORMANCE.md` — this item closes it properly.

**Acceptance.** A single-series query in a 1M-row block decodes ≤ 1 row group (measured via `parquet-tools inspect` + scan timing). Narrow-query scan cost independent of `row_group_size`.

**Effort** M · **Risk** Medium (ordering change affects compaction grouping and the pruning soundness argument — extend the existing tests at `parqtel-core/src/storage/mod.rs:243-328`)

---

## BL-03-08 (M, partly landed) — No column projection: scans decode every column of every candidate block

**Evidence** — `scanner.rs:123-126` (metrics), `:350-352` (logs), `:475-477` (traces) build the reader with no projection mask:
```rust
let reader = reader_builder.build()?;
```
`ParquetRecordBatchReaderBuilder::with_projection` is never used. The metrics scan then reads columns 0, 1, 3, 11, 12, 13, 14 (`scanner.rs:134-178`) out of 15; log/trace scans via `StorageModel::row_to_log`/`row_to_span` need fewer than all 19/26 columns but pay for all of them.

**Gap.** For a `resource_attributes` selection over metrics, columns 4–10 (six dictionary columns) and `value_complex` are decoded for nothing. For trace search, `events`, `links`, `trace_state`, `status_message` are decoded per span and then JSON-parsed by `row_to_span` (`scanner.rs:490-503`) even when the caller only filters on service/operation.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).** Project explicitly per signal and per query shape: metrics scan → `[0,1,3,11,12,13,14]`; log count/volume path → `[0, severity, attributes, resource_attributes]`; trace filter-only path → defer `row_to_span` entirely and filter on the cheap columns, materialising full spans only for rows that pass (this also fixes the asymmetry noted in BL-02-12). Add a `Projection` parameter to the scanner entry points.

**Acceptance.** Bytes decompressed per query reduced by ≥ 30 % on the seeded dataset for all three signals.

**Effort** M · **Risk** Medium (a wrong mask silently truncates columns — add a test asserting each scan path's projection matches its row decoder's needs)

---

## BL-03-09 (M, partly landed) — Compaction is a full decode→re-encode with fixed, small merge limits and one merge per signal per pass

**Evidence** — `compactor.rs:55-113` (`compact_once`) merges blocks with `row_count < 10000`, up to **8** at a time (`:71`), via full row decode (`read_source_blocks`, `:209-231`) and full re-encode (`write_merged`, `:256-370`). `compact_tiered` (`:117-207`) merges ≤ **12** blocks (`:174`) and then `break`s — **one merge per signal per pass** (`:202-203`). Block selection ignores adjacency for `compact_once` (it takes the first 8 small blocks from an unordered `filter`), and `write_merged` rebuilds `label_names`/`metric_names` but sets `label_values: Default::default()` (`:388`), so **compaction silently discards the label-value index** built at flush.

**Gap.**
- Merge limits are hard-coded, not driven by `max_rows_per_block`, so compaction cannot keep up with a high block-arrival rate and the small-block population grows.
- One merge per signal per 3 600 s interval is far too slow to converge.
- Compaction is read-amplifying: it decodes **every column** (BL-03-08) and re-serialises labels per row (`storage/writer.rs:57`).
- Losing `label_values` on compaction regresses label-value autocomplete for all compacted blocks — a correctness-adjacent regression hidden in a maintenance path.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Drive merge limits from `max_rows_per_block` / `row_group_size` instead of literals; select **adjacent** blocks (sorted by `start_timestamp_ns`, which the index already maintains) so merged blocks stay time-contiguous and pruning stays effective.
- Loop until no mergeable group remains, bounded by a per-cycle budget and a concurrency limit, instead of one merge per pass.
- Carry `label_values` through the merge (union of source blocks) or rebuild from the new block's own flush-time collection.
- Use projection to skip unused columns during compaction decode, and consider rewriting via Parquet row-group-level copy when the schema is unchanged (avoids row materialisation entirely).

**Acceptance.** Small-block count returns to steady state within one compaction interval at 10× the current block-arrival rate; compacted blocks retain their label-value index.

**Effort** L · **Risk** Medium

---

## BL-03-10 (M, landed) — Trace compaction is skipped entirely

**Evidence** — `compactor.rs:178-182`: for `SignalType::Traces` the tiered pass `continue`s with the comment "skip read_source_blocks (which only handles metrics/logs) and just leave them for now". `read_source_blocks` (`:209-254`) only decodes metrics (`row_to_point`) and logs (`row_to_log`); there is no `row_to_span` branch.

**Gap.** Trace blocks never merge. With `retention_days = 7` for traces and a 30-minute-ish flush cadence, the trace block count grows monotonically until retention deletes them, and every trace query pays the per-block overhead (open, footer decode, page decode) across all of them. Trace search is the query most sensitive to block count because each block read also JSON-parses span attributes.

**Resolution taken.** `read_source_blocks` decodes spans as a third arm
(`StorageModel::row_to_span`), `write_merged` encodes them with
`StorageModel::traces_to_chunk` sorted by `start_time_ns` so the merged block
keeps the time-ordered layout row-group pruning relies on, and the tiered pass
no longer `continue`s past traces. Both passes therefore handle all three
signals uniformly.

Row-group-size preservation is inherited: `write_merged` already applied the
config's `row_group_size` for metrics and logs, and the trace arm goes through
the same writer.

**Acceptance.** Three small trace blocks merge into one, all spans survive and
the merged block still decodes — pinned by `test_compactor_merges_trace_blocks`.

**Still open:** trace compaction now reads all 24 span columns (BL-03-08's
projection work).

**Acceptance.** Trace block count is bounded under sustained ingest; trace search latency flat over a 7-day window rather than degrading linearly.

**Effort** M · **Risk** Low

---

## BL-03-11 (M) — Retention holds the write lock across saves and deletes; no size-aware or dry-run policy

**Evidence** — `retention.rs:23-60`: `let mut idx = index.write().await;` then `idx.blocks.retain(...)` (`:31-39`), `idx.save()?` (`:50`) and a `fs::remove_file` loop (`:52-54`) — **all under the write lock**. Sweep interval is a hard-coded 3 600 s (`retention.rs:14`). `retention_days` is time-based only; there is no size cap.

**Gap.** Same lock-held-across-IO problem as BL-03-02, and it recurs hourly. Time-based-only retention means a high-cardinality or high-ingest deployment can fill the disk long before the retention horizon, with no back-pressure signal to the operator other than the deletion log line.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Compute the deletion set under the read lock, release, delete files lock-free, then take the write lock once to remove the entries and persist.
- Add `retention_max_bytes` (soft disk cap) alongside `retention_days`, deleting oldest-first when exceeded, with `parqtel_retention_deleted_blocks_total` and a disk-usage gauge exported.
- Move the sweep interval into config rather than a literal (`retention.rs:14`).
- Add a `--dry-run`/stats mode to the retention path so operators can preview deletions.

**Acceptance.** Index write-lock hold time during retention < 1 ms; a disk-usage alert threshold exists and is documented.

**Effort** M · **Risk** Low

---

## BL-03-12 (H, **landed for metrics**) — No write-ahead log: crash loss is up to an entire block window

**Evidence** — `parqtel-core/src/config/ingest.rs:58` — `wal_enabled: false` by default (`log_wal_enabled: true` at `:59` is unused by this path). The buffer is drained only after a successful flush (`parqtel-ingest/src/service.rs:255-259`, `:276-280`), and blocks rotate on row count or the (effectively dead) time trigger — see BL-01-04.

**Gap.** On crash or OOM-kill, everything in the memory buffer is lost. Combined with `block_duration_secs = 7200` that is potentially hours of telemetry. For an SRE tool this is the most damaging reliability gap in the backlog: the data you lose is exactly the data you wanted during the incident.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).**
- Implement a WAL per signal: append decoded points/records to a length-prefixed, CRC-checked segment file on the blocking pool (batched, not per point); on startup replay and re-ingest; truncate after a successful block flush.
- Enable by default for metrics and logs once it is implemented; expose `wal_sync_mode` (`none`/`interval`/`fsync`) so the durability/throughput trade-off is explicit.
- Bound WAL size and segment count; expose replay duration and last-replay outcome on `/api/v1/stats`.

**Also the prerequisite for BL-01-14.** The async flush worker (acknowledge
before durable) is deferred until this lands, because a WAL is what makes an
unacknowledged flush recoverable. If the async flush is wanted sooner, do this
item first.

**Effort** XL · **Risk** Medium (taken; verified rather than assumed)

---

## BL-03-16 (CRITICAL, open — data loss) — Block index is not durable before the WAL commits

**Status: open. Reproduced on unmodified `main` (`c4c60d6`).** Found while
implementing `BL-01-14`; it is not caused by that work.

**Reproduction.** A metrics block large enough to flush, then `SIGKILL`
immediately after the flush reports completion:

```
at kill      : ingested=64000  flushed=60000  blocks=3
after restart: blocks=0  rows=0
parquet files on disk: 3      index.json: absent
```

Sixty thousand acknowledged points are on disk as three Parquet files and
invisible to every query, and nothing replays them.

**Cause.** The ordering between the block write, the index sidecar and the WAL
commit is wrong:

| step | what happens |
|---|---|
| 1 | block written and renamed — the bytes are durable |
| 2 | metadata published to the index task, which marks the sidecar dirty |
| 3 | **WAL committed** — those rows are now considered recovered |
| 4 | sidecar written, on a **2 s debounce** (`index_persist_interval_secs`) |

A crash between 3 and 4 leaves the block on disk, absent from the sidecar, and
already committed in the WAL — so replay skips it and no query can find it. The
window is as wide as the index debounce, and #44 introduced the debounce while
#58 introduced the commit. Neither was wrong alone; together they lose data.

**Fix.** The index must be durable *before* the WAL is committed. That needs a
handshake, because the flush path and the index task are different components:

1. `run_flush_job` publishes the metadata, then waits for the index to confirm
   the sidecar write completed, and only then commits the WAL. The index task
   already owns persistence; it can acknowledge after a forced write rather
   than waiting for the debounce.
2. Preferably *also* make the sidecar self-healing: on startup, if the sidecar
   is missing or older than the newest block file, rebuild it by reading the
   block footers. That removes the ordering requirement entirely and also
   recovers from any other way the index can be lost.

While this is open, `parqtel_index_sidecar_bytes` and
`parqtel_index_pending_writes` are the metrics to watch: a non-zero
`pending_writes` that never returns to 0 means blocks exist that the sidecar
does not yet know about.

**Acceptance.** The reproduction above must show `blocks=3 rows=60000` after
the `SIGKILL`, with the sidecar present. Until then, a crash can lose
acknowledged data, which is the one guarantee a WAL exists to provide.

**Effort** M · **Risk** Medium

---

## BL-03-17 (H, open) — Effective HTTP body limit is ~2 MiB, not `ingest.max_body_size`

**Status: open. Reproduced on `main`.**

`ingest.max_body_size` defaults to 10 MiB and is applied with
`RequestBodyLimitLayer`, but axum's `DefaultBodyLimit` (2 MiB) wins, so larger
batches are rejected:

```
payload 2033672 bytes ( 1.94 MiB) -> HTTP 200
payload 2259632 bytes ( 2.15 MiB) -> HTTP 413
```

**Why it matters.** An operator who raises `max_body_size` to accept larger
OTLP batches will see no change and a `413` from `RequestBodyLimitLayer`, with
nothing pointing at the real limit. It also caps ingest throughput, since
throughput is batch-size-limited.

**Fix.** Remove `DefaultBodyLimit` (or set it above the configured value) on
the ingest routes so `ingest.max_body_size` is authoritative, and state the
effective limit in the 413 body.

**Acceptance.** A payload of `max_body_size + 1` is rejected with a message
naming `max_body_size`; a payload just under it is accepted.

**Effort** S · **Risk** Low

---

## BL-03-13 (L) — Schema and writer hygiene

| # | Gap | Evidence | Resolution |
|---|-----|----------|------------|
| a | `value_complex` stores histogram/summary payloads as a JSON string | `schema.rs:91`, `storage/writer.rs:37`, `:74` | Once BL-03-04 lands, consider native list/struct columns; measure before committing |
| b | `MetricValue` histograms carry `Vec<f64>`/`Vec<u64>` that are cloned per row per step on read | `parqtel-query/src/aggregation.rs:120-133`, `:336` | Tracked as BL-02-18 |
| c | `fs::create_dir_all` per flush and `fs::metadata` after every rename | `writer.rs:131`, `:139`, `:269`, `:277`, `:360`, `:368`; `compactor.rs:341`, `:370` | Create once at startup (`main.rs:150-151`); take size from the writer's byte counter |
| d | `Uuid::new_v4()` per block filename | `writer.rs:126`, `:264`, `:355`; `compactor.rs:336` | Monotonic `ulid` (already vendored, `Cargo.toml:59`) — also gives lexical ordering |
| e | No `schema_version` / checksum on the index sidecar; the 10 000-value cap is applied silently | `schema.rs:7-23`; `writer.rs:96`, `:108`, `:236`, `:248` | Add `schema_version` + `index_version`; surface truncation as a counter/log line |
| f | `row_group_size` is documented as the pruning knob but not validated against `max_rows_per_block` | `writer.rs:385-391`; `compactor.rs:351-354` | Validate at config load: `row_group_size <= max_rows_per_block` |
| g | `parquet` `encryption` feature enabled but unused | `Cargo.toml:31` | Remove — build time and binary size |
| h | `set_max_row_group_row_count` only; no `set_data_page_row_count_limit` | `writer.rs:408` | Partly done in `BL-03-03`: `data_page_size_limit` is now set (512 KB). `data_page_row_count_limit` still unset |

---

## BL-03-14 (L) — Compaction tier policy is time-based only and hard-coded

**Evidence** — `compactor.rs:117-121`: warm tier > 6 h → 6 h blocks; cold tier > 24 h → 24 h blocks, both as literals. Tier window choice (`:149-156`) is per-pass and driven by whether *any* candidate is > 24 h old, so a single old block switches the whole pass to 24 h targets. Candidates are capped by `row_count < 500_000` (`:132`) with no size or cost budget.

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).** Make tier boundaries and target sizes config-driven (`compaction.tier_warm_secs`, `tier_cold_secs`, `max_merge_blocks`, `max_merge_bytes`); select the tier per merge group rather than per pass; add a cost budget so a compaction cycle cannot monopolise disk I/O. Pair with BL-03-05 for tier-specific compression.

**Effort** S · **Risk** Low

---

## BL-03-15 (L) — Storage-level observability is thin

**Gap.** There is no metric for index size, index save duration, compaction bytes read/written, compaction amplification ratio, or blocks-per-signal over time. `docs/benchmarks/PERFORMANCE.md` records throughput, but production operators have no visibility into compaction amplification or index growth — the two numbers that predict "disk full in 4 days".

**Status: landed for all three signals.** Verified end-to-end by SIGKILLing a
container and restarting it.

**Resolution.**

`parqtel_core::wal` implements the log, with the crash-ordering contract as the
central design decision. A flush and the WAL must agree or a crash either
duplicates or loses data, so the sequence is:

1. rows appended to the WAL, then the request acknowledged;
2. a flush takes rows, snapshotting the WAL position it is about to cover;
3. the block is written and renamed;
4. the **commit file** is advanced to the snapshotted position;
5. segments entirely below the commit are deleted.

The commit file is the single source of truth and is advanced *after* the
rename, so every crash point is safe:

| crash after | result |
|---|---|
| (1) append | replayed, nothing on disk yet — correct |
| (2) snapshot | replayed, no block written — correct |
| (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; the WAL still holds the rows and replay rewrites them. The orphan is invisible, so there is exactly one copy |
| (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |

Advancing the commit *before* the rename would instead be able to lose a
block, which is the one failure a WAL exists to prevent.

Framing is `[u32 len][u32 CRC32][JSON payload]`, with segments named
`%020d.wal` so lexical order is sequence order. Positions are
`(segment << 32) | (offset + 1)` — **1-based**, because a 0-based first record
would collide with `START` and be silently skipped on replay. That was a real
bug caught by the round-trip test.

Twelve unit tests cover: ordered positions, round trip, commit-point skipping,
covered-segment deletion, torn-tail recovery and truncation, bad-CRC handling,
segment reuse across restart, commit persistence, writer poisoning, CRC
sensitivity, and record preservation across segment rollover.

**Ingest wiring.** The WAL is appended **while the shard lock is held**, so WAL
order and writer order are the same total order — that is what makes the
position a flush commits correct. Each shard carries the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped, so a block can only ever commit positions for rows it
actually contains.

Replay runs before the listener binds and feeds `ingest_metrics`, so recovered
telemetry is indistinguishable from data that was never lost.

**A bug the crash test caught, which the unit tests did not.** The first
version buffered appends in a 64 KB `BufWriter` and only flushed on the fsync
interval. A `SIGKILL` therefore lost every record still sitting in *userspace*
— 4 of 5 in the first run. The module docs had claimed "the page cache survives
a process crash", which is true of `write` and false of an unflushed userspace
buffer. Every append is now flushed to the OS; `fsync` remains governed by
`WalSyncMode`. Verified by re-running the same crash: **5 of 5 recovered**.

The same test run also showed the record type must be a single `Metric`, not
the batch `Vec<Metric>` — the rotator appends per metric. Both the unit test and
the replay helper were wrong in the same way and would have recovered nothing.

**Verified against a real container, not only unit tests:**

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; all 5 queryable again |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

**Cost.** The flagged trade-off — a file write under the ingest lock — measured
as **54 ns average lock wait over 9,695 acquisitions** (0.52 ms total) under
the load generator, so it is not material. `WalSyncMode::default()` is
`Interval` rather than `None`, because a default that quietly means "do not
sync" is a footgun in a durability feature.

Acceptance: crash (SIGKILL) mid-ingest loses **0 of 5** acknowledged points;
steady-state lock-wait impact negligible. Both met.

**Resolution (original plan).** Export: `parqtel_index_bytes`, `parqtel_index_save_duration_seconds`, `parqtel_blocks{signal}`, `parqtel_compaction_bytes_read`, `parqtel_compaction_bytes_written`, `parqtel_compaction_amplification_ratio`, `parqtel_retention_deleted_blocks_total`, `parqtel_block_write_duration_seconds` (split encode vs. rename). Follows from BL-03-01/02/09/11.

**Effort** S · **Risk** Low

---

## Verification commands

```bash
# block size / row group / codec inspection
parquet-tools inspect data/*.parquet | head -60

# storage growth and compaction behaviour under load
python3 scripts/gen_bench_data.py --help
python3 scripts/varying-load-test.py --help
python3 scripts/check-memory.sh

# compaction + retention stats from the running server
curl -s localhost:8080/api/v1/stats | jq '.storage'
```
