# Durability and recovery

The write-ahead log, commit ordering, and the self-healing block index.

## Write-ahead log

Bounded crash loss, verified by killing a real container.

### The ordering contract

A flush and the WAL must agree, or a crash duplicates or loses data:

1. rows appended to the WAL, request acknowledged
2. a flush takes rows, **snapshotting the WAL position it will cover**
3. the block is written and renamed
4. the **commit file** is advanced to that snapshot
5. segments below the commit are deleted

The commit file is the single source of truth and advances **after** the rename:

| crash after | result |
|---|---|
| (1)/(2) | replayed, nothing on disk — correct |
| (3) rename, before (4) | block exists but is not committed, so it is not indexed; the WAL still holds the rows and replay rewrites them. The orphan is invisible → one copy |
| (4) commit, before (5) | rows covered; replay skips at or below the commit → discarded, not duplicated |

Advancing the commit *before* the rename could lose a block — the one failure a
WAL exists to prevent.

The WAL is appended **while the shard lock is held**, so WAL order and writer
order are the same total order. Each shard records the highest WAL position
among the rows in its writer, taken and reset under that shard's own lock as
its writer is swapped — so a block can only ever commit positions for rows it
actually contains.

### A bug the crash test caught and the unit tests did not

The first version buffered appends in a 64 KB `BufWriter` and flushed only on
the fsync interval. A `SIGKILL` lost everything still in **userspace** — 4 of 5
points on the first crash run.

The module docs had claimed *"the page cache survives a process crash"*, which
is true of `write` and **false of an unflushed userspace buffer**. Every append
is now flushed to the OS; `fsync` remains governed by `WalSyncMode`.

The same run also showed the record type had to be a single `Metric`, not the
batch `Vec<Metric>` the rotator receives. The unit test and the replay helper
were wrong in the same way and would have recovered nothing.

### Verified against a real container

| scenario | outcome |
|---|---|
| ingest 5 points, nothing flushed, `SIGKILL`, restart | replay `records=5`; **all 5 queryable again** |
| ingest 3 points, graceful stop (flushes + commits), restart | replay `records=0 skipped=3`; 1 block / 3 rows; all 3 queryable, **not duplicated** |

### Cost

The flagged trade-off — a file write under the ingest lock — measured as
**54 ns average lock wait over 9 695 acquisitions** (0.52 ms total) under the
load generator:

```
parqtel_ingest_lock_wait_seconds{signal="metrics"}_sum   0.000522
parqtel_ingest_lock_wait_seconds{signal="metrics"}_count 9695
```

So it is not material, which is worth having measured rather than assumed.

`WalSyncMode::default()` is `Interval`, not the fastest option: a default that
quietly means "do not sync" is a footgun in a durability feature.

### All three signals

`LogRotator` and `TraceRotator` now carry the same `wal`/`covered` pair,
appended under the rotator lock (which the caller already holds) and committed
only after the block is renamed. `log_wal_enabled` is genuinely wired rather
than being a dead flag.

Verified in a single crash — 3 metrics, 3 logs and 3 server spans ingested with
nothing flushed, then `SIGKILL` and restart:

```
recovered telemetry from the write-ahead log  signal="metrics" records=12
recovered telemetry from the write-ahead log  signal="logs"    records=3
recovered telemetry from the write-ahead log  signal="traces"  records=3
```

`metrics=12` is 9 RED-derived metrics (the server spans derive the RED bridge)
plus the 3 originals. After recovery `m1`/`m2`/`m3` each returned exactly
**one** sample — no duplication — and the log bodies and span operation names
came back intact.

Lock-wait cost on all three signals under the load generator:

| signal | acquisitions | total wait | mean |
|---|---|---|---|
| metrics | 9 555 | 0.51 ms | 53 ns |
| logs | 56 | 0.007 ms | 126 ns |
| traces | 56 | 0.065 ms | 1.2 µs |

### Configuration

`wal_enabled` (default **true**), `wal_sync_mode` (`interval`),
`wal_sync_interval_ms` (1000), `wal_max_segment_bytes` (64 MB). A WAL that
cannot be opened **fails startup** rather than silently accepting unrecoverable
data.

## Index durability before the WAL commit

A crash in the window between a block being written and the index sidecar
catching up lost acknowledged data: the block was on disk, absent from the
sidecar, and already marked recovered in the WAL, so nothing replayed it and no
query could find it. Reproduced on `main`:

```
at kill      : ingested=64000  flushed=60000  blocks=3
after restart: blocks=0  rows=0      index.json: absent
```

The fix is a durability handshake. `PendingIndex` carries an optional oneshot
alongside the metadata; the flush waits on it and commits the WAL only once the
index task has **forced** the sidecar write, rather than waiting out the 2 s
debounce that created the window.

The wait is bounded (30 s) and fails safe — on timeout the WAL is deliberately
*not* committed, so the rows stay in it and a replay restores them. An unbounded
wait would have turned a wedged index task into a full ingest outage, which is a
worse failure than an orphaned block.

Same reproduction, after:

```
after restart: blocks=3  rows=60000     index.json: present
```

## Effective HTTP body limit

axum's `DefaultBodyLimit` (2 MB) was the real ingest limit; `ingest.max_body_size`
(10 MiB) was applied via `RequestBodyLimitLayer` and silently lost. Raising the
setting changed nothing and the `413` pointed nowhere useful.

`DefaultBodyLimit::max(ingest.max_body_size)` is now layered as well. With
`max_body_size` at its 10 MiB default:

| payload | before | after |
|---|---|---|
| 2.15 MiB | 413 | **200** |
| 4.02 MiB | 413 | **200** |
| 10.69 MiB | 413 | 413 |

This also raises the ingest throughput ceiling, since throughput is
batch-size-limited.

## Self-healing block index

The sidecar is a cache of on-disk state. `reconcile` walks the difference in
both directions at startup — adopting block files the index has not seen, and
dropping entries whose file has gone — then persists the repair and logs a
`WARN` so it is never silent.

Cost when the index already agrees, which is the normal case, is **one
`read_dir`**; footers are parsed only for unknown files and the sidecar is
rewritten only on change.

Verified by deleting `index.json` with four blocks (64 000 rows) on disk:

```
reconcile: adopted=4 dropped=0 unreadable=0
after: blocks=4 rows=64000    sidecar rebuilt (1164 bytes)
big_0, big_3, big_7 -> all READABLE
```

### Three things a Parquet footer cannot give, and what happens instead

| field | recoverable? | resolution |
|---|---|---|
| `metric_names` | min/max only — a *range*, not the set | left **empty**, and `query` treats empty as "unknown" so the block stays visible. An earlier version recorded just min and max, which made every other metric in the block unfindable because `query` tests set membership. |
| `label_names`, `label_values` | no — flush-time only, stored solely in the sidecar | lost until compaction rewrites the block; queries unaffected, only `/api/v1/label/:name/values` degrades for it |
| timestamps | yes, but per row group | aggregated across **all** row groups. Reading only row group 0 made a block claim to end where its first group ends, so it was pruned for any later query — indexed, and silently returning nothing. |

### Signal detection uses the schema, not the filename

Metrics and traces share a data directory, so adoption is per signal. The
authoritative test is a column only that signal's schema has — `severity_text`
(logs), `metric_kind` (metrics), `start_time_ns` (traces). The filename prefix is
a cheap pre-filter *before* opening the file, so startup does not re-parse the
other signals' footers; a wrong name can only cause a block to be **missed**,
never wrongly adopted.

The order is the reverse of the obvious, and the multi-signal test caught it: the
**log schema has a `span_id` column too**, so testing for `span_id` first
classified log blocks as traces.

## Async flush worker

A request that crossed the block cap used to wait for the Parquet encode before
being acknowledged. It no longer does: the shard writers are swapped out under
their own short locks and the encode runs on a background task. The rows are
already in the WAL, so durability is unchanged.

Measured with the body limit fixed to its configured 10 MiB, on
130 000-point batches with the block cap set to 130 000 so every request
triggers exactly one ~125 ms encode:

| | before | after |
|---|---|---|
| single flush-triggering request | ~330 ms | **~280 ms** |
| 4 concurrent flush-triggering requests | 1.291 s | **0.932 s** |

Modest — and the reason is that **OTLP decode dominates**: a 7 MiB batch costs
~175 ms to decode against ~125 ms to encode, so taking the encode off the
request path can only remove about 40 % of it. The larger relative gain is
under concurrency, where encodes used to serialise behind the flush lock.

### A sequential worker was worse than no worker at all

The first version ran one worker loop that processed jobs one at a time. That
serialised the encodes exactly as the flush lock had, and made four concurrent
requests **slower**: 1.461 s versus the 1.291 s baseline. The worker now spawns
one task per job, bounded by a semaphore, so encodes genuinely overlap. That
change is what produced 0.932 s — without it the whole idea was a regression.

### The correctness point: when the buffer is drained

The memory-buffer drain moved from the service into `run_flush_job`, and now
happens only after the block is **durable**. Draining when a flush was merely
*scheduled* would leave rows in neither the buffer nor any block — invisible to
every query until the encode landed. This is the easiest thing in the change to
get wrong and the hardest to notice, so
`test_background_flush_keeps_rows_visible_throughout` drives the real service and
asserts rows stay in the buffer until a block file exists, then leave it.

### Bounds and shutdown

`ingest.max_inflight_flushes` (default 4) caps concurrent encodes and bounds the
queue. A full queue makes a flush encode inline rather than queueing further, so
backpressure degrades to the previous synchronous behaviour instead of growing an
unbounded backlog.

Shutdown drops the channel sender, then awaits every in-flight encode, so a
graceful stop cannot leave a block half-written or skip a WAL commit. Verified:
after a restart, `storage_rows` is preserved and the data queryable.

### The WAL is a precondition, not a recommendation

The worker's contract is *"the rows are already logged, so nothing is lost if
this process dies before the encode lands"*. With `ingest.wal_enabled = false`
that contract is **void**, so `start_flush_worker` refuses to start and
`flush_locked` takes the synchronous path — still acknowledged only once the
block is on disk. A `WARN` says so at startup.

Refusing rather than failing startup is deliberate: `wal_enabled = false` is a
legitimate operator choice, it just cannot also have a background flush. Falling
back to the safe behaviour beats silently weakening durability.

Verified with the WAL disabled: the refusal warning fires, and a
flush-triggering request returns in 390 ms with its block already written — the
encode happened inline.

Pinned by `test_flush_worker_refuses_to_start_without_a_wal` and
`test_flush_worker_starts_with_a_wal`.

## Block index size: measured

The block index sidecar carries per-block `metric_names`, `label_names` and
`label_values`, and is re-serialised whole on every debounced pass.

Measured on the load-generator stack after ~10 minutes:

| | |
|---|---|
| blocks | 1 226 |
| data (`storage_bytes`) | 35.6 MB — **29 064 B/block** |
| `index.json` | 8.20 MB — **6 688 B/block** |
| ratio | **the sidecar is 23 % of the data it describes** |

Growth is linear in block count — ~32 blocks/min in that run, i.e. **~0.3 GB of
`index.json` per day**, and unlike the data it is not bounded by retention.

### Where the bytes go

Per block, on a representative shape (600 series, 4 label fields):

| field | bytes/block | share |
|---|---|---|
| `metric_names` | ~500 | **55 %** |
| `label_values` | ~107 | 12 % |
| `label_names` | ~57 | 6 % |
| rest (path, sizes, timestamps) | ~243 | 27 % |

`metric_names` dominates *because the same name is stored once per block* — on the
load stack, where a block spans 50–100 distinct metric names, more so still.

### Why this is recorded rather than built

The fix — a per-block **bloom filter** over metric names, ~10 bits per name
instead of a repeated string — is sound (a false positive costs one extra block
read, never a missing result) and would cut ~55 % of a sidecar that is itself
23 % of storage: **~13 % of total storage**.

But it is a **sidecar format change** needing a read-old/write-new story, and
the series-dictionary item is a standing reminder that this class of change can
cost more than it returns. So it is recorded here as a measured, sized proposal rather than built.

The low-risk half needs **no** format change: `label_values` is read only by
`/api/v1/label/:name/values` and is rebuilt on compaction, so splitting it into
a sibling file is straightforward.

### Reproducing

```bash
curl -s localhost:9090/metrics | grep -E 'parqtel_index_sidecar_bytes|parqtel_storage_(blocks|bytes)'
```
