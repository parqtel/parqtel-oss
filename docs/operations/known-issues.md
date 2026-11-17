# Known issues

What a beta user is most likely to hit, and what to do about it. Kept short
and specific: if something here is not worth knowing before you deploy, it does
not belong here.

---

## The in-memory buffer has a row and byte bound

**Symptom.** `parqtel_buffer_dropped_total{signal,reason}` climbs during a
sustained spike, and queries over the pre-flush window can miss the oldest
rows.

**Why.** Points are buffered in memory so they are immediately queryable, and
flushed to a Parquet block on a timer or at the row cap. `ingest.buffer_max_points`
(default **5 000 000** rows) and `ingest.buffer_max_bytes` (default **2 GiB**
estimated) bound that buffer. Under `drop_oldest` the oldest buffered rows are
evicted; under `reject_new` the incoming batch is refused. The writer and WAL
hold the same rows, so eviction does not lose acknowledged data — it only
shortens the window that is queryable before the flush.

**Tuning.**

- Lower `storage.block_duration_secs` (default **7200**) to flush more often;
  buffered rows scale directly with it.
- Raise `ingest.buffer_max_points` / `buffer_max_bytes` when the node has RAM
  to spare and the pre-flush window matters.
- Set `ingest.label_interner_capacity` to at least **twice** the active series
  count (default 1 048 576). Below that, every point keeps its own copy of its
  labels and memory grows with points instead of series.
- Watch `parqtel_buffer_bytes`, `parqtel_buffer_points{signal}`,
  `parqtel_buffer_dropped_total{signal,reason}` and
  `parqtel_process_rss_bytes`.
- Size the container from the formula in [performance-sizing.md](performance-sizing.md)
  rather than from a default.

The allocator is jemalloc, which returns freed memory to the OS instead of
keeping it resident in glibc arenas.

---

## Authentication is optional

**Symptom.** With the default `auth.enabled = false`, every HTTP endpoint is
open; the OTLP gRPC listener has no token layer even when HTTP auth is enabled.

**Mitigating now.** Enable `[auth]` with `read`/`write`/`admin` bearer tokens
and/or OIDC for HTTP, and keep gRPC behind a proxy or mesh. Auth is disabled by
default so the single-user/edge topology keeps working; do not expose `8080` or
`4317` without either auth or a network boundary. In-process TLS is available;
it does not add authentication by itself.

---

## A range query does not evaluate its final partial step

**Symptom.** `/api/v1/query_range` over `[now-300, now]` with `step=60` returns
nothing for the most recent minute, while the data is plainly there.

**Why.** Steps are evaluated from `start` while `ts < end`, so the last instant
evaluated is up to one `step` before `end`. This matches Prometheus.

**Mitigating now.** Use `step <= 15` for a window ending at "now", or end the
window at `now - step`. An **instant** `/api/v1/query` at the same `end` does
see those samples, through the 5-minute lookback (`query.lookback_delta_ns`).

---

## Instant queries only see the last 5 minutes

**Symptom.** An instant query returns no data for a metric that is clearly being
ingested and is in the logs.

**Why.** Instant queries look back `query.lookback_delta_ns` (default **5
minutes**). Anything older is on disk but not yet in the lookback window.

**Mitigating now.** Use `/api/v1/query_range`, or raise
`query.lookback_delta_ns`. Remember that raising it costs memory, because
recent points are also held in the buffer above.

---

## Log and trace scans are two-phase

**Symptom.** A wide log or trace query can still cost more than a narrow metric
selector, especially when it returns many rows.

**Why.** Logs and traces use late materialisation: a first pass reads
timestamps/ids (and for logs, severity/body) to select rows, then a second pass
decodes attributes, events and links only for the selected rows. This is not a
full-column decode, but a query that selects most of a block still pays for the
second pass.

**Workaround.** Narrow the time range, push a selective field predicate, and use
the log skip index by preferring conditions on time, severity, body terms,
`trace_id`/`span_id`, `service`, `attr.K` or `res.K`. Avoid selecting a broad
range with `limit` far above what you will display.

---

## Upgrade between versions

**Symptom.** After upgrading, a query returns nothing for blocks written by the
previous version.

**Why.** Blocks are immutable and read by schema, so this should not happen —
but a `SIGKILL` between a block being renamed and the index sidecar being
written could leave an orphaned block.

**What happens now.** At startup the block index is reconciled against the data
directory: block files the index has never seen are adopted (recovered from
their Parquet footer), and entries whose file has gone are dropped. So a lost or
stale index repairs itself. Watch for a `block index did not match the data
directory; reconciled` warning at startup.

**Caveat.** A recovered block carries an **empty** metric-name set, which means
"unknown". The block stays visible to every query, but name-based pruning does
not apply to it until compaction rewrites it with a full flush-time index.
Label-value autocomplete also stays empty for that block until then.

**Rolling back** is not always safe. Blocks are self-describing Parquet, but
two on-disk shapes changed with the L0/L1 work:

- New metric blocks keep their labels only in the footer series table; the
  row-level `labels` column is gone. An older binary cannot read them.
- The block-index sidecar is now `{"blocks": [...]}` and the autocomplete
  dictionary lives in `label_values.json` beside it. An older binary can parse
  the block sidecar but will not see the sibling dictionary.

Forward migration is automatic — the loader still accepts the old sidecar
shape and folds its per-block values in. Rolling back is only safe once the
superseded `m_l0_` sources have been removed. The arrow2 → arrow 60 migration
is a harder boundary: a wipe was required there.

---

## Alert evaluation has a dedicated scan slot

**Symptom.** `parqtel_alert_eval_skipped_total` climbs and an alert evaluation
cycle was skipped.

**Why.** Query scans share a process-wide budget of 4 slots. Alert evaluation
has its own single slot, so saturating dashboard queries cannot starve the
alert loop; the alert cycle is skipped only when its own slot is still busy
from the previous evaluation.

**Mitigating now.**

- Reduce the query fan-out of the alert rule (narrower window, fewer series).
- Roll metric blocks up into 5-minute L1 windows more aggressively by lowering
  `storage.compaction_interval_secs` (default 300).
- Watch `parqtel_scan_background_slots_in_use` against
  `parqtel_scan_background_slots_limit` and
  `parqtel_alert_eval_skipped_total`.

---

## A corrupt object fails the whole query

**Symptom.** With object storage enabled, a query returns HTTP 500 and a
Parquet footer error even though other blocks are healthy.

**Why.** A missing object is treated as not-found and skipped. A **corrupt**
object cannot be decoded, so the scan returns an error rather than silently
returning partial results.

**Mitigating now.** Identify the key from the server log, remove or replace the
corrupt object, and retry. If the data is still in the local cache, delete the
cached copy so it is re-fetched. Narrow the query range to exclude the block
while you repair it. See [Object storage](object-storage.md#failure-behaviour).

---

## Object storage is one-writer-per-prefix

**Symptom.** Two Parqtel writers publish to the same bucket prefix and blocks
or catalog entries intermittently disappear.

**Why.** The shared catalog uses compare-and-swap and writer-owned retirement,
but there is no lease/fencing protocol. Concurrent writers can overwrite each
other's compaction outputs.

**Mitigating now.** Use one writer per prefix. Reader nodes may adopt blocks
from that prefix. Run compaction and retention only on the writer.

---

## Contributing to this list

If you hit something not here, that is a bug in the docs rather than only in the
code. Open an issue or add it.
