# Known issues

What a beta user is most likely to hit, and what to do about it. Kept short
and specific: if something here is not worth knowing before you deploy, it does
not belong here.

---

## The in-memory buffer is unbounded

**Symptom.** Resident memory (`parqtel_process_rss_bytes`) grows with ingest
rate × `storage.block_duration_secs` and does not come down until a block is
flushed. A sustained rate that fits at 100 k points/s can OOM at 300 k.

**Why.** Points are buffered in memory so they are immediately queryable, and
flushed to a Parquet block on a timer or at the row cap. There is no maximum
size, no byte accounting and no eviction, so the only bound is time.

**Mitigating now.**

- Lower `storage.block_duration_secs` (default **7200**). This is the strongest
  lever: buffered points scale directly with it.
- Watch `parqtel_process_rss_bytes` and alert on it.
- Size the container from the formula in [PERFORMANCE_SIZING.md](PERFORMANCE_SIZING.md)
  rather than from a default.

**Planned.** `ingest.buffer_max_points` / `buffer_max_bytes` with an explicit
eviction policy and a `parqtel_buffer_dropped_total` counter.

---

## No authentication or authorization

**Symptom.** Every endpoint, including ingest and the OTLP gRPC port, is
unauthenticated.

**Mitigating now.** Do not expose port `8080` (HTTP) or `4317` (OTLP gRPC)
publicly. Put them behind a reverse proxy or service mesh that terminates TLS
and authenticates. The embedded console reads and writes data with the same
privileges as the API.

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

## Log and trace scans decode every column

**Symptom.** Wide log or trace queries are slower than the equivalent metric
query, and cost more memory than the row counts suggest.

**Why.** Only the **metrics** scan projects columns; the log and trace decoders
read all 19 and 26 respectively.

**Workaround.** Narrow the query window, and use index blocks for pre-filtering
rather than scanning everything (`service.name` selectors are pushed into the
scan).

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

**Rolling back** is safe: blocks are self-describing Parquet and the previous
binary reads them. Data written by a newer version may not be readable by an
older one — see the `AGENTS.md` note on the arrow2 → arrow migration, where a
wipe was required.

---

## Contributing to this list

If you hit something not here, that is a bug in the docs rather than only in the
code. Open an issue or add it.
