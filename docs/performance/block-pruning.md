# Block and row-group pruning

What a scan can skip before it decodes a page.

## Metric-name pruning uses statistics first

Bloom filters on `metric_name` were first added on the premise that
row-group statistics cannot discriminate on a value column. **They can.** Rows
are written grouped by metric name, so a row group's `metric_name` min and max
are usually the *same value* — an exact, free answer requiring nothing to be
read from the filter region.

Probing the writer output confirms it: for a five-metric block with one row
group per metric, `metric_name` statistics are `min == max == "metric_N"` in
every group. (`service_name` statistics came back all-null in that probe only
because the fixture put `service.name` in point labels rather than resource
attributes, so the column was never populated.)

### The real comparison is three-way

200 metrics × 500 points in one block, querying one metric of 200, through the
production `Scanner::scan` path:

| configuration | size | write | query |
|---|---|---|---|
| neither (old blocks) | 1.29 MB | 38 ms | 30.33 ms |
| **statistics only** | 1.47 MB | 55 ms | **2.34 ms** |
| statistics + bloom | 1.49 MB | 42 ms | 2.49 ms |

Against "neither": **12.1× for 14 % size with statistics alone**; adding bloom
filters on top moves the query by nothing measurable (2.34 → 2.49 ms is noise)
and costs a further ~2 % size.

So the honest conclusion is that **statistics do the work and bloom filters are
a fallback**, not the primary mechanism. Bloom filters are retained only because
they still answer correctly when statistics are absent — a block written with
statistics disabled, or one where a long value was truncated out of the index —
and because they cost ~2 % of size for that.

An absent metric now costs **0.02 ms** instead of 30 ms: nothing is decoded at
all.

### A bug the change exposed

`list_label_values` calls `Scanner::scan` with an **empty** metric name to mean
"no metric filter" — it falls back to a full block scan for blocks that predate
the flush-time label index. The empty string sorts before every real metric
name, so treating it as a value to match pruned the whole block and silently
returned no label values. `test_list_label_values` caught it.

An empty metric name now means "no filter" and keeps every candidate, pinned by
`metric_pruning_treats_an_empty_name_as_no_filter` across all four
statistics/filter combinations.

### Why the statistics test is one-sided

Testing "the needle is outside `[min, max]`" proves **absence**, which is all
that is needed to skip a group. Unlike the time prune, it needs no null check:
min/max cover non-null values only, and a null row cannot contain the value
being searched for. Byte comparison is sound because UTF-8 byte order is
code-point order.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Bloom filters on the metric-name column

Row-group statistics only narrow on **time**. A metrics block interleaves every
metric it holds, so a query for one metric found every row group overlapping the
window and decoded all of them — pruning could only ever help a *range* query,
never a *selectivity* query, which is the common case in an SRE tool.

Bloom filters on `metric_name` and `service_name` answer "could this row group
contain this value?" without decoding a page.

### Result

200 metrics × 500 points in one block, queried for one metric of 200, measured
through the production `Scanner::scan` path:

| | without bloom | with bloom | change |
|---|---|---|---|
| block size | 1.47 MB | 1.49 MB | **+1.5 %** |
| block write | 44 ms | 59 ms | **+33 %** |
| single-metric query | 31.98 ms | **2.83 ms** | **11.3×** |

Both paths return the same 500 points, asserted by the benchmark. A query for a
metric that is not in the block decodes nothing at all and returns empty.

### Why the write cost is worth paying

+33 % on a 100 000-row block is ~15 ms. At the default 1 M-row block size that is
roughly 150 ms extra per block, and blocks are written every few minutes. Against
an 11× read improvement on the query that an SRE dashboard actually issues, that
is not a close call — but it is a real cost and is stated rather than omitted.

Filters are enabled **per column**, not globally, so `labels` (already the
largest column) does not pay for a filter nothing reads.

### Soundness, and the fail-open rule

A bloom filter may report a false positive but must never report a **false
negative** — a false negative silently drops data from a query result. Every
condition that cannot be evaluated keeps the row group:

- no filter on that column for that group,
- an unreadable filter,
- the column absent from the schema.

So the worst case is exactly the behaviour that existed before bloom filters.
Pinned by `bloom_pruning_never_drops_a_row_group_that_contains_the_metric`,
`bloom_pruning_reduces_the_row_groups_read`,
`bloom_pruning_drops_everything_for_an_absent_metric`,
`bloom_pruning_fails_open_when_no_filter_exists` and
`bloom_pruning_fails_open_for_an_unknown_column`.

### Backwards compatibility

The page index is loaded with `PageIndexPolicy::Optional`, not `Required`:
blocks written before this change have no page index and must still be
readable. `bloom_pruning_fails_open_when_no_filter_exists` writes a block with
the *old* writer properties and asserts every row group is kept.

### Why `service_name` has no bloom conjunct

Rows are grouped by metric and resource, so the row-group statistics prune on
service *exactly* and for free — strictly better than a filter that can only be
probabilistic.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Service-level row-group pruning

Since `metric_name` row-group statistics are exact and free, only the
**service** dimension is left to prune on. That is available without
re-ordering any rows either: rows are already written
grouped by `(metric, resource)`, so `service_name` statistics are exact per row
group too.

Probing a production-shaped block (one metric, eight services, one row group
each, service in the dotted OTLP resource key `service.name`) gives:

```
metric_name:   rg0..rg7  min == max == "http_requests"
service_name:  rg0..rg7  min == max == "svc-0" .. "svc-7"
```

Both exact. So `Scanner::scan` now takes an optional service, taken from the
query's `service.name` equality matcher, and prunes on it after pruning on the
metric.

### Result

Multi-tenant shape — one metric, eight services, querying `service.name="svc-3"`:

| | time | rows decoded |
|---|---|---|
| without the service predicate | 2.05 ms | 4 000 (whole block) |
| **with service pruning** | **0.43 ms** | 500 |

**4.8×**, and metric-only pruning cannot help here at all: all eight row groups
are the same metric, so this is entirely the service dimension.

### Only equality matchers, and only the real label

- `!=` and `=~` are ignored. They exclude some rows rather than identifying one
  group, so there is nothing safe to prune from.
- The underscored `service_name` spelling is **not** treated as an alias. No
  series carries that label — the resource attribute is stored under the
  dotted OTLP key — so such a matcher matches nothing at row level, and
  honouring it here would prune the block on a value no row can hold. Caught
  by live probing: the alias returned 0 series, which is correct only because
  the matcher matches nothing, not because pruning worked.

### Not over-pruning

The failure mode that matters is dropping a row group that *does* hold the
requested service, which is silent data loss. `test_service_selector_matches_the_unfiltered_query`
builds a two-service × two-host fixture with the dotted key and asserts
`cpu{service.name="web"}` returns exactly the web series, that an absent
service returns none, and that the fixture itself is production-shaped — the
existing `setup_with_data` fixture uses the underscored key, so it would have
made the test pass for the wrong reason. `service_pruning_is_exact_in_both_directions`
does the same at the row-group level.

Verified live: `http_requests_total_0{service.name="load-generator"}` returns
the same 30 series as the unfiltered query, and a nonexistent service returns
none.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Column projection on the metrics scan

A metrics block has 15 columns; the scan decoded all 15 to use 7. The eight
skipped — `metric_kind`, the six k8s dictionaries and `resource_attributes` —
are per-row data a range query never touches.

The scan now projects to `timestamp_ns`, `metric_name`, `service_name`,
`labels`, `value_float`, `value_int`, `value_complex`.

### Result

200 metrics × 500 points in one block:

| query shape | no projection | projected | change |
|---|---|---|---|
| single metric of 200 (1 row group) | 2.47 ms | 2.33 ms | −6 % |
| one service of 8 (1 row group) | 0.40 ms | 0.34 ms | −15 % |
| **full-block scan, 100 000 rows** | **53.10 ms** | **38.13 ms** | **−28 %** |

The target was a 30 % improvement for all three signals. **It is not met, because
metric and service pruning came first.** They mean a typical query decodes a *single* row group, so the skipped
columns were never where the time went. Projection pays in proportion to how
many row groups a query must read — a wide panel or an old block — which is
the −28 % row.

### Two things that made it safe

**Indices resolved by name.** The decoder indexes columns positionally, and a
projection renumbers them. Every index is now looked up from the batch's own
schema once per batch. Hard-coded positions against a projection is exactly how
a projection silently starts reading the wrong column.

**The projection is intersected with the file's schema.** `with_projection`
errors on a column the block does not have, and blocks written by older builds
may lack one — so intersecting keeps them readable.

### The failure mode is silent, so the test is adversarial

A wrong mask does not crash: `labels` and `value_complex` are both `Utf8`, so
swapping them type-checks and returns wrong data. `projection_does_not_change_decoded_points`
compares timestamps and values against a full unprojected decode and asserts
each point's `host` label matches the series it came from.

Verified to **fail** for both a plain type mismatch and the same-typed
`labels`/`value_complex` swap.

Labels are compared by content rather than by fingerprint against
`StorageModel::row_to_point`, because the two decoders legitimately differ
there: the scan merges the dedicated `service_name` column back into the series
labels, while `row_to_point` leaves it in the resource attributes. Comparing
them would have asserted a difference that was already there.

### Reproducing

```bash
cargo run --release -p parqtel-core --example bench_bloom
```

## Series dictionary for the `labels` column: measured, not worth doing

Replacing the per-row JSON `labels` column with a series-ID dictionary was
proposed on the reasoning that *"zstd must re-compress the same text per row"*.
It would have been the largest storage change, with an on-disk format change
behind it.

**It was measured instead of built, and the premise is false.**

100 000 rows, 200 series, 5 000 rows/row-group:

| configuration | `Utf8` (current) | `Dictionary(Int32, Utf8)` | change |
|---|---|---|---|
| zstd (production) | 810 614 B | 810 678 B | **0.0 %** |
| uncompressed | 1 784 431 B | 1 784 495 B | **0.0 %** |
| write time | 0.034 s | 0.035 s | wash |

Reading the encodings back out of a block written with the **plain `Utf8`
column**:

```
"labels"  ["PLAIN", "RLE", "RLE_DICTIONARY"]
```

**Parquet dictionary-encodes string columns by default.** The Arrow-level field
type only decides whether *Arrow* does the encoding; Parquet applies the same
`RLE_DICTIONARY` either way. A series dictionary would therefore add an encoding
that is already present, and zstd compresses what is left — 9 MB of raw labels
JSON becomes 810 KB.

So: no schema version bump, no migration, no v1-read/v2-write reader. The
expensive, risky work is avoided rather than done for nothing.

The same conclusion applies to `metric_kind`, the other plain-`Utf8` per-row
column.

### What does survive

The per-row `labels.to_json()` on the write path and the per-series
`from_json()` on the read path are real **CPU** costs. The read side is already
covered by the per-chunk label cache from `perf(storage)`. The write side was
not measured here; `parqtel_flush_duration_seconds` is the metric to watch if
anyone wants to quantify it.

### Evidence kept

`probe_labels_size` is retained rather than deleted, both as the record of why
this was closed and as a guard for anyone who revisits the assumption.

```bash
cargo run --release -p parqtel-core --example probe_labels_size
```
