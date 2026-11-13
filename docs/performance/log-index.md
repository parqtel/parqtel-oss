# Log search: skip index and two-phase scan

Same machine and builds as the section above (thin-LTO release, 300 000 log
records in the dataset, search over the last hour, `limit=100`, newest first).
`logs-search` looks for `status=500`, which is in a fifth of the records;
`logs-search-rare` for `request 150000 handled`, which is in one.

| p50 | before | after | before, 8 clients | after, 8 clients |
|---|---|---|---|---|
| `status=500` (20% of records) | 102 ms | 23 ms | 311 ms | 64 ms |
| `request 150000 handled` (1 record) | 29 ms | 12 ms | 113 ms | 32 ms |

Ingest of logs is unchanged (1.5-1.7 M records/s) and the data directory is the
same size to within 0.5%: the filters add about a byte per row and writing bodies
without a dictionary takes back about as much.

## What each change bought

| Change | Effect on `status=500` |
|---|---|
| Log scans use the cached footer and its page index (they re-read the footer and had no page index, so the row filter could not skip pages) | 102 -> 100 ms (cached footer and plain body column together) |
| Body search looks for the needle's rarest byte with `memchr` instead of comparing a window at every offset | not measured on its own (the machine was busy); it is the top frame of the profile before and absent after |
| Predicate that is only severity + body: count and rank from three columns, decode the other sixteen only for the returned rows | 100 -> 36 ms |
| Memory buffer returns only matching rows instead of cloning every buffered record first | 36 -> 23 ms together with the memchr search (rare term: 24 -> 12 ms) |
| Skip index (granule bloom filters) | rare term 30 -> 24 ms before the buffer change; leaves a dense term alone |

The skip index does nothing for `status=500` because a fifth of the records match:
every granule qualifies. It is for the selective searches that dominate incident
work - a request id, a user, an error code, a `pod=...` attribute.

## Correctness

`test_log_skip_index_does_not_change_scan_results` compares, over a block with
several row groups, the rows, count and volume buckets of a scan with the index
against the same rows without it: rare, common and absent needles, a rare and a
resource attribute, a needle and an attribute that live in different granules,
severities, six windows and both newest- and oldest-first.
`test_exact_log_prefilter_matches_row_by_row_scan` does the same for the two-phase
scan. An index that is not JSON, or counts the wrong number of rows, is ignored.
`kill -9` under load recovered every acknowledged log and span in two runs each.

## Which conditions use the index

Body substrings of three or more bytes, equalities on attributes, resource
attributes, `trace_id`, `span_id`, `service`, `attr.K` and `res.K`, severity and
the time window narrow the granules read. So do the label matchers of
`/api/v1/logs`. `OR`, `NOT`, regexes and `!=` do not, because they are not
necessary conditions, and neither do bare field names when the caller supplies
pipeline-enriched fields. A term present in a large share of the records cannot be
narrowed: `status=500` above is limited by decoding and testing the bodies of the
matching granules.
