# Head-to-head benchmark method

The goal is a result a third party can reproduce in a day, not a marketing
number.

## Setup

- One host, one corpus, one client. Record CPU model/count, RAM, disk, OS, and
  both engine versions in the output.
- Same ingest payload and retention for both engines. Let each engine finish
  compaction before measuring.
- Warm the page cache with one untimed run; measure 10+ runs and report p50.

## Query set

`scripts/bench/comparison-queries.example.json` has the shape. Keep the
questions identical even when the dialect is not:

| Question | Parqtel | Parseable |
|---|---|---|
| one series | PromQL selector | `SELECT … WHERE … LIMIT` |
| group by | `sum by (env) (rate(...))` | `GROUP BY env` |
| log search | ParqtelQL predicate | SQL `WHERE` |
| p99 latency | `histogram_quantile` | `quantile`/approx |

## Run

```bash
./scripts/bench/compare_queries.py scripts/bench/comparison-queries.example.json \
  --parqtel http://127.0.0.1:8080 \
  --parseable http://127.0.0.1:8000 \
  --runs 10 --out comparison-results.json
```

Publish `comparison-results.json` and the host shape beside it. Do not report a
ratio without both raw files; do not compare a laptop run to a cluster claim.
