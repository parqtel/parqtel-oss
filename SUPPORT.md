# Support

## Where to ask

| I want to… | Go to |
|---|---|
| Ask how to use Parqtel | [GitHub Discussions](https://github.com/parqtel/parqtel-oss/discussions) |
| Report a bug | [Bug report template](.github/ISSUE_TEMPLATE/bug_report.yml) |
| Propose a feature | [Feature request template](.github/ISSUE_TEMPLATE/feature_request.md) |
| Report a vulnerability | [SECURITY.md](SECURITY.md) — **not** a public issue |
| Contribute code | [CONTRIBUTING.md](CONTRIBUTING.md) |

Parqtel is maintained by volunteers. There is no guaranteed response time, and
there is no paid support offering.

## Before opening an issue

Most questions are answered by:

- [docs/getting-started/getting-started.md](docs/getting-started/getting-started.md) — the first 15 minutes
- [docs/operations/configuration.md](docs/operations/configuration.md) — every knob and its default
- [docs/guides/pql-guide.md](docs/guides/pql-guide.md) — metric and log/trace query syntax
- [docs/operations/troubleshooting.md](docs/operations/troubleshooting.md) — symptoms and the
  `/metrics` counters that explain them

Two things that surprise people most often:

- **An instant query looks back 5 minutes** (`query.lookback_delta_ns`). Older
  points become visible after a flush, which is 2 hours by default
  (`storage.block_duration_secs`). Use `/api/v1/query_range`, or shorten the
  block duration, if you need to see recent history.
- **A range query does not evaluate the final partial step**, so samples newer
  than one `step` before `end` are invisible to `/api/v1/query_range`. See
  [docs/guides/pql-guide.md](docs/guides/pql-guide.md).

## Producing a useful bug report

Include the version, the deployment method, and the relevant `/metrics`
counters. The bug template lists which ones matter for which subsystem. Most
ingest and query problems are explained by six counters:

```
parqtel_ingest_lock_wait_seconds   parqtel_flush_duration_seconds
parqtel_flush_inflight             parqtel_index_pending_writes
parqtel_storage_blocks             parqtel_query_duration_ms
```

## Reporting a vulnerability

Do not open a public issue. Follow [SECURITY.md](SECURITY.md).
