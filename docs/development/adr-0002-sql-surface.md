# ADR 0002: SQL surface — measure DataFusion before hand-rolling

Status: proposed (Phase 2 decision spike)

## Context

Parqtel's query engine is a purpose-built PromQL evaluator. It has no
relational operators, joins, subqueries or CTE support. Parseable exposes SQL
through DataFusion, which is its main breadth advantage.

A hand-written SELECT subset would still need a parser, logical plan, projection
pushdown, aggregation, sorting, limits and a type system. Joins and window
functions would be a second project. The “no DataFusion” plan in the comparison
report is therefore the expensive path, not the cheap one.

## Decision

Run a measured spike before committing:

1. Build a feature-gated (`sql`) prototype that registers Parquet blocks as a
   DataFusion table provider and executes `SELECT … FROM … WHERE … GROUP BY …
   ORDER BY … LIMIT`.
2. Record default-binary size, compile time, dependency count, query latency on
   the existing corpus, memory under a 10M-row scan, and whether joins/window
   functions work without writing planners.
3. Compare with a deliberately limited hand-written SELECT subset on the same
   scanner.

Go/no-go:

- **Use DataFusion** if the default binary stays under the 25 MB soft budget
  (feature-gated build may exceed it), query latency is within 2× the PromQL
  path, and it removes the need for a relational planner.
- **Hand-roll** only if DataFusion cannot meet the binary/size constraints and
  the required surface is provably SELECT/FROM/WHERE/GROUP BY/ORDER BY/LIMIT
  with no joins.

No SQL code lands in `main` until this ADR is accepted.

## Surface and safety

- `POST /api/v1/sql` (or `GET ?query=`) behind the read scope.
- Pushdown: time range, metric name, service/label predicates; cap rows and
  bytes; reuse the scan admission budget.
- Never expose a raw filesystem path or a write statement.
- Arrow Flight/FlightSQL is a separate opt-in feature and follows the same
  admission and memory rules.

## Consequences

- If DataFusion wins, the binary grows only in the `sql` build and the query
  engine remains the default for PromQL/dashboards.
- If hand-rolled wins, joins/subqueries/BI-tool compatibility are explicitly out
  of scope until a later ADR.
