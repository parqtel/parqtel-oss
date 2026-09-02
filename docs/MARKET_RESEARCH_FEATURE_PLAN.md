# Parqtel OSS — Market Research & Feature Plan (2026)

**Author:** Product Design & Analysis
**Status:** Proposal — input for roadmap planning
**Date:** September 2026

---

## 1. Executive Summary

The OSS observability market has consolidated around three truths in 2026:

1. **OpenTelemetry is the graduated, de facto standard** (CNCF graduation May 2026; #2 velocity project behind Kubernetes). OTLP ingestion is now table stakes — vendors without it are legacy.
2. **AI workloads are reshaping telemetry economics.** AI-native companies generate ~2.4× more telemetry; LLM/agent calls fan out into huge span counts and introduce new signals (tokens, cost, output quality). The market leaders (SigNoz, Datadog) have pivoted roadmaps to "agent-native observability."
3. **Operating cost decides OSS adoption.** Teams evaluate self-hosted tools on total infrastructure footprint (stateful dependencies, upgrades, backups) — not feature checklists. Single-binary, small-footprint backends win the segment that ClickHouse-based stacks price out.

Parqtel is unusually well-positioned for truths #1 and #3 (OTLP-native, single ~15 MB binary, zero-dependency, Parquet-open storage, and — notably — **7 MCP servers already shipped**, putting it ahead of most of the market on #2's agent-native axis). But it has first-generation gaps in the capabilities buyers now screen for: gRPC ingestion, span-metrics/RED, service maps, dashboards, and LLM-span support.

This document benchmarks the market, maps Parqtel's gaps, and proposes a prioritized feature list organized around Parqtel's defensible position: **the lightweight, agent-native, single-binary observability backend that stores open Parquet.**

---

## 2. Market Landscape (2026)

### 2.1 The competitive field

| Tool | Architecture | Stateful deps (self-hosted) | Position |
|---|---|---|---|
| **SigNoz (OSS)** | Unified OTLP platform | ClickHouse + misc services | Full APM parity player; moving upmarket to AI-scale telemetry |
| **Prometheus + Grafana** | Composable metrics-first | Prometheus + Grafana (+ Thanos/Cortex for LTS) | Metrics-only; traces/logs need separate backends |
| **Elastic APM** | ES stack extension | Elasticsearch + Kibana + APM Server | For teams already on ES; license complexity |
| **Uptrace** | Unified OTel | ClickHouse + PostgreSQL + Redis | Multi-signal APM; heavy dependency footprint |
| **Jaeger v2** | Tracing backend | ES/OpenSearch/Cassandra + collector | CNCF-graduated tracing specialist |
| **Zipkin** | Tracing backend | Pluggable (Cassandra/ES/MySQL) | Minimal tracing; no auth, no metrics/logs |
| **VictoriaMetrics** | TSDB | Single binary (standalone) | The footprint benchmark for "lightweight" at scale |
| **Grafana Loki/Mimir/Tempo** | Composable OSS stack | Object store + multiple components | Grafana-ecosystem default |
| **Quickwit** | Search on object storage | Object store | Log search cost disruptor (Parquet-native lineage) |

### 2.2 What buyers now screen for (2026 criteria)

From current market comparisons and practitioner evaluation guides:

1. **OTLP over gRPC and HTTP** — SDKs default to gRPC (:4317). HTTP-only ingestion forces collector sidecars or SDK reconfiguration; it's a silent dealbreaker.
2. **RED/USE out of the box** — auto-derived Rate/Errors/Duration from traces; teams expect service dashboards without hand-building PromQL.
3. **Cross-signal correlation** — jump from alert → trace → logs in context. The Atlassian-scale lesson: humans shouldn't be correlation engines.
4. **Agent-native access (MCP)** — exposing telemetry to coding agents (Claude Code, Cursor, internal agents) is now a purchase criterion, not a novelty.
5. **LLM/GenAI telemetry** — `gen_ai.*` semantic conventions, token/cost tracking, quality scoring; million-span trace rendering.
6. **Self-hosted operating cost** — count every stateful dependency, queue, and object store; then the engineering time to scale/backup/upgrade them.
7. **Security & tenancy in OSS** — teams now *verify* which controls are in the open edition rather than assuming; opaque "enterprise-only" baselines lose trust (the Zipkin lesson: no built-in auth = production blocker).
8. **Open storage formats** — Parquet-on-disk means the data outlives the tool (query with DuckDB, Spark, Athena). A rising adoption argument as data-sovereignty concerns grow.

### 2.3 Trend signals (Aug–Sep 2026)

- CNCF platform-engineering content: internal platform teams want self-service observability with low ops burden.
- Observability-sovereignty content (EU): data locality + open formats matter; air-gapped capability is rare and valuable.
- SigNoz's "observability for the AI era": MCP server, in-product AI assistant (Noz), agent-native dashboard schema built on **CNCF Perses** spec, 100k-span flame graphs, LLM span filters. This defines where the unified-APM leaders are going.
- OpenTelemetry roadmap signals: profiling signal, GenAI semconv, OTel Arrow (columnar OTLP transport for throughput), Weaver (schema governance), Injector (zero-code instrumentation).
- Predictive autoscaling / GPU workload content: the AI infra wave is generating exactly the high-cardinality, high-volume telemetry Parqtel's columnar storage handles well.

---

## 3. Parqtel Position: Where It Already Wins

Before listing gaps, the honest asset inventory — these are differentiators to defend, not rebuild:

| Asset | Market status |
|---|---|
| **Single binary, ~15 MB, zero runtime deps** | Beats Uptrace (3 stateful systems), SigNoz (ClickHouse cluster), Elastic (full ES stack). Only VictoriaMetrics competes on footprint — and it's metrics-only. |
| **Parquet as the storage format** | Open, queryable by external tools (DuckDB/Spark/anything Arrow). Aligns with sovereignty + data-outlives-tool trend. SigNoz/Uptrace use ClickHouse (queryable but heavier); Prometheus TSDB is closed chunks. |
| **7 MCP servers shipped** (Slack, PagerDuty, Jira, Notion, Discord, GDocs, Parqtel-query) | Ahead of market. SigNoz shipped its first MCP server only in 2026. This is Parqtel's agent-native head start. |
| **Built-in AI RCA, postmortems, rule proposals** | In-product AI is SigNoz-Cloud-tier functionality; having it in OSS is distinctive. |
| **Cross-signal correlation API** (`/v1/correlate`, ±window dimension join) | Rare in OSS; the "humans shouldn't be correlation engines" argument is mainstream now. |
| **Zero-dependency embedded UI** (works air-gapped) | No OSS competitor ships a console with zero external requests; sovereignty deployments notice. |
| **PromQL + Prom API compatibility** | Drop-in for existing Grafana dashboards and the massive PromQL skill base. |
| **K8s-aware schema** (dedicated correlation columns) | Fast equality filters on service/pod/namespace — good bones for RED and service-map features. |

**Positioning statement:** *Parqtel is the single-binary, agent-native observability backend for teams that want OTLP in, open Parquet out, and zero infrastructure drama — the "VictoriaMetrics of all three signals, with MCP built in."*

---

## 4. Gap Analysis (verified against codebase)

| # | Gap | Market evidence | Severity |
|---|---|---|---|
| G1 | **No OTLP gRPC ingestion** (HTTP only) | OTel SDKs default to :4317 gRPC; every competitor takes both. Forces collector for direct-SDK users. | Critical |
| G2 | **No span-metrics / RED auto-derivation** | Table stakes in SigNoz/Elastic/Uptrace ("out-of-the-box RED dashboards"). Parqtel has trace data + recording-rule machinery but no bridge. | Critical |
| G3 | **No service map / dependency topology** | Standard APM surface (Jaeger, Zipkin, all unifiers). Parqtel has parent/child span data to derive it. | High |
| G4 | **No saved dashboards** (ad-hoc UI queries only) | Every competitor persists dashboards; CNCF Perses is the emerging open spec; agents need a dashboard schema to operate on. | High |
| G5 | **No LLM/GenAI span support** | `gen_ai.*` semconv, token/cost panels, LLM-span filters are the #1 roadmap item at market leaders. Parqtel's RCA engine + MCP is the right substrate. | High (rising fast) |
| G6 | **No auth/RBAC/API keys in OSS** | "Verify what's in the open edition" is now standard procurement practice; Zero-trust self-hosting requires at least API-key auth + read-only users. | High |
| G7 | **No trace sampling controls** | Jaeger's differentiator (head/tail sampling). Without tail sampling, LLM-era span volume is unmanageable. | Medium-High |
| G8 | **No profiling signal** | OTel profiling GA'd; one signal Parqtel's Parquet schema could host naturally (stack traces = high-compression columnar data). | Medium (fast-follow) |
| G9 | **No SLOs / error budgets** | Market moved from "alerts on thresholds" to SLO-driven ops; alert engines without SLOs feel 2019-era. | Medium |
| G10 | **No anomaly detection** (threshold rules only) | Claimed in marketing; only threshold conditions exist. Either ship baseline-deviation detection or fix the claim. | Medium |
| G11 | **Trace memory-buffer absence** (spans invisible until block flush) | Competitors show traces immediately; Parqtel's buffer asymmetry is a demo-killer and on-call annoyance. | Medium (cheap fix) |
| G12 | **No multi-tenancy** | Blocks hosted-platform & MSP use; fine for single-team self-hosted (keep scope honest). | Low-Medium |
| G13 | **No OTel Arrow receiver** | Throughput advantage (columnar OTLP); niche but differentiating for edge→center pipelines. | Low |
| G14 | **No webhook/alert routing engine** (Alertmanager parity) | Teams glue Prometheus Alertmanager; native routing/mute-timings/inhibition closes a familiar gap. | Medium |

---

## 5. Prioritized Feature List

Priorities: **P0** = adoption-blocking / positioning-critical · **P1** = competitive necessity within 2 quarters · **P2** = differentiation · **P3** = ecosystem/fast-follow. Effort is relative T-shirt sizing against this codebase.

### P0 — Remove adoption blockers (this quarter)

| Feature | What | Why now | Effort |
|---|---|---|---|
| **F1. OTLP gRPC ingestion** (`:4317`, tonic) | gRPC server alongside HTTP routes; same decode path; health per OTLP spec. | The silent dealbreaker (G1). Every OTel SDK defaults here. Without it, "OTLP-native" marketing overpromises. | M |
| **F2. Trace memory buffer** | Mirror metrics/logs: spans queryable immediately post-ingest. | Fixes the asymmetry on-call engineers actually feel (G11); cheap, visible. | S |
| **F3. Span-metrics bridge (RED)** | Recording-rule engine derives `requests_total`, `errors_total`, `duration` histograms per service/operation from spans; persist as normal metrics. | Turns Parqtel from "storage backend" into "APM" in one feature (G2). Reuses: recording rules + correlation columns. | M-L |
| **F4. Ship RED service dashboard** (minimal) | One prebuilt UI view: per-service rate/errors/p95 from F3. | F3 without a surface is invisible. Proves the loop. | S-M |

### P1 — Competitive necessity (next 2 quarters)

| Feature | What | Why | Effort |
|---|---|---|---|
| **F5. Service map** | Derive service→service edges from span parentage + `peer.service`/`server.address` attrs; render force/adjacency graph in UI (canvas, no libs). | Standard APM surface (G3); the K8s columns already carry the data. | M |
| **F6. Saved dashboards (Perses-inspired schema)** | `page → sections → panels → queries → variables` JSON schema; store as Parquet-row or JSON sidecar; UI CRUD + import/export. | Users need to save what they build (G4); agents need a schema to operate on (agent-native trend); adopting Perses-shape keeps door open for Grafana interop. | M-L |
| **F7. Auth & API keys in OSS** | Bearer API keys (ingest/write/read-only scopes) + optional static-user login for UI; config-file or simple store. Reverse proxy stays for SSO/RBAC. | Trust + procurement reality (G6). Scope honestly: keys, not enterprise RBAC. | M |
| **F8. GenAI span support (LLM observability)** | Ingest/store `gen_ai.*` semconv attrs (they already flow as span attributes); UI: LLM-span filter, token/cost columns, model facet; alert rules on tokens/cost. | The fastest-moving market requirement (G5). Parqtel's Parquet schema needs no migration — attrs ride along. Surface + rules only. | M |
| **F9. Tail sampling controls** | Configurable tail-sampling in ingest (by error/latency/probability/rate), pre-rotator. | Span-volume economics (G7); prerequisite for AI-scale telemetry claims. | M |
| **F10. Alert routing (Alertmanager-lite)** | Routes (severity→webhook/MCP), silences/mute windows, inhibition rules; reuse MCP servers as sinks. | Familiar mental model; MCP sinks make it *more* capable than vanilla Alertmanager (G14). | M |

### P2 — Differentiation (choose deliberately)

| Feature | What | Why | Effort |
|---|---|---|---|
| **F11. Agent-native everything** | Expand `parqtel-mcp-parqtel` tools to cover new features (RED, service map, dashboards, SLOs); publish MCP tool schemas; "agents can run the platform" docs + examples. | Parqtel's head start here is real but underexploited. This is the *positioning*, not a feature list item. | S-M (continuous) |
| **F12. Diff-observability: link telemetry to deploys/PRs** | Accept deploy/PR annotations (env or API); overlay markers on charts; alert evidence includes recent deploys. | The Atlassian/SigNoz "issue → change that caused it" loop; rarely in lightweight OSS. Correlation columns make joins natural. | M |
| **F13. SLOs & error budgets** | SLO definitions (indicator query, target, window); burn-rate alerts; budget panels. | Modern on-call standard (G9); pairs with F10. | M-L |
| **F14. Profiling signal (OTel profiling)** | New Parquet schema for profile trees; ingestion via OTLP profiling; flamegraph view (canvas). | One more signal nobody lightweight has (G8); columnar compression suits stack traces. | L |
| **F15. Anomaly detection rules** | Baseline (rolling median/MAD) condition type in the alert engine; seasonal-naive is fine. | Fixes marketing honesty (G10) and adds real value over thresholds. | M |
| **F16. Object-storage cold tier** | Optional s3:// data_dir tier; index sidecar stays local; scan via range-GET. | The Quickwit/VictoriaMetrics cost play; Parquet-native makes this natural. | L |
| **F17. OTel Arrow receiver** | Ingest OTAP records → write directly into Arrow writers (no proto decode). | Throughput + edge story (G13); internal pipeline is already Arrow. | M-L |

### P3 — Ecosystem & trust

| Feature | What | Why | Effort |
|---|---|---|---|
| **F18. Docker/Helm parity pass** | Ship multi-arch images + Helm values for every P0/P1 feature; compose profiles (`--profile full`, `--profile minimal`). | Adoption friction audit. | S-M |
| **F19. Prometheus remote-write receiver** | Accept remote_write for teams migrating from Prom TSDB. | Widens the ingestion funnel beyond OTLP. | M |
| **F20. Grafana Parquet datasource docs** | Document querying Parqtel data dir from Grafana's Arrow/Parquet datasource for exit-ramp analytics. | Reinforces "data outlives tool" sovereignty story. | S |
| **F21. OpenAPI → spec tests parity** | Extend `/oas` to cover every new endpoint; contract tests in CI. | Enterprise procurement evaluates API hygiene. | S |
| **F22. Load-test harness for AI-scale traces** | 100k-span trace + 2.4×-telemetry benchmark suite; publish numbers. | Claims need receipts; benchmark the exact market narrative. | M |

### Explicit non-goals (avoid the trap)

- **Full ClickHouse-style columnar query engine** — the point is being the lightweight option.
- **Enterprise RBAC/SSO/multi-tenancy in OSS core** — keys (F7) yes; role hierarchies stay commercial. (G12 stays out.)
- **Frontend framework rewrite** — the zero-dependency console is a moat, not a liability.
- **eBPF auto-instrumentation** — different product category; OTel Injector handles zero-code.
- **Chasing SigNoz Cloud parity** — managed AI features are their game; Parqtel's game is local-first + agents.

---

## 6. Sequenced Roadmap Proposal

```
Now ────────────── Q+1 ──────────────────── Q+2 ─────────────────── Later
P0: gRPC ingest     P1: GenAI spans          P2: profiling          P2: object tier
P0: trace buffer    P1: tail sampling       P2: SLO/burn alerts     P3: remote-write
P0: span-metrics    P1: alert routing       P2: anomaly rules
P0: RED dashboard  P1: auth keys           P2: deploy correlation
                   P1: service map
                   P1: saved dashboards
Continuous: F11 agent-native expansion (every feature lands with MCP tool coverage)
```

**Themes per quarter:** Q0 "APM credibility" → Q1 "AI-era readiness & trust" → Q2 "Differentiation" → sustained agent-native leadership.

---

## 7. Success Metrics

| Dimension | Metric | Target (2 quarters post-P1) |
|---|---|---|
| Adoption | Direct OTel SDK (no collector) installs possible | gRPC path used in >30% of ingest payloads |
| APM credibility | Services showing RED without hand-written PromQL | 100% of traced services auto-derive span-metrics |
| Agent-native | MCP tool invocations per install | Baseline + dashboards/RED tools shipped |
| AI-era | LLM spans ingested with `gen_ai.*` attrs | Rendered, filterable, alertable |
| Trust | OSS edition auth | API-key auth usable without reverse proxy |
| Footprint (defend) | Binary size / deps / RSS at 1k samples/s | ≤16 MB / 0 stateful deps / no regression |
| Community | OSS comparison pages listing Parqtel | Being "the lightweight option" in 2+ mainstream comparisons |

---

## 8. Assumptions & Risks

- **Scope risk:** P0+P1 is ~a quarter of focused work for this codebase; anything more dilutes the lightweight position.
- **The VictoriaMetrics lesson:** footprint *is* the product. Every feature must pass the "does this add a stateful dependency?" gate — answer must stay no.
- **GenAI attr drift:** semconv is still stabilizing; store attrs opaquely (already true), render known keys, don't hard-code schemas.
- **Dashboards scope creep:** Perses-*inspired*, not Perses-*compatible* on day one; compatibility can come via export.
- **Commercial boundary:** auth keys in OSS must not cannibalize the commercial RBAC story — keys ≠ users.

## 9. Sources

- CNCF blog (Sep 2026): platform engineering maturity; OTel graduation post (Aug 31, 2026)
- SigNoz (Aug 2026): "Top 6 Open Source APM Tools in 2026"; "Building observability for the AI era" (agent-native, MCP, Noz, 100k-span traces, 2.4× telemetry stat); new trace detail view; agent-native dashboards on Perses schema
- OpenTelemetry: graduation criteria, profiling signal, GenAI semconv, OTel Arrow/Weaver/Injector roadmap signals
- Market comparison sets (SigNoz comparisons, 2026 editions): evaluation criteria — OTLP support, RED, log correlation, service maps, dashboards/alerting, operating cost, open-edition security verification
