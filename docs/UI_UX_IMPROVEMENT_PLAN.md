# Parqtel Console — UI/UX Modernization Plan

**Author:** Principal UI/UX Design
**Status:** Proposed
**Applies to:** `parqtel-server/src/ui.html` (embedded console served at `/ui`)
**Hard constraint:** Zero impact on parqtel-oss server performance; keep the single-file, embedded, dependency-free architecture.

---

## 1. Executive Summary

The Parqtel console is a single-file vanilla-JS dark UI (~118 KB raw / 29.5 KB gzipped) embedded in the binary and served with gzip + ETag caching. This architecture is a **performance asset** and must be preserved. The console already has a strong skeleton — virtualized log/table rendering, a correlation feature, keyboard shortcuts, and AI-assisted RCA — but it is held back by dead dependencies, developer-only query affordances, missing navigation state, unfinished features, and accessibility failures.

This plan modernizes the UI to market-leader standards (Grafana, Datadog, New Relic, SigNoz) across 6 phases, each independently shippable. Every phase carries an explicit performance budget; several phases actually **reduce** payload and runtime cost (removing a dead 47 KB CDN charting library, eliminating render-blocking external fonts).

**Top 10 findings:**

| # | Finding | Severity | Phase |
|---|---------|----------|-------|
| 1 | uPlot (47 KB JS + CSS from unpkg CDN) is loaded but **never instantiated** — dead render-blocking code | Critical | 0 |
| 2 | Google Fonts loaded from CDN — render-blocking, breaks air-gapped/offline use | Critical | 0 |
| 3 | No URL state — refresh/share loses query + time range (all market leaders deep-link) | Critical | 2 |
| 4 | No overview/home — users land on an empty metrics pane | High | 2 |
| 5 | Search requires PromQL literacy from the first keystroke; no guided query builder | High | 3 |
| 6 | Emoji icons (📈🪵🧬🔔) — inconsistent cross-platform, unprofessional | High | 1 |
| 7 | `--text-muted #3d5166` on `#070a0e` = **2.4:1 contrast — fails WCAG AA** (4.5:1) | High | 1/5 |
| 8 | Alert Evidence tab shows literal placeholder "Chart loads with metric data" | High | 4 |
| 9 | Accessibility: 12 ARIA attributes total, no focus traps, no reduced-motion support | High | 5 |
| 10 | Pipelines & Recording Rules APIs exist but have **no UI** | Medium | 2 |

---

## 2. Current-State Audit

### 2.1 What is already excellent (keep, do not regress)

| Strength | Why it matters |
|----------|----------------|
| Embedded gzip + ETag + `Cache-Control: max-age=3600` | Zero server cost at runtime; instant cached paint. Market-leading pattern for a self-hosted binary. |
| Virtualized rendering | Logs (2,000 rows) and metrics table (1,000 rows) use windowed DOM with absolute positioning — Datadog-scale technique. |
| Hand-rolled canvas charts, ≤50 series cap | Zero chart-library runtime cost; crisp at 2× DPR. |
| Cross-signal correlation (metrics ↔ logs via dimension, ±5s window) | Differentiator; absent from most OSS competitors. |
| AI RCA / postmortem / rule proposals | Differentiator; needs polish, not re-architecture. |
| Keyboard-first culture (j/k, Ctrl+K, Esc, A/R/N) | Power-user parity with Grafana/Datadog. |
| AbortController on queries; loading bar; severity chips with counts | Good async hygiene. |

### 2.2 Findings vs market leaders

**Grafana parity gaps:** no URL state, no time-picker zoom-out controls, no dashboard/panel model (deferred), no annotations.
**Datadog parity gaps:** no unified search with preserved context across signal tabs, no facets sidebar for logs, no saved views.
**New Relic parity gaps:** no guided query builder alongside raw mode.
**SigNoz/Jaeger parity gaps:** no trace list (browse by trace_id with duration/error columns) — current UI dumps raw spans of "recent traces" without trace grouping; no minimap; no service legend in waterfall.
**WCAG 2.2 AA failures:** muted text 2.4:1; `:focus-visible` exists but no focus traps on slide-over panels; interactive divs without roles; no `prefers-reduced-motion` despite 6 infinite animations.

### 2.3 Bugs & dead code inventory

1. **Dead dependency:** `uPlot` CSS (line 10) + IIFE JS (line 478) loaded; `new uPlot` count = **0**. Charts are hand-drawn canvas.
2. **Alert severity filter logic** (`filteredAlerts`) contains redundant/contradictory double-checks (`sevOn.has('X')` fallback never matches).
3. **Rules toggle** flips a CSS class but never PUTs `enabled` to `/api/v1/rules/{id}` — silently loses state.
4. **`renderRulesView` adds a click listener to `#alert-rules-view` on every fetch** — listener stacking.
5. **Evidence tab** renders static placeholder text instead of data.
6. **Alert polling** runs every 10 s regardless of tab visibility or active signal.
7. **Chart tooltip** does a linear `xs.forEach` scan per mousemove — replace with binary search (matters at 300+ points × 50 series).
8. **Traces empty-state hint** mutates after every query; initial helper text never restored on signal re-entry.
9. **`window.resetAll`/`closeBanner` globals** leak from IIFE for inline `onclick` handlers — inline handlers should be removed.
10. **`logs.heights` cache invalidation** deletes only previously open row — heights map grows unbounded in long sessions.

---

## 3. Design Principles (adopted standards)

1. **Performance is a feature.** The console is the product's face; it must stay dependency-free, embedded, and instant. No framework migration. Budgets in §7 are CI-enforced.
2. **Progressive disclosure.** Guided mode first (query builder, chips, facets); expert mode one keystroke away (raw PromQL/logQL, YAML editor). New Relic / Grafana Explore pattern.
3. **Keyboard-first, pointer-complete.** Every action reachable in ≤3 keystrokes or ≤2 clicks. Shortcuts discoverable via persistent hint bar + `?` help modal.
4. **One information scent per concept.** A metric, a log line, a span, and an alert all link to the same correlated context (±time window) from anywhere. Correlation is the product's moat — make it ambient.
5. **State lives in the URL.** Everything shareable is in the hash. Everything private (theme, saved views) is in `localStorage`.
6. **Accessible by default.** WCAG 2.2 AA contrast, ARIA Authoring Practices patterns for tabs/dialogs/treegrid, focus management, reduced-motion respect.
7. **Terminal aesthetic, product-grade polish.** Keep the monospace, dark-first identity (the #00F5D4 accent is distinctive); replace accidental roughness (emoji, inconsistent radii, ad-hoc spacing) with a tokenized system.
8. **Empty states teach.** Every empty state shows how to get data in (per-signal OTLP snippets), not just "No data".

---

## 4. Target Design System

### 4.1 Tokens (Phase 1)

Consolidate the existing ad-hoc CSS variables into semantic tokens, fix contrast, add missing scales:

```
Color (dark):   --bg-void #070a0e | --bg-surface #0f141c | --bg-raised #161d28
                --text-primary #e2e8f0 (14.9:1 ✓) | --text-secondary #7a8fa6 (5.9:1 ✓)
                --text-tertiary #64748b (4.2:1 — UI/large text only)
                --text-muted #3d5166 → DEMOTED to decorative hairlines/borders only
Accent:         --accent #00F5D4 (keep — brand) | derived focus ring 2px offset
Severity ramp:  trace/DEBUG #64748b · INFO accent · WARN #fbbf24 · ERROR #f87171 · FATAL #ff1a40
                (all verified ≥4.5:1 on surface backgrounds; large-text badges ≥3:1)
Spacing:        4/8/12/16/20/24/32 (existing scale — enforce; audit 67 inline styles)
Radius:         sm 4px (inputs) · md 6px (cards) · full (chips/pills)
Type:           display 13/16 · body 13/20 · label 11/16 (600) · micro 10/14 (mono)
                code 11/16 JetBrains Mono → system mono fallback stack (Phase 0)
Motion:         fast 120ms ease-out (hovers) · base 200ms (panels) · no infinite
                animations unless status semantics (pulse dot); all behind
                @media (prefers-reduced-motion: reduce) → 0ms
Z-index:        base 0 · sticky 10 · dropdown 200 · slide-over 200 · modal 300 · toast 9999
Elevation:      flat + 1px borders (terminal aesthetic) · tooltips get
                0 4px 12px rgba(0,0,0,.5)
```

**Light theme:** mirror palette behind `[data-theme="light"]` + `color-scheme`; default follows `prefers-color-scheme`, user override persisted in `localStorage`. Dark remains brand default.

### 4.2 Icon system (Phase 1)

Replace all emoji with a hand-inlined **SVG sprite** (`<symbol>` defs, currentColor stroke, 16px grid): activity(line-chart), list(logs), git-branch→node graph(traces), bell(alerts), gear, search, chevrons, x, play/pause, star, link, cpu, database, zap, keyboard. ~18 icons ≈ 3 KB. No icon font, no CDN.

### 4.3 Component inventory (Phase 1)

Standardize from the 77 one-off functions into reusable primitives (still vanilla JS):
`Button (primary/secondary/ghost/danger)` · `Input/Select` · `Chip/FilterChip` · `Badge (severity/state)` · `Tabs (APG pattern)` · `SlideOverPanel (focus-trapped)` · `Toast` · `Tooltip` · `EmptyState (illustration + cause + action)` · `Skeleton (for chart/list loading)` · `Modal (?-help, confirm)` · `KVTabsTable (attributes view)`.

---

## 5. Phase Plan

### Phase 0 — Hygiene & Performance Foundation (1–2 days, zero visual change)

**Goal:** remove all external runtime dependencies and fix perf bugs. This phase *reduces* cost.

| # | Change | Impact |
|---|--------|--------|
| 0.1 | Delete uPlot `<link>` + `<script>` (dead code) | −47 KB JS, −1 render-blocking request, air-gap capable |
| 0.2 | Delete Google Fonts; switch to system stacks: `--sans: ui-sans-serif, -apple-system, "Segoe UI"...`, `--mono: ui-monospace, "SF Mono", Menlo, Consolas...` | −2 render-blocking requests, zero FOUT, offline OK. (Optional follow-up: subset JetBrains Mono woff2 inlined as data-URI if brand insists; budget-checked.) |
| 0.3 | Pause alert polling on `document.hidden`; poll 10 s while Alerts view, 30 s otherwise (badge only) | Cuts idle CPU/network |
| 0.4 | Binary-search nearest x in chart tooltip | Mousemove O(log n) instead of O(n) |
| 0.5 | Fix `logs.heights` unbounded growth (LRU cap 500) | Long-session memory |
| 0.6 | Fix rules-view listener stacking (`onclick` assignment instead of `addEventListener`) | Correctness |

**Verification:** embedded size before/after; manual offline smoke test (server on localhost, network disabled); `make lint` + workspace tests.

### Phase 1 — Design System & Visual Refresh (3–5 days)

**Goal:** consistent, accessible, professional surface without changing layout architecture.

- 1.1 Tokenize per §4.1; migrate the 67 inline `style=` occurrences into classes.
- 1.2 Contrast fixes: demote `--text-muted` to decorative; secondary text ≥4.5:1; verify all badge text-on-tint pairs. Run automated contrast audit.
- 1.3 SVG icon sprite per §4.2; replace emoji in tabs/RCA/proposals/toasts.
- 1.4 Component primitives per §4.3; unify buttons/inputs/chips.
- 1.5 `prefers-reduced-motion` support; replace 2 of 6 infinite animations with 2-iteration variants; keep status pulse dot.
- 1.6 Skeleton loading states for chart/list/trace (replaces indeterminate top bar where a region loads).
- 1.7 Polish: focus-visible ring consistent (2px accent, 2px offset), scrollbars already slim (keep), selection color, cursor affordances on interactive rows.

**Deliverable:** same console, visibly crisper; Lighthouse a11y ≥ 90.

### Phase 2 — Information Architecture & Navigation (4–6 days)

**Goal:** findability, shareability, and a front door.

- 2.1 **Hash-based URL state** (no server involvement): `#/overview`, `#/metrics?query=http_requests_total%7Bservice%3D%22api%22%7D&start=…&end=…&step=auto`, `#/logs?…&sev=ERROR`, `#/traces?trace_id=…`, `#/alerts?view=rules`. `history.replaceState` on every filter change (no history spam); full `pushState` on signal switch. Refresh/restored session = same view. **Parity: Grafana/Datadog deep links.**
- 2.2 **Overview pane** (new): stat cards per signal built only from existing APIs — indexed series count (`/api/v1/label/__name__/values`), log volume (`/v1/logs/count`), firing alerts (`/api/v1/alerts`), plus a 6h volume sparkline (canvas, ≤2 KB code). Zero new backend endpoints.
- 2.3 **Left icon rail** (48 px, collapsible to 200 px labels) replacing top signal tabs: Overview · Metrics · Logs · Traces · Alerts · Rules · Pipelines. Frees header for global search. Responsive: rail collapses to bottom tab bar ≤768 px.
- 2.4 **Expose Pipelines & Recording Rules** (APIs `/api/v1/pipelines`, `/api/v1/recording_rules` already exist): read-first tables with status + detail slide-over; create/edit deferred to Phase 4 tooling. Closes feature gap #10.
- 2.5 Header refactor: logo · global search (Ctrl+K) · time picker cluster (moved to header, see 3.x) · status dot · settings. Removes the per-signal toolbar juggling.

### Phase 3 — Query & Exploration Experience (5–8 days) — *the core UX lift*

**Goal:** make the console usable by on-call engineers who don't write PromQL daily.

- 3.1 **Guided query builder (Metrics):** toggle "Builder ⇄ Code" (Grafana Explore pattern). Builder = metric `<select>` (searchable, from `__name__` values) + label filter rows (`key` from `/api/v1/labels`, `value` from `/api/v1/label/{name}/values`, operators `=`/`!=`/`=~`) + live PromQL preview (read-only, click → copies to Code mode). **Value autocomplete closes finding #5** — current UI inserts `key=` and stops.
- 3.2 **Logs facets sidebar** (Datadog pattern) using existing `/v1/logs/fields` + `/v1/logs/field_values`: top fields (service, severity, k8s.*, custom) with value counts; click value → adds chip filter → URL. Keep raw query mode.
- 3.3 **Traces browse list:** group `/v1/traces/search` results by `trace_id` → table (trace, root service, spans, duration, errors ▲). Click → waterfall. Adds service legend (color = service, matches bar colors), minimap (tiny canvas density strip, click-to-jump), and %ile column. **Parity: SigNoz/Jaeger.**
- 3.4 **Time picker cluster** (header): relative presets (keep), absolute range, **zoom out / back / forward buttons**, and `Shift+drag` on chart = zoom (in addition to existing histogram drag). Preserve histogram as the volume context strip (it's good).
- 3.5 **Saved views:** star current state per signal (query + range + filters) in `localStorage`; starred list in search dropdown; URL share button (copies deep link).
- 3.6 **Cross-signal ambient linking:** metric legend/table row → "logs at this window"; log line with trace_id → waterfall (exists — keep); span → "metrics for service"; alert → Evidence tab now *loads* correlated logs + the rule's metric mini-chart (Phase 4 finishes this).
- 3.7 **Unified search semantics:** search bar stays per-signal but **switching signals preserves the time range and relevant context** (service=… carries over). Search dropdown gains: recent queries, saved views, metric names, log fields — grouped, keyboard-navigable (existing AC extended).

### Phase 4 — Alerts, Rules & Workflow Polish (4–6 days)

**Goal:** incident workflow that matches Datadog-grade ergonomics.

- 4.1 **Bug fixes:** severity filter logic rewrite; rules toggle → real `PUT enabled`; delete confirmation modal (not `confirm()`); toasts on all mutations (some exist).
- 4.2 **Evidence tab (real):** fetch rule metric via existing `/api/v1/query_range` (mini canvas chart) + `/v1/correlate`-style log window ±5 min from the alert's start. Removes placeholder.
- 4.3 **Form-based rule editor:** fields (name, severity, type, query via Phase 3.1 builder, condition operator/value, for-durations) + "YAML" escape hatch (keep raw textarea for power users, with client-side YAML lint). Server validation surfaced inline.
- 4.4 **Alert list ergonomics:** group by rule (collapsible), sort severity→recency (keep), severity color-coded left border, relative duration column, bulk ack on selected (keyboard: j/k + A), and `?` shortcut overlay listing all shortcuts (persistent kb-hints bar shown in *all* views, not just Alerts).
- 4.5 **Postmortem viewer:** render markdown into the slide-over with proper focus management; "Copy" and "Download .md" actions; regenerate flow with inline progress (not raw alert).

### Phase 5 — Accessibility, Onboarding & Responsive (3–5 days)

- 5.1 **WCAG 2.2 AA pass:** roles/states per ARIA APG (tabs, tree for spans, dialog for slide-overs, listbox for autocomplete); focus trap + restore in slide-overs/modals; skip-to-content link; `aria-live="polite"` regions for loading/error; ≥44 px touch targets.
- 5.2 Keyboard-only audit script (documented pass/fail checklist in repo); axe-core CI job (dev-dependency only, not shipped).
- 5.3 **Onboarding empty states:** per-signal "Get data in" with copy-paste OTLP endpoint snippets (`curl` / OTel collector config tab), linking to docs/GETTING_STARTED.md.
- 5.4 **Responsive/mobile:** bottom nav ≤768 px; tables → stacked cards; waterfall keeps horizontal pan; no new frameworks.
- 5.5 Light-theme QA (from 1.x) + print stylesheet for postmortems.

### Phase 6 — Backlog (explicitly out of scope for this plan)

Dashboard/panel grids · service map · trace compare (Jaeger-style diff) · SLO/burn-rate widgets · annotation events · multi-tenant RBAC (commercial) · timezone display setting. These require product decisions beyond UI polish and are listed to prevent scope creep.

---

## 6. Performance Budget & Guardrails (CI-enforced)

| Metric | Current | Budget | Enforcement |
|--------|---------|--------|-------------|
| Raw payload | 118 KB | **≤ 160 KB** | CI check `gzip -c ui.html \| wc -c` against `/docs` budget file |
| Gzipped payload | 29.5 KB | **≤ 42 KB** | same |
| External requests at load | 4 (2 fonts CSS+woff2, uPlot JS+CSS) | **0** | grep CI for `http`/`https` src/href in ui.html |
| Server rendering cost | 0 (static embed) | **0 — unchanged** | architecture review gate |
| Chart series / DOM rows | 50 / windowed | keep windowing; rows > 200 must virtualize | code-review checklist |
| Polling when tab hidden | 10 s | **paused** | manual test |
| Cached TTFP (localhost) | < 100 ms | ≤ 100 ms | smoke benchmark |
| Lighthouse (perf/a11y/best-practices) | n/a | ≥ 95 / ≥ 95 / 100 | CI (optional headless) |

**Non-goals (explicit):** no React/Vue/Svelte migration; no bundler in the build path; no server-side templating; no new backend endpoints in Phases 0–5 (2.2 overview composes existing APIs only). The single-file discipline stays; if authoring ergonomics suffer, an *optional* Makefile "assemble" target may concatenate `ui/` fragments — output stays one file, no runtime deps.

---

## 7. Success Metrics

- **Adoption/UX:** new-user first successful query < 60 s (moderated test, target n=5); deep-link round-trip (share → open → same view) 100%; guided-vs-raw query ratio trending toward guided for first-time users.
- **Quality:** axe-core critical violations = 0; keyboard-only task completion (run query, correlate, ack alert) = 100%; contrast tokens all pass AA.
- **Performance:** budgets in §6 all green; no regression in server p50/p99 ingestion/query latency (UI is client-only; verify via existing benchmarks).
- **Consistency:** 0 emoji icons in chrome UI; 0 inline style attributes (except template-generated positioning).

---

## 8. Risks & Mitigations

| Risk | Mitigation |
|------|-----------|
| Single-file growth hurts maintainability | Strict component-primitive reuse; optional build-time concatenation (no runtime change); section lint comments |
| Muscle-memory regression | Phase rule: existing shortcuts are only ever **added to**, never removed or rebound |
| Guided builder drifts from PromQL semantics | Builder always renders the live PromQL preview and round-trips through Code mode; e2e tests against query API |
| Light theme doubles palette QA | Ship dark-first; light theme behind flag until QA'd (Phase 5 gate) |
| Over-ambition (Phase 6 creep) | Backlog explicitly fenced; phase PRs are independently revertible |

---

## 9. Rollout Order & Effort

| Phase | Effort | Ships value standalone? |
|-------|--------|--------------------------|
| 0 Hygiene | 1–2 d | Yes — faster, offline-capable |
| 1 Design system | 3–5 d | Yes — instant perceived quality |
| 2 IA/Navigation | 4–6 d | Yes — overview + deep links |
| 3 Query & Exploration | 5–8 d | Yes — the headline UX lift |
| 4 Alerts/Workflow | 4–6 d | Yes — fixes broken workflows |
| 5 A11y/Onboarding | 3–5 d | Yes — compliance + first-run |
| **Total** | **20–32 days** | |

Recommended sequence: 0 → 1 → 2 → 3 → 4 → 5 with 0/1 merged into the first PR for immediate visible + measurable wins.
