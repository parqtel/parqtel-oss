# CI/CD Pipeline

## Design goals

1. **Fast PR feedback** — contributors get lint/test results in minutes, not an hour. Expensive jobs (Docker, Helm) are skipped unless their inputs changed.
2. **No wasted compute** — jobs that duplicate release work (cross-platform binary builds) were removed from PRs entirely.
3. **Quality releases** — a tag can never publish untested binaries: the release workflow re-runs clippy + the full test suite as a hard gate before any artifact is built or pushed.

## Pipeline layout

### `ci.yml` — pull requests & pushes to `main`

| Job | Runs when | Typical duration |
|-----|-----------|------------------|
| `changes` | always | ~10 s |
| `lint` | Rust sources / manifests changed | ~4–6 min |
| `test` | Rust sources changed | ~5–7 min |
| `msrv` | Rust sources changed | ~4 min |
| `security` | always | ~2 min |
| `helm-lint` | `deploy/charts/**` or `compose/**` changed | <1 min |
| `docker` + smoke test | Dockerfile/sources/deps changed **and** lint+test pass | ~10–15 min |

Typical docs-only PR: ~2 min (security only).
Typical code PR: ~15–20 min wall clock, ~35–45 worker-minutes.

Key mechanics:

- **Concurrency**: `cancel-in-progress` kills superseded runs on the same branch — pushing five commits in a row bills once.
- **Path gating** (`dorny/paths-filter`): Helm lint doesn't run because you edited a README; Docker doesn't build because you touched a chart.
- **Docker smoke test**: after building the image, the job boots it, waits for `/health`, and checks `/metrics`. A distroless image that compiles but doesn't serve fails the PR.
- **`--locked` everywhere**: CI builds exactly what's pinned in `Cargo.lock`; dependency drift fails loudly instead of silently changing the tested surface.
- **`-D warnings` scoped to clippy only**: setting it globally via `RUSTFLAGS` also compiles *dependencies* with warnings-as-errors and breaks spuriously when new rustc lints land upstream.
- **Least privilege**: workflow-level `permissions: contents: read`; no write tokens in the PR lane.
- **Removed from PRs**: the 4-target cross-compile release matrix (linux/amd64+arm64, macOS ×2). Those binaries were uploaded and never used by the PR flow, and `release.yml` rebuilds identical targets on tags anyway. This was ~45–50 min of compute per PR, mostly on 10×-billed macOS runners.

### `release.yml` — tag push (`v*`)

```
test ──┬─► build-binaries (4 targets, --locked)
       ├─► docker-publish (multi-arch, SBOM + provenance attestation)
       │        ├─► trivy-scan (SARIF → Security tab)
       │        ├─► sign-image (cosign keyless)
       │        └─► helm-publish (OCI chart)
       └────────────────────► github-release (needs everything above)
```

The new `test` gate re-runs clippy `-D warnings` + the full workspace suite on the tagged commit. Tags can point at stale commits — without this gate a tag could publish binaries that were never validated.

Supply chain guarantees retained: build provenance attestation, SBOM, cosign signing, Trivy image scan, checksums for every binary asset.

### `scorecard.yml` — weekly OpenSSF Scorecard analysis

## Worker-hour impact (approx., per typical code PR)

| Lane | Before | After |
|------|--------|-------|
| Lint | 6 min | 5 min |
| Test | 8 min | 6 min |
| MSRV | 5 min | 4 min |
| Security audit | 7 min (compiled cargo-audit every run!) | 2 min (prebuilt action) |
| Build matrix | ~48 min (4 targets) | **0 min (moved to release)** |
| Docker | 15 min (always) | 0–15 min (path-gated) |
| Helm | 1 min (always) | 0–1 min (path-gated) |
| **Total** | **~90 min** | **~17–33 min** |

≈ 60–80% reduction in CI compute per PR; wall-clock feedback drops from ~25 to ~12 minutes since heavy lanes are gone.

## Local equivalents

```bash
make lint        # fmt --check + clippy -D warnings
cargo test --workspace   # same suite CI runs
make docker      # same multi-stage build CI validates
helm lint deploy/charts/parqtel   # note: charts live under deploy/, not charts/
```

## Maintenance notes

- **MSRV is 1.86** and lives in three places that must move together: `ci.yml` (`MSRV` env + toolchain matrix), `Cargo.toml` `[workspace.package].rust-version`, and `Dockerfile` `RUST_VERSION`. It's pinned by the lockfile: ICU 2.x (pulled in via `url` → `reqwest`) requires rustc 1.86, so lowering it needs precise dependency pins.
- `rustsec/audit-check` reads `Cargo.lock` only; run `cargo update` deliberately and review the audit diff.
- If runner minutes ever matter less than latency again, the Docker job is safe to promote back to always-on via the `changes.outputs.docker` condition.
