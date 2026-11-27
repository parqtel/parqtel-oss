# Release Process

Parqtel releases are tag-driven and versioned with [Semantic Versioning](https://semver.org/).
One version string drives every artifact; a script writes it everywhere and
verifies that nothing drifted.

| Step | Command | Result |
|------|---------|--------|
| Preview | `make cut-release DRY_RUN=1` | prints next version and every file it would touch |
| Cut | `make cut-release` | commit `chore(release): vX.Y.Z`, annotated tag, push |
| Publish | (automatic) | `.github/workflows/release.yml` builds, signs and releases |
| Verify | `make verify-release` | checks the release, assets, checksums, image, chart, attestation |

## Version source of truth

`[workspace.package].version` in `Cargo.toml` is the only hand-edited version.
`scripts/release/release_tool.py` writes it and every derived site:

| File | Field |
|------|-------|
| `Cargo.toml` | `[workspace.package].version` |
| `Cargo.lock` | the eight `parqtel-*` package entries |
| `deploy/helm/parqtel/Chart.yaml` | `version` and `appVersion` |
| `docs/api/openapi.yaml` | `info.version` |
| `README.md` | the status line (`` `X.Y.Z` is a public beta ``) |
| `docs/getting-started/getting-started.md` | the `ghcr.io/parqtel/parqtel-oss:X.Y.Z` image tag |
| `docs/guides/pql-guide.md` | the `Version` / `Applies to` line |
| `CHANGELOG.md` | newest release heading and its compare links |

`make release-check` (and the `Release Metadata` CI job) fails if any of these
disagree, if a changelog section lost its date, if sections are out of
descending semver order, or if a compare link no longer contains its version.
A missing site is a failure, not a skip: adding a version reference somewhere
means adding it to `_sites()` in the tool.

## Semantic version policy

The bump is computed from conventional commits since the last tag
(`git-cliff --bumped-version`), not typed by hand:

| Commit type | Bump |
|-------------|------|
| `feat:` | minor |
| `fix:`, `perf:`, `refactor:`, `revert:`, `build:` | patch |
| `!` / `BREAKING CHANGE:` | **minor** while `0.x`, major from `1.0.0` |
| `docs:`, `chore:`, `ci:`, `test:`, `style:` | none |

`cliff.toml` sets `breaking_always_bump_major = false`, which is the correct
reading of semver for a `0.x` project: breaking changes may land in a minor
release, and `1.0.0` is a decision, not an accident. Override when the history
is misleading:

```bash
make cut-release BUMP=patch       # force a patch
make cut-release BUMP=1.0.0       # explicit version (must be greater)
scripts/release/release.sh 0.4.0  # same, without make
```

## Changelog

`CHANGELOG.md` follows Keep a Changelog. It has two halves:

1. **Hand-written prose** — highlights, migrations, security notes. Written by
   the person cutting the release.
2. **Generated block** — everything between `<!-- changelog:auto-start -->` and
   `<!-- changelog:auto-end -->`, rebuilt from conventional commits with
   git-cliff. Never edit it by hand: `make changelog` regenerates it. The
   `Release Metadata` CI job regenerates the block and fails when the committed
   copy differs, so a pull request that adds `feat:`, `fix:`, `perf:` or
   `refactor:` commits must include the regenerated block.

The generated block lists user-visible work only: Features, Bug Fixes,
Performance, Refactoring, Reverts, Security, Build. `docs:`, `chore:`, `ci:`,
`test:` and `style:` commits are omitted. Commit subjects are therefore
user-facing text — write them as instructions or outcomes, not as file names.

Refresh locally after a batch of commits:

```bash
make changelog
```

`make cut-release` regenerates the block, folds `[Unreleased]` into
`## [X.Y.Z] - YYYY-MM-DD`, opens a fresh Unreleased section and rewrites the
trailing compare links. The release body on GitHub is that section plus an
install/verify footer (`make release-notes TAG=vX.Y.Z` prints it), so the
release page and the changelog cannot disagree.

## Cutting a release

Prerequisites: clean tracked tree, on `main`, `main` even with `origin/main`,
tag absent, `gh` authenticated (only for verification), git-cliff available
(`make cliff` fetches a pinned binary into `.tools/bin`).

```bash
git switch main && git pull
make cut-release DRY_RUN=1     # inspect the plan
make cut-release               # commit + tag + push
```

`scripts/release/release.sh` then:

1. computes the next version (or takes `BUMP=X.Y.Z`),
2. refuses to run off `main`, with a dirty tree, with an existing tag, or when
   local `main` diverged from `origin/main`,
3. rewrites the changelog and every version site,
4. runs `release_tool.py check` and `cargo metadata --locked`,
5. commits, creates the annotated tag `vX.Y.Z`, pushes `main` then the tag.

The tag push starts `release.yml`, which re-runs **clippy and the full test
suite on the tagged commit** before anything is published. A tag can point at
an untested commit; this gate is what makes that harmless. It also runs the
release-metadata check with `--tag`, so a tag whose version disagrees with
`Cargo.toml` cannot publish.

After the gate: four platform binaries with `.sha256` files, a multi-arch
image (`ghcr.io/parqtel/parqtel-oss`) tagged `X.Y.Z`, `X.Y`, `X`, `latest` and
`sha-<commit>`, build provenance attestation, SBOM, cosign signature, Trivy
image scan, the Helm chart under `oci://ghcr.io/parqtel/charts/parqtel`, the
GitHub release, and a `repository_dispatch` to `parqtel/homebrew-tap`.

Use `--no-push` to inspect the commit and tag locally first.

## Verifying a release

```bash
make verify-release                  # newest GitHub release
make verify-release TAG=v0.4.0
scripts/release/verify-release.sh v0.4.0 --skip-download
```

It asserts: release is published (not draft/prerelease), all eight assets
exist, every tarball matches its `.sha256`, the linux/amd64 binary reports the
tagged version, all four image tags resolve to the same digest, the
attestation verifies with `gh attestation verify`, and the chart version
matches. Missing `docker`/`helm` are reported as SKIP; any failed assertion
exits non-zero.

## Rollback

Releases are immutable by convention; never re-tag the same version.

1. If the workflow failed: fix the cause, delete the tag and release
   (`gh release delete vX.Y.Z --yes --cleanup-tag`), then re-cut the same
   version only if nothing was published. If binaries or the image were
   published, cut the next patch instead.
2. If a published release is bad: publish a fixed patch. Then mark the bad
   release as a prerelease or edit its body with a warning — deleting it breaks
   anyone who pinned `X.Y.Z`.
3. The image keeps its tags; push a corrected patch rather than moving `X`,
   `X.Y` or `latest` back. Immutable tags are what makes digests and
   attestations meaningful.

## CI wiring

| Workflow / job | Role |
|----------------|------|
| `ci.yml` → `Release Metadata` | version sites, changelog structure and generated-block freshness on every PR and push |
| `release.yml` → `Test Release Candidate` | metadata check with `--tag`, clippy, full tests |
| `release.yml` → `GitHub Release` | body from `release_tool.py notes` |

The generated block is refreshed by the contributor or by
`scripts/release/release.sh`, not by a bot commit to `main`. GitHub does not
allow the GitHub Actions integration to bypass a **repository** ruleset (only
an organization ruleset can name it; deploy keys are disabled for this
repository), and a bot pull request cannot satisfy the required review. The
gate therefore runs in the other direction: CI fails when the committed block
is stale, which keeps `CHANGELOG.md` accurate without weakening the branch
protection. If the organization later moves this ruleset to the organization
level, a `changelog.yml` push job can be added and the freshness gate relaxed.
