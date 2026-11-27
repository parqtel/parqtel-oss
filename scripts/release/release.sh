#!/usr/bin/env bash
# Cut a parqtel-oss release.
#
# One command performs the whole local half of the release:
#   1. computes the next semantic version from conventional commits (or takes
#      an explicit version),
#   2. folds CHANGELOG.md [Unreleased] into a dated section and refreshes the
#      generated commit block,
#   3. writes the version to Cargo.toml, Cargo.lock, the Helm chart, the
#      OpenAPI description and the versioned docs,
#   4. verifies the release metadata contract and the lockfile,
#   5. commits `chore(release): vX.Y.Z`, creates the annotated tag and pushes
#      both. The tag push runs .github/workflows/release.yml, which re-tests,
#      publishes the binaries, image and chart, signs them and creates the
#      GitHub release with this changelog section as the body.
#
# Usage:
#   scripts/release/release.sh                 # auto bump (feat -> minor)
#   scripts/release/release.sh --bump patch
#   scripts/release/release.sh 0.4.0
#   scripts/release/release.sh --dry-run       # print the plan, change nothing
#   scripts/release/release.sh --no-push       # commit and tag locally
#
# Environment:
#   SKIP_GIT_FETCH=1  do not fetch before checking main is up to date.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

TOOL=(python3 scripts/release/release_tool.py)
VERSION=""
BUMP="auto"
DRY_RUN=0
PUSH=1

usage() {
  sed -n '2,25p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bump)
      BUMP="${2:?--bump needs a value}"
      shift 2
      ;;
    --bump=*)
      BUMP="${1#*=}"
      shift
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    --no-push)
      PUSH=0
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    -*)
      echo "error: unknown option $1" >&2
      exit 2
      ;;
    *)
      if [[ -n "$VERSION" ]]; then
        echo "error: version given twice ($VERSION, $1)" >&2
        exit 2
      fi
      VERSION="$1"
      shift
      ;;
  esac
done

if [[ -n "$VERSION" && "$BUMP" != "auto" ]]; then
  echo "error: pass either an explicit version or --bump, not both" >&2
  exit 2
fi

current="$("${TOOL[@]}" version)"
if [[ -n "$VERSION" ]]; then
  next="$VERSION"
else
  next="$("${TOOL[@]}" next --bump "$BUMP")"
fi

if ! [[ "$next" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '$next' is not a semantic version" >&2
  exit 1
fi
if [[ "$next" == "$current" ]]; then
  echo "error: next version equals the current version ($current); nothing to release" >&2
  exit 1
fi

# ── Preflight ───────────────────────────────────────────────────────────────
branch="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$branch" != "main" ]]; then
  echo "error: releases are cut from main (currently on '$branch')" >&2
  exit 1
fi

if [[ -n "$(git status --porcelain --untracked-files=no)" ]]; then
  echo "error: the working tree has uncommitted changes:" >&2
  git status --short --untracked-files=no >&2
  exit 1
fi

if git rev-parse -q --verify "refs/tags/v$next" >/dev/null; then
  echo "error: tag v$next already exists" >&2
  exit 1
fi

if [[ "$PUSH" -eq 1 && "${SKIP_GIT_FETCH:-0}" != "1" ]]; then
  echo "==> fetching origin"
  # Force only the release tags: a locally re-annotated tag (or a stale one)
  # otherwise makes a plain `fetch --tags` fail and hides the real state.
  if ! git fetch --quiet origin main '+refs/tags/v*:refs/tags/v*'; then
    echo "error: git fetch origin failed" >&2
    exit 1
  fi
  ahead_behind="$(git rev-list --left-right --count origin/main...HEAD)"
  read -r behind ahead <<<"$ahead_behind"
  if [[ "$behind" != "0" ]]; then
    echo "error: local main is $behind commit(s) behind origin/main; pull first" >&2
    exit 1
  fi
  if [[ "$ahead" != "0" ]]; then
    echo "error: local main has $ahead unpushed commit(s); push or reset them first" >&2
    exit 1
  fi
fi

if [[ "$DRY_RUN" -eq 1 ]]; then
  echo "==> dry run: $current -> $next"
  echo "    changelog: [Unreleased] would become [$next] - $(date +%F)"
  "${TOOL[@]}" sync-versions --version "$next" --dry-run
  echo "    tag: v$next (not created)"
  echo "    push: $([[ "$PUSH" -eq 1 ]] && echo "origin main + v$next" || echo "disabled")"
  exit 0
fi

# ── Changelog + versions ────────────────────────────────────────────────────
echo "==> releasing $current -> $next"
"${TOOL[@]}" prepare-changelog --version "$next"
"${TOOL[@]}" sync-versions --version "$next"

# ── Verification ────────────────────────────────────────────────────────────
echo "==> verifying release metadata"
"${TOOL[@]}" check

echo "==> verifying the lockfile matches the workspace"
cargo metadata --locked --no-deps --format-version 1 >/dev/null

# ── Commit + tag ────────────────────────────────────────────────────────────
release_files=(
  Cargo.toml
  Cargo.lock
  CHANGELOG.md
  deploy/helm/parqtel/Chart.yaml
  docs/api/openapi.yaml
  README.md
  docs/getting-started/getting-started.md
  docs/guides/pql-guide.md
)
git add -- "${release_files[@]}"

if git diff --cached --quiet; then
  echo "error: nothing staged for the release commit" >&2
  exit 1
fi

git commit --quiet -m "chore(release): v$next" -m "Release $next.

The body of this release, and its GitHub release notes, come from the
CHANGELOG.md [$next] section. Verification: `python3
scripts/release/release_tool.py check` and `scripts/release/verify-release.sh`."

git tag -a "v$next" -m "v$next"

echo "==> created commit and tag v$next"
git --no-pager show --stat --oneline --decorate HEAD

if [[ "$PUSH" -eq 0 ]]; then
  echo
  echo "Not pushed (--no-push). To publish:"
  echo "  git push origin main && git push origin v$next"
  exit 0
fi

echo "==> pushing main and v$next"
git push --quiet origin main
git push --quiet origin "v$next"

echo
echo "Release v$next pushed. The Release workflow now:"
echo "  - re-runs clippy and the full test suite on the tagged commit,"
echo "  - builds the four platform binaries, the multi-arch image and the Helm chart,"
echo "  - signs the image, attests provenance and creates the GitHub release."
echo
echo "Watch it:  gh run watch \$(gh run list --workflow=release.yml --limit=1 --json databaseId --jq '.[0].databaseId')"
echo "Verify:    scripts/release/verify-release.sh v$next"
