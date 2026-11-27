#!/usr/bin/env bash
# Verify a published parqtel-oss release end to end.
#
# Checks the GitHub release (state, assets, checksums), the multi-arch image
# tags and their digest identity, the OCI Helm chart, the image attestation
# and — on linux/amd64 — the binary's reported version.
#
# Usage:
#   scripts/release/verify-release.sh              # newest GitHub release
#   scripts/release/verify-release.sh v0.4.0
#   scripts/release/verify-release.sh v0.4.0 --skip-download   # no tarballs
#   scripts/release/verify-release.sh v0.4.0 --skip-image      # GitHub only
#
# Needs: gh (authenticated), docker (for the image checks), helm (chart check).
# Missing optional tools skip their step and are reported as SKIP, which does
# not fail the run; any failed assertion does.

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
REPO="parqtel/parqtel-oss"
IMAGE="ghcr.io/parqtel/parqtel-oss"
CHART="oci://ghcr.io/parqtel/charts/parqtel"

TAG=""
DO_DOWNLOAD=1
DO_IMAGE=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-download) DO_DOWNLOAD=0; shift ;;
    --skip-image) DO_IMAGE=0; shift ;;
    -h | --help)
      sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    -*)
      echo "error: unknown option $1" >&2
      exit 2
      ;;
    *)
      TAG="$1"
      shift
      ;;
  esac
done

command -v gh >/dev/null || { echo "error: gh is required" >&2; exit 2; }
gh auth status >/dev/null 2>&1 || { echo "error: gh is not authenticated" >&2; exit 2; }

failures=0
pass() { printf '  \033[32mPASS\033[0m  %s\n' "$*"; }
skip() { printf '  \033[33mSKIP\033[0m  %s\n' "$*"; }
fail() { printf '  \033[31mFAIL\033[0m  %s\n' "$*"; failures=$((failures + 1)); }

if [[ -z "$TAG" ]]; then
  TAG="$(gh release view --repo "$REPO" --json tagName --jq .tagName)" || {
    echo "error: cannot read the newest GitHub release" >&2
    exit 1
  }
  echo "==> newest release: $TAG"
else
  echo "==> release: $TAG"
fi

if [[ ! "$TAG" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "error: '$TAG' is not a vX.Y.Z tag" >&2
  exit 2
fi
VERSION="${TAG#v}"
MAJOR="${VERSION%%.*}"
REST="${VERSION#*.}"
MINOR="${REST%%.*}"

# ── GitHub release ──────────────────────────────────────────────────────────
echo
echo "GitHub release"
release_json="$(gh release view "$TAG" --repo "$REPO" \
  --json tagName,isDraft,isPrerelease,publishedAt,url,assets,body)" || {
  echo "  \033[31mFAIL\033[0m  release $TAG does not exist"
  exit 1
}
echo "$release_json" | jq -e '.isDraft == false' >/dev/null &&
  pass "not a draft" || fail "release is still a draft"
echo "$release_json" | jq -e '.isPrerelease == false' >/dev/null &&
  pass "not a prerelease" || fail "release is marked prerelease"
echo "$release_json" | jq -e '.publishedAt != null' >/dev/null &&
  pass "published" || fail "release has no publish time"
pass "url: $(echo "$release_json" | jq -r .url)"

expected_assets=(
  parqtel-linux-amd64.tar.gz
  parqtel-linux-amd64.tar.gz.sha256
  parqtel-linux-arm64.tar.gz
  parqtel-linux-arm64.tar.gz.sha256
  parqtel-darwin-amd64.tar.gz
  parqtel-darwin-amd64.tar.gz.sha256
  parqtel-darwin-arm64.tar.gz
  parqtel-darwin-arm64.tar.gz.sha256
)
actual_assets="$(echo "$release_json" | jq -r '.assets[].name' | sort)"
missing=0
for asset in "${expected_assets[@]}"; do
  if grep -qxF "$asset" <<<"$actual_assets"; then
    pass "asset $asset"
  else
    fail "missing asset $asset"
    missing=1
  fi
done

# ── Release notes ──────────────────────────────────────────────────────────
# The release body is generated from the tag's CHANGELOG.md section, so a body
# that differs from what the changelog yields means the two drifted apart.
echo
echo "Release notes"
local_notes="$(mktemp)"
if (cd "$ROOT" && python3 scripts/release/release_tool.py notes --tag "$TAG") \
    >"$local_notes" 2>/dev/null; then
  normalize() { sed -e 's/[[:space:]]*$//' "$1" | sed -e :a -e '/^\n*$/{$d;N;};/\n$/ba'; }
  if diff -q <(normalize "$local_notes") <(echo "$release_json" | jq -r .body | normalize /dev/stdin) >/dev/null; then
    pass "body matches the changelog section"
  else
    fail "body differs from 'release_tool.py notes --tag $TAG' (changelog and release page disagree)"
  fi
else
  skip "no local changelog for $TAG (run from a checkout that has the tag)"
fi
rm -f "$local_notes"

# ── Checksums + binary version ──────────────────────────────────────────────
if [[ "$DO_DOWNLOAD" -eq 1 && "$missing" -eq 0 ]]; then
  echo
  echo "Artifacts"
  workdir="$(mktemp -d)"
  trap 'rm -rf "$workdir"' EXIT
  if gh release download "$TAG" --repo "$REPO" \
    --pattern '*.tar.gz' --pattern '*.tar.gz.sha256' --dir "$workdir" >/dev/null; then
    for sha in "$workdir"/*.sha256; do
      name="$(basename "$sha" .sha256)"
      if (cd "$workdir" && sha256sum -c "$(basename "$sha")" >/dev/null 2>&1); then
        pass "checksum $name"
      else
        fail "checksum mismatch for $name"
      fi
    done

    host_arch="$(uname -s)/$(uname -m)"
    if [[ "$host_arch" == "Linux/x86_64" ]]; then
      tar -xzf "$workdir/parqtel-linux-amd64.tar.gz" -C "$workdir" parqtel
      reported="$("$workdir/parqtel" --version 2>&1 | head -n 1)"
      if [[ "$reported" == *"$VERSION"* ]]; then
        pass "binary reports $VERSION ($reported)"
      else
        fail "binary reports '$reported', expected $VERSION"
      fi
    else
      skip "binary --version on this host ($host_arch)"
    fi
  else
    fail "could not download release assets"
  fi
else
  [[ "$DO_DOWNLOAD" -eq 0 ]] && { echo; echo "Artifacts"; skip "downloads disabled"; }
fi

# ── Container image ────────────────────────────────────────────────────────
if [[ "$DO_IMAGE" -eq 1 ]]; then
  echo
  echo "Container image"
  if ! command -v docker >/dev/null; then
    skip "docker not installed"
  else
    # GHCR packages of a new repository are private by default. Log in with
    # the gh token when anonymous inspection fails, so the check still runs.
    inspect() { docker buildx imagetools inspect "$1" 2>/dev/null; }
    if ! inspect "$IMAGE:$VERSION" >/dev/null; then
      token="$(gh auth token 2>/dev/null)"
      user="$(gh api user --jq .login 2>/dev/null)"
      if [[ -n "$token" && -n "$user" ]]; then
        echo "$token" | docker login ghcr.io -u "$user" --password-stdin >/dev/null 2>&1 || true
      fi
    fi

    reference_digest=""
    for tag in "$VERSION" "$MAJOR.$MINOR" "$MAJOR" "latest"; do
      output="$(inspect "$IMAGE:$tag")"
      if [[ -z "$output" ]]; then
        fail "image tag $tag not found (or not readable)"
        continue
      fi
      digest="$(sed -n 's/^Digest: *//p' <<<"$output" | head -n 1)"
      if [[ -z "$digest" ]]; then
        digest="$(grep -o 'sha256:[0-9a-f]\{64\}' <<<"$output" | head -n 1)"
      fi
      pass "image tag $tag -> ${digest:-unknown}"
      if [[ -z "$reference_digest" ]]; then
        reference_digest="$digest"
      elif [[ "$digest" != "$reference_digest" ]]; then
        fail "image tag $tag points at $digest, not $reference_digest"
      fi
    done

    # `gh attestation` landed in gh 2.49; older clients cannot verify, and
    # this must not look like a release defect. The release workflow runs the
    # same check with the runner's gh after the attestation is created.
    if ! gh attestation verify --help >/dev/null 2>&1; then
      skip "provenance signature needs gh >= 2.49 ($(gh --version | head -n 1))"
    elif gh attestation verify "oci://$IMAGE:$VERSION" --owner parqtel >/dev/null 2>&1; then
      pass "build provenance attestation verifies"
    else
      fail "build provenance attestation missing or invalid"
    fi
  fi

  echo
  echo "Helm chart"
  if ! command -v helm >/dev/null; then
    skip "helm not installed"
  else
    err_file="$(mktemp)"
    if chart_meta="$(helm show chart "$CHART" --version "$VERSION" 2>"$err_file")"; then
      chart_version="$(sed -n 's/^version: *//p' <<<"$chart_meta" | head -n 1)"
      if [[ "$chart_version" == "$VERSION" ]]; then
        pass "chart $VERSION published"
      else
        fail "chart version is '$chart_version', expected $VERSION"
      fi
      rm -f "$err_file"
    elif grep -qiE 'unauthorized|forbidden|denied|authentication|anonymous token' "$err_file"; then
      rm -f "$err_file"
      # A new GHCR package starts private, and the gh OAuth token in a normal
      # checkout has no read:packages scope. Nothing can be concluded.
      skip "chart package not readable (GHCR package needs public visibility or a read:packages token)"
    else
      fail "chart $VERSION not found in $CHART: $(head -n 1 "$err_file")"
      rm -f "$err_file"
    fi
  fi
else
  echo
  echo "Container image / Helm chart"
  skip "image checks disabled"
fi

echo
if [[ "$failures" -eq 0 ]]; then
  echo "release $TAG verified"
  exit 0
fi
echo "release $TAG has $failures problem(s)"
exit 1
