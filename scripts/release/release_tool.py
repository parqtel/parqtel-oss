#!/usr/bin/env python3
"""Release metadata tool for parqtel-oss.

One implementation of the release metadata contract, shared by the local
release driver (``scripts/release/release.sh``), CI
(``scripts/checks/check_release.py``) and the changelog workflow:

* every place that carries the product version agrees with
  ``[workspace.package].version`` in ``Cargo.toml``;
* ``CHANGELOG.md`` is a valid Keep-a-Changelog file whose top released version
  is the product version and whose compare links resolve;
* the generated commit block between the changelog markers is refreshed from
  conventional commits with git-cliff;
* a release folds ``[Unreleased]`` into a dated ``## [X.Y.Z]`` section and
  opens a fresh Unreleased section;
* release notes for a tag are extracted from the changelog section.

Only the Python standard library is used. The git-cliff binary is looked up in
``.tools/bin`` first (``make tools``), then on ``PATH`` (CI installs it with
``taiki-e/install-action``).
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[2]
CHANGELOG = ROOT / "CHANGELOG.md"
CARGO_TOML = ROOT / "Cargo.toml"
CARGO_LOCK = ROOT / "Cargo.lock"
CHART = ROOT / "deploy" / "helm" / "parqtel" / "Chart.yaml"
OPENAPI = ROOT / "docs" / "api" / "openapi.yaml"
README = ROOT / "README.md"
GETTING_STARTED = ROOT / "docs" / "getting-started" / "getting-started.md"
PQL_GUIDE = ROOT / "docs" / "guides" / "pql-guide.md"

MARK_START = "<!-- changelog:auto-start -->"
MARK_END = "<!-- changelog:auto-end -->"
EMPTY_BLOCK = "_No unreleased changes._"
REPO_SLUG = "parqtel/parqtel-oss"

WORKSPACE_CRATES = (
    "parqtel-core",
    "parqtel-ingest",
    "parqtel-query",
    "parqtel-alert",
    "parqtel-pipeline",
    "parqtel-server",
    "parqtel-mcp-core",
    "parqtel-mcp-parqtel",
)

SEMVER_RE = re.compile(r"^\d+\.\d+\.\d+$")
HEADING_RE = re.compile(
    r"^## \[(?P<version>[^\]]+)\](?: - (?P<date>\d{4}-\d{2}-\d{2}))?[ \t]*$",
    re.MULTILINE,
)
LINK_RE = re.compile(r"^\[(?P<version>[^\]]+)\]: (?P<url>\S+)[ \t]*$", re.MULTILINE)


class ReleaseError(RuntimeError):
    """A release-metadata invariant is broken."""


# ── small helpers ────────────────────────────────────────────────────────────


def read(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def write(path: pathlib.Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def parse_semver(version: str) -> tuple[int, int, int]:
    if not SEMVER_RE.match(version):
        raise ReleaseError(f"not a semantic version: {version!r}")
    major, minor, patch = (int(part) for part in version.split("."))
    return major, minor, patch


def format_semver(parts: tuple[int, int, int]) -> str:
    return ".".join(str(part) for part in parts)


def git(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if check and result.returncode != 0:
        raise ReleaseError(f"git {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def latest_tag() -> str:
    tag = git("describe", "--tags", "--abbrev=0", "--match", "v[0-9]*", check=False)
    return tag or ""


def git_cliff() -> list[str]:
    for candidate in (ROOT / ".tools" / "bin" / "git-cliff",):
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return [str(candidate)]
    found = shutil_which("git-cliff")
    if found:
        return [found]
    raise ReleaseError(
        "git-cliff not found; install it (cargo install git-cliff) or run "
        "`make cliff` to fetch the pinned binary into .tools/bin"
    )


def shutil_which(name: str) -> str | None:
    for directory in os.environ.get("PATH", "").split(os.pathsep):
        candidate = pathlib.Path(directory) / name
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return str(candidate)
    return None


# ── version sites ────────────────────────────────────────────────────────────


def cargo_version() -> str:
    text = read(CARGO_TOML)
    match = re.search(r"^\[workspace\.package\][^\[]*?^version = \"(?P<v>[^\"]+)\"", text, re.MULTILINE | re.DOTALL)
    if not match:
        raise ReleaseError("Cargo.toml: [workspace.package].version not found")
    return match.group("v")


def _sites() -> list[tuple[pathlib.Path, re.Pattern[str], str]]:
    """(file, pattern with one ``{v}`` capture, human description)."""

    return [
        (
            CHART,
            re.compile(r"^version: (?P<v>\d+\.\d+\.\d+)$", re.MULTILINE),
            "Helm chart version",
        ),
        (
            CHART,
            re.compile(r'^appVersion: "(?P<v>\d+\.\d+\.\d+)"$', re.MULTILINE),
            "Helm chart appVersion",
        ),
        (
            OPENAPI,
            re.compile(r'^  version: "(?P<v>\d+\.\d+\.\d+)"$', re.MULTILINE),
            "OpenAPI info.version",
        ),
        (
            README,
            re.compile(r"`(?P<v>\d+\.\d+\.\d+)` is a public beta"),
            "README status line",
        ),
        (
            GETTING_STARTED,
            re.compile(r"ghcr\.io/parqtel/parqtel-oss:(?P<v>\d+\.\d+\.\d+)"),
            "getting-started image tag",
        ),
        (
            PQL_GUIDE,
            re.compile(
                r"\*\*Version:\*\* (?P<v>\d+\.\d+\.\d+) · \*\*Applies to:\*\* Parqtel (?P<v2>\d+\.\d+\.\d+)"
            ),
            "PQL guide version line",
        ),
    ]


def _sync_site(path: pathlib.Path, pattern: re.Pattern[str], version: str) -> str:
    text = read(path)

    def _sub(match: re.Match[str]) -> str:
        # Replace every numeric version group (v, or v and v2 when one line
        # carries the version twice) without touching the rest of the match.
        spans = sorted(
            (match.span(name) for name in pattern.groupindex if name.startswith("v")),
            reverse=True,
        )
        rendered = match.group(0)
        for start, end in spans:
            relative_start = start - match.start()
            relative_end = end - match.start()
            rendered = rendered[:relative_start] + version + rendered[relative_end:]
        return rendered

    new_text, count = pattern.subn(_sub, text)
    if count != 1:
        raise ReleaseError(f"{path.relative_to(ROOT)}: expected exactly one version match, found {count}")
    return new_text


def sync_versions(version: str, dry_run: bool = False) -> None:
    parse_semver(version)

    text = read(CARGO_TOML)
    match = re.search(
        r"(?ms)^(\[workspace\.package\]\n(?:.*?\n)??version = \")(?P<v>[^\"]+)(\")",
        text,
    )
    if not match or match.group("v") == version:
        if not match:
            raise ReleaseError("Cargo.toml: [workspace.package].version not found")
        print(f"  Cargo.toml: already {version}")
    else:
        text = text[: match.start("v")] + version + text[match.end("v") :]
        print(f"  Cargo.toml: {match.group('v')} -> {version}")
        if not dry_run:
            write(CARGO_TOML, text)

    text = read(CARGO_LOCK)
    changed = 0
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("name = \"") and line.split('"')[1] in WORKSPACE_CRATES:
            if index + 1 < len(lines) and lines[index + 1].startswith("version = "):
                if lines[index + 1] != f'version = "{version}"':
                    print(f"  Cargo.lock: {line.split('\"')[1]} -> {version}")
                    lines[index + 1] = f'version = "{version}"'
                    changed += 1
    if changed and not dry_run:
        write(CARGO_LOCK, "\n".join(lines) + "\n")

    for path, pattern, description in _sites():
        text = read(path)
        new_text = _sync_site(path, pattern, version)
        if new_text == text:
            print(f"  {description}: already {version}")
            continue
        print(f"  {description}: updated to {version}")
        if not dry_run:
            write(path, new_text)


# ── changelog parsing ────────────────────────────────────────────────────────


def changelog_sections(text: str) -> list[dict[str, object]]:
    matches = list(HEADING_RE.finditer(text))
    sections: list[dict[str, object]] = []
    for position, match in enumerate(matches):
        end = matches[position + 1].start() if position + 1 < len(matches) else len(text)
        sections.append(
            {
                "version": match.group("version"),
                "date": match.group("date"),
                "start": match.start(),
                "end": end,
                "body_start": match.end(),
                "body_end": end,
            }
        )
    return sections


def strip_auto_block(body: str) -> str:
    start = body.find(MARK_START)
    end = body.find(MARK_END)
    if start != -1 and end != -1:
        body = body[:start] + body[end + len(MARK_END) :]
    return body


def section_body(text: str, section: dict[str, object]) -> str:
    return text[int(section["body_start"]) : int(section["body_end"])]


def release_versions(text: str) -> list[dict[str, object]]:
    return [section for section in changelog_sections(text) if section["version"] != "Unreleased"]


def check_changelog(text: str, tag: str | None, problems: list[str]) -> None:
    sections = changelog_sections(text)
    if not sections or sections[0]["version"] != "Unreleased":
        problems.append("CHANGELOG.md: the first section must be '## [Unreleased]'")

    released = release_versions(text)
    if not released:
        problems.append("CHANGELOG.md: no released version section found")
        return

    version = cargo_version()
    if released[0]["version"] != version:
        problems.append(
            f"CHANGELOG.md: newest released section is {released[0]['version']}, "
            f"but Cargo.toml says {version}"
        )

    seen: set[str] = set()
    previous: tuple[int, int, int] | None = None
    for section in released:
        name = str(section["version"])
        try:
            parts = parse_semver(name)
        except ReleaseError as error:
            problems.append(f"CHANGELOG.md: {error}")
            continue
        if name in seen:
            problems.append(f"CHANGELOG.md: duplicate release section {name}")
        seen.add(name)
        if previous is not None and parts >= previous:
            problems.append(
                f"CHANGELOG.md: release sections are not in descending order at {name}"
            )
        previous = parts
        if not section["date"]:
            problems.append(f"CHANGELOG.md: release {name} has no ' - YYYY-MM-DD' date")
        body = strip_auto_block(section_body(text, section))
        if not re.search(r"^- \S", body, re.MULTILINE):
            problems.append(f"CHANGELOG.md: release {name} has no entries")

    if text.count(MARK_START) != 1 or text.count(MARK_END) != 1:
        problems.append("CHANGELOG.md: the changelog auto markers must appear exactly once")
    elif text.index(MARK_START) > text.index(MARK_END):
        problems.append("CHANGELOG.md: changelog auto markers are out of order")

    refs = {match.group("version"): match.group("url") for match in LINK_RE.finditer(text)}
    newest = str(released[0]["version"])
    unreleased_ref = refs.get("Unreleased")
    if not unreleased_ref:
        problems.append("CHANGELOG.md: missing [Unreleased] compare link")
    elif not unreleased_ref.endswith(f"compare/v{newest}...HEAD"):
        problems.append(
            f"CHANGELOG.md: [Unreleased] link must be compare/v{newest}...HEAD, got {unreleased_ref}"
        )
    for section in released:
        name = str(section["version"])
        if name not in refs:
            problems.append(f"CHANGELOG.md: missing compare link for {name}")
        elif f"v{name}" not in refs[name]:
            problems.append(f"CHANGELOG.md: compare link for {name} does not contain v{name}")

    if tag:
        tag_version = tag[1:] if tag.startswith("v") else tag
        if tag_version != version:
            problems.append(
                f"tag {tag} does not match Cargo.toml version {version}"
            )
        if tag_version != newest:
            problems.append(
                f"tag {tag} does not match newest CHANGELOG release {newest}"
            )


def check_versions(problems: list[str]) -> None:
    version = cargo_version()
    for path, pattern, description in _sites():
        text = read(path)
        matches = list(pattern.finditer(text))
        if len(matches) != 1:
            problems.append(
                f"{path.relative_to(ROOT)}: expected one {description}, found {len(matches)}"
            )
            continue
        for group in (name for name in pattern.groupindex if name.startswith("v")):
            if matches[0].group(group) != version:
                problems.append(
                    f"{path.relative_to(ROOT)}: {description} is {matches[0].group(group)}, "
                    f"but Cargo.toml says {version}"
                )

    lock_text = read(CARGO_LOCK)
    lines = lock_text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("name = \"") and line.split('"')[1] in WORKSPACE_CRATES:
            if index + 1 >= len(lines) or not lines[index + 1].startswith("version = "):
                problems.append(f"Cargo.lock: {line} has no version line")
                continue
            crate_version = lines[index + 1].split('"')[1]
            if crate_version != version:
                problems.append(
                    f"Cargo.lock: {line.split(chr(34))[1]} is {crate_version}, "
                    f"but Cargo.toml says {version}"
                )


# ── generated block ──────────────────────────────────────────────────────────


GROUP_ORDER = (
    "Features",
    "Bug Fixes",
    "Performance",
    "Refactoring",
    "Reverts",
    "Security",
    "Build",
)


def normalize_block(block: str) -> str:
    """Fix group order and collapse whitespace in a git-cliff rendering.

    The template renders groups in first-seen commit order and leaves blank
    lines between template loops; both are noise in a file a human reads.
    Repeated bullets are dropped as well: history contains cherry-picked
    commits that git-cliff reports twice.
    """

    groups: dict[str, list[str]] = {}
    current: str | None = None
    for line in block.splitlines():
        stripped = line.strip()
        if stripped.startswith("#### "):
            current = stripped[len("#### ") :]
            groups.setdefault(current, [])
        elif stripped.startswith("- ") and current is not None:
            if stripped not in groups[current]:
                groups[current].append(stripped)

    if not groups:
        return EMPTY_BLOCK

    ordered = [group for group in GROUP_ORDER if group in groups]
    ordered += [group for group in groups if group not in GROUP_ORDER]
    lines: list[str] = []
    for group in ordered:
        lines.append(f"#### {group}")
        lines.append("")
        lines.extend(groups[group])
        lines.append("")
    return "\n".join(lines).strip()


def render_block() -> str:
    command = [
        *git_cliff(),
        "--config",
        str(ROOT / "cliff.toml"),
        "--unreleased",
        "--strip",
        "header",
    ]
    result = subprocess.run(
        command,
        cwd=ROOT,
        check=False,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if result.returncode != 0:
        raise ReleaseError(f"git-cliff failed: {result.stderr.strip()}")
    block = normalize_block(result.stdout).strip()
    if not block:
        return EMPTY_BLOCK
    return block


def replace_block(text: str, block: str) -> str:
    start = text.find(MARK_START)
    end = text.find(MARK_END)
    if start == -1 or end == -1 or end < start:
        raise ReleaseError("CHANGELOG.md: changelog auto markers are missing")
    return text[: start + len(MARK_START)] + "\n" + block + "\n" + text[end:]


def changelog_update(dry_run: bool = False) -> None:
    text = read(CHANGELOG)
    # `git-cliff --unreleased` reports every commit since the newest tag. If
    # that tag is missing from the clone — a race with the tag push, or a
    # shallow checkout — the whole history shows up as unreleased and the
    # block looks stale for no reason. Say so instead of writing that block.
    released = release_versions(text)
    if released:
        newest = f"v{released[0]['version']}"
        tag_exists = subprocess.run(
            ["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{newest}"],
            cwd=ROOT,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode == 0
        if not tag_exists:
            raise ReleaseError(
                f"tag {newest} is missing locally; run `git fetch --tags` "
                "(without it every commit looks unreleased)"
            )
    block = render_block()
    new_text = replace_block(text, block)
    if new_text == text:
        print("CHANGELOG.md: generated block is already current")
        return
    if dry_run:
        print(block)
        return
    write(CHANGELOG, new_text)
    print("CHANGELOG.md: refreshed the generated block")


def write_refs(text: str, versions: list[str]) -> str:
    """Rewrite the trailing compare-link block for the given release order."""

    # Order: Unreleased first, then newest release downwards. `versions` holds
    # releases newest-first.
    refs: list[str] = [f"[Unreleased]: https://github.com/{REPO_SLUG}/compare/v{versions[0]}...HEAD"]
    for position, name in enumerate(versions):
        if position + 1 < len(versions):
            previous = versions[position + 1]
            refs.append(f"[{name}]: https://github.com/{REPO_SLUG}/compare/v{previous}...v{name}")
        else:
            refs.append(f"[{name}]: https://github.com/{REPO_SLUG}/releases/tag/v{name}")

    matches = list(LINK_RE.finditer(text))
    if matches:
        block_start = matches[0].start()
        block_end = matches[-1].end()
        text = text[:block_start] + "\n".join(refs) + text[block_end:]
    else:
        text = text.rstrip("\n") + "\n\n" + "\n".join(refs) + "\n"
    return text


def changelog_prepare(version: str, date: str, dry_run: bool = False) -> None:
    parse_semver(version)
    text = read(CHANGELOG)

    sections = changelog_sections(text)
    if not sections or sections[0]["version"] != "Unreleased":
        raise ReleaseError("CHANGELOG.md: the first section must be '## [Unreleased]'")
    if any(section["version"] == version for section in sections):
        raise ReleaseError(f"CHANGELOG.md: {version} already has a section")

    block = render_block()
    text = replace_block(text, block)

    unreleased = changelog_sections(text)[0]
    body = section_body(text, unreleased)
    kept = strip_auto_block(body).strip("\n")
    new_unreleased = (
        "## [Unreleased]\n\n"
        f"{MARK_START}\n{EMPTY_BLOCK}\n{MARK_END}\n\n"
        f"## [{version}] - {date}\n"
    )
    # Markers delimit only the Unreleased block, so they stay unique: a
    # released section receives the generated list verbatim, frozen with the
    # commits it describes, behind its hand-written highlights.
    released_body = "\n"
    if kept:
        released_body += kept + "\n\n"
    released_body += block + "\n\n"
    new_text = (
        text[: int(unreleased["start"])]
        + new_unreleased
        + released_body
        + text[int(unreleased["end"]) :]
    )

    releases = [version] + [str(section["version"]) for section in release_versions(new_text)]
    new_text = write_refs(new_text, releases)

    if dry_run:
        print(f"CHANGELOG.md: would release {version} on {date}")
        return
    write(CHANGELOG, new_text)
    print(f"CHANGELOG.md: released {version} on {date}")


IMAGE = "ghcr.io/parqtel/parqtel-oss"


def release_notes(tag: str) -> str:
    """Render the GitHub release body for ``tag``.

    Prose and generated commit list come from the changelog section, so the
    release page and the file can never disagree. The install and verify
    footer is fixed: it documents the artifacts release.yml publishes.
    """

    version = tag[1:] if tag.startswith("v") else tag
    text = read(CHANGELOG)
    body = None
    for section in changelog_sections(text):
        if section["version"] != version:
            continue
        body = section_body(text, section)
        body = re.sub(r"\n{3,}", "\n\n", body).strip()
        break
    if body is None:
        raise ReleaseError(f"CHANGELOG.md: no section for {version}")

    footer = f"""
## Install

**Docker**

```bash
docker pull {IMAGE}:{version}
```

**Helm**

```bash
helm install parqtel oci://ghcr.io/parqtel/charts/parqtel --version {version}
```

**Binary**

Download the archive for your platform from the assets below, or use Homebrew:

```bash
brew tap parqtel/tap
brew install parqtel
```

## Verify

```bash
# Image build provenance (keyless, signed by this workflow)
gh attestation verify oci://{IMAGE}:{version} --owner parqtel

# Binary checksum
sha256sum -c parqtel-linux-amd64.tar.gz.sha256
```

**Full changelog:** https://github.com/{REPO_SLUG}/blob/v{version}/CHANGELOG.md
"""
    return body + "\n" + footer.lstrip("\n")


# ── next version ─────────────────────────────────────────────────────────────


def next_version(bump: str) -> str:
    current = cargo_version()
    major, minor, patch = parse_semver(current)

    if SEMVER_RE.match(bump):
        target = parse_semver(bump)
        if target <= (major, minor, patch):
            raise ReleaseError(f"{bump} is not greater than the current version {current}")
        return bump

    if bump == "auto":
        result = subprocess.run(
            [*git_cliff(), "--config", str(ROOT / "cliff.toml"), "--bumped-version"],
            cwd=ROOT,
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            raise ReleaseError(f"git-cliff --bumped-version failed: {result.stderr.strip()}")
        candidate = result.stdout.strip().lstrip("v")
        if not SEMVER_RE.match(candidate):
            raise ReleaseError(f"git-cliff returned an invalid version: {candidate!r}")
        return candidate

    if bump == "major":
        return format_semver((major + 1, 0, 0))
    if bump == "minor":
        return format_semver((major, minor + 1, 0))
    if bump == "patch":
        return format_semver((major, minor, patch + 1))
    raise ReleaseError(f"unknown bump: {bump!r} (expected auto, major, minor, patch or X.Y.Z)")


# ── CLI ──────────────────────────────────────────────────────────────────────


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("version", help="print the current product version")

    next_parser = subparsers.add_parser("next", help="print the next semantic version")
    next_parser.add_argument(
        "--bump", default="auto", help="auto, major, minor, patch or an explicit X.Y.Z"
    )

    subparsers.add_parser("update-changelog", help="refresh the generated changelog block")

    prepare_parser = subparsers.add_parser(
        "prepare-changelog", help="fold [Unreleased] into a dated release section"
    )
    prepare_parser.add_argument("--version", required=True)
    prepare_parser.add_argument(
        "--date", default=_dt.date.today().isoformat(), help="release date (default: today)"
    )
    prepare_parser.add_argument("--dry-run", action="store_true")

    sync_parser = subparsers.add_parser("sync-versions", help="write the version everywhere")
    sync_parser.add_argument("--version", required=True)
    sync_parser.add_argument("--dry-run", action="store_true")

    check_parser = subparsers.add_parser("check", help="verify the release metadata contract")
    check_parser.add_argument("--tag", help="also require this tag to match the version")

    notes_parser = subparsers.add_parser(
        "notes", help="print the GitHub release body for a tag"
    )
    notes_parser.add_argument("--tag", required=True)

    args = parser.parse_args(argv)

    try:
        if args.command == "version":
            print(cargo_version())
        elif args.command == "next":
            print(next_version(args.bump))
        elif args.command == "update-changelog":
            changelog_update()
        elif args.command == "prepare-changelog":
            changelog_prepare(args.version, args.date, args.dry_run)
        elif args.command == "sync-versions":
            sync_versions(args.version, args.dry_run)
        elif args.command == "notes":
            print(release_notes(args.tag))
        elif args.command == "check":
            problems: list[str] = []
            check_versions(problems)
            check_changelog(read(CHANGELOG), args.tag, problems)
            if problems:
                for problem in problems:
                    print(f"error: {problem}", file=sys.stderr)
                return 1
            print("release metadata OK")
    except ReleaseError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
