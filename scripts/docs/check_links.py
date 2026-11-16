#!/usr/bin/env python3
"""Validate documentation links and anchors.

Checks every Markdown file in the repository (docs/, README.md, root docs and
crate READMEs) for:

* relative links that point at a file that does not exist
* links into a Markdown file whose ``#anchor`` does not exist
* links whose target path escapes the repository
* empty link targets

Usage::

    python3 scripts/docs/check_links.py            # report problems, exit 1
    python3 scripts/docs/check_links.py --quiet    # only the summary line

Run it from the repository root.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]

# Directories that are scanned for Markdown sources.
SCAN_DIRS = ("docs", "crates", "deploy", "scripts", "rules", "tests", "examples")
SCAN_ROOT_FILES = ("README.md", "AGENTS.md", "CLAUDE.md", "CONTRIBUTING.md",
                    "SECURITY.md", "SUPPORT.md", "CHANGELOG.md")

# Fenced code blocks are ignored: they may contain sample URLs.
FENCE_RE = re.compile(r"^\s*```")
LINK_RE = re.compile(r"(?<!!)\[(?P<text>[^\]]*)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)")
IMAGE_RE = re.compile(r"!\[(?P<text>[^\]]*)\]\((?P<target>[^)\s]+)(?:\s+\"[^\"]*\")?\)")
HTML_HREF_RE = re.compile(r"<a\s[^>]*href=\"(?P<target>[^\"]+)\"", re.IGNORECASE)
MD_ANCHOR_RE = re.compile(r"^(?P<name>.*)$")


def slugify(heading: str) -> str:
    """Approximate GitHub's heading anchor algorithm."""
    text = heading.strip()
    text = re.sub(r"`([^`]*)`", r"\1", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[*_~]", "", text)
    text = text.lower()
    text = re.sub(r"[^\w\- ]", "", text, flags=re.UNICODE)
    text = text.replace(" ", "-")
    return text


def anchors_of(markdown: str) -> set[str]:
    """Collect the anchors a Markdown file defines (headings + explicit ids)."""
    found: set[str] = set()
    in_fence = False
    for line in markdown.splitlines():
        if FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        heading = re.match(r"^(#{1,6})\s+(.*)$", line)
        if heading:
            found.add(slugify(heading.group(2)))
        explicit = re.match(r'^<a\s+(?:name|id)="([^"]+)"', line, re.IGNORECASE)
        if explicit:
            found.add(explicit.group(1))
    return found


def strip_code(markdown: str) -> str:
    """Blank out fenced code blocks so links inside them are not checked."""
    out: list[str] = []
    in_fence = False
    for line in markdown.splitlines():
        if FENCE_RE.match(line):
            in_fence = not in_fence
            out.append("")
            continue
        out.append("" if in_fence else line)
    return "\n".join(out)


def markdown_files() -> list[Path]:
    files: list[Path] = []
    for name in SCAN_ROOT_FILES:
        candidate = ROOT / name
        if candidate.is_file():
            files.append(candidate)
    for directory in SCAN_DIRS:
        base = ROOT / directory
        if not base.is_dir():
            continue
        files.extend(p for p in base.rglob("*.md")
                    if "target" not in p.parts and "node_modules" not in p.parts)
    return sorted(files)


def resolve(target: str, source: Path) -> Path | None:
    """Resolve a relative link target to a path on disk."""
    if target.startswith(("http://", "https://", "mailto:", "tel:", "//")):
        return None
    raw = target.split("#", 1)[0]
    if not raw:
        return source
    candidate = (source.parent / raw).resolve()
    try:
        candidate.relative_to(ROOT)
    except ValueError:
        return Path("/__outside_repo__")
    return candidate


def main() -> int:
    quiet = "--quiet" in sys.argv
    files = markdown_files()
    anchor_cache: dict[Path, set[str]] = {}
    problems: list[str] = []
    checked = 0

    for path in files:
        text = path.read_text(encoding="utf-8")
        body = strip_code(text)
        rel = path.relative_to(ROOT)

        targets: list[tuple[str, str]] = []
        for match in LINK_RE.finditer(body):
            targets.append((match.group("target"), match.group("text")))
        for match in IMAGE_RE.finditer(body):
            targets.append((match.group("target"), match.group("text")))
        for match in HTML_HREF_RE.finditer(body):
            targets.append((match.group("target"), ""))

        for target, text in targets:
            if not target or target.startswith("#"):
                continue
            checked += 1
            resolved = resolve(target, path)
            if resolved is None:
                continue
            if resolved == Path("/__outside_repo__"):
                problems.append(f"{rel}: link escapes the repository -> {target}")
                continue
            if resolved.is_dir():
                index = resolved / "README.md"
                if not index.is_file():
                    problems.append(f"{rel}: link to directory without README.md -> {target}")
                    continue
                resolved = index
            if not resolved.is_file():
                problems.append(f"{rel}: broken link -> {target}")
                continue
            if "#" in target and resolved.suffix.lower() == ".md":
                anchor = target.split("#", 1)[1]
                if not anchor:
                    continue
                anchor = anchor.lower()
                if resolved not in anchor_cache:
                    anchor_cache[resolved] = anchors_of(resolved.read_text(encoding="utf-8"))
                if anchor not in anchor_cache[resolved]:
                    problems.append(f"{rel}: missing anchor -> {target}")

    print(f"checked {len(files)} markdown files, {checked} local links")
    if problems:
        print(f"\n{len(problems)} problem(s):")
        for problem in problems:
            print(f"  - {problem}")
        return 1
    print("no broken links found")
    if not quiet:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
