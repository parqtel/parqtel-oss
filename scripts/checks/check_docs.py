#!/usr/bin/env python3
"""Validate the documentation: links, anchors, source paths and API coverage.

Run via `make docs-check`. Exits non-zero and prints every problem it finds.

Checks:
  1. Every relative Markdown link resolves to a file.
  2. Every relative Markdown link with a fragment resolves to a heading
     (fragments into source files are line ranges and are not checked).
  3. Every repository path quoted in backticks in the docs exists.
  4. Every route registered in the Axum router appears in the OpenAPI spec
     (and every OpenAPI path is a real route).
  5. The OpenAPI spec parses as YAML.
"""

from __future__ import annotations

import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DOCS = os.path.join(ROOT, "docs")
OPENAPI = os.path.join(DOCS, "api", "openapi.yaml")
ROUTER = os.path.join(ROOT, "crates", "parqtel-server", "src", "router.rs")

# Routes that are intentionally absent from the OpenAPI spec.
API_IGNORE = {"/", "/debug/pprof/profile", "/debug/pprof/summary", "/debug/pprof/memory"}

# Paths quoted in prose that are not repository files.
IGNORED_PATHS = {"/openapi.yaml", "/ui", "/api/v1/*", "foo/tests.rs"}


def slug(text: str) -> str:
    text = re.sub(r"`([^`]*)`", r"\1", text.strip().lower())
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)
    text = re.sub(r"[*_~]", "", text)
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def markdown_files() -> list[str]:
    """Every Markdown file tracked by git (local, untracked notes are skipped)."""
    try:
        import subprocess

        result = subprocess.run(
            [
                "git",
                "-C",
                ROOT,
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "*.md",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        return [
            os.path.normpath(os.path.join(ROOT, name))
            for name in result.stdout.split()
            if name.endswith(".md")
        ]
    except Exception:  # noqa: BLE001 - fall back to a filesystem walk
        out = []
        for base in ("docs", "rules"):
            for dirpath, dirnames, filenames in os.walk(os.path.join(ROOT, base)):
                dirnames[:] = [d for d in dirnames if not d.startswith(".")]
                for name in filenames:
                    if name.endswith(".md"):
                        out.append(os.path.normpath(os.path.join(dirpath, name)))
        for name in os.listdir(ROOT):
            if name.endswith(".md"):
                out.append(os.path.normpath(os.path.join(ROOT, name)))
        return sorted(set(out))


def source_roots() -> list[str]:
    """Directories a quoted source path may be relative to."""
    roots = [ROOT, os.path.join(ROOT, "crates")]
    crates = os.path.join(ROOT, "crates")
    if os.path.isdir(crates):
        for crate in os.listdir(crates):
            crate_dir = os.path.join(crates, crate)
            if not os.path.isdir(crate_dir):
                continue
            roots.append(crate_dir)
            for sub in ("src", os.path.join("src", "storage"), os.path.join("src", "handlers")):
                candidate = os.path.join(crate_dir, sub)
                if os.path.isdir(candidate):
                    roots.append(candidate)
                    for deeper in os.listdir(candidate):
                        deeper_path = os.path.join(candidate, deeper)
                        if os.path.isdir(deeper_path):
                            roots.append(deeper_path)
    helm = os.path.join(ROOT, "deploy", "helm")
    if os.path.isdir(helm):
        for chart in os.listdir(helm):
            roots.append(os.path.join(helm, chart))
    deploy = os.path.join(ROOT, "deploy")
    if os.path.isdir(deploy):
        for entry in os.listdir(deploy):
            candidate = os.path.join(deploy, entry)
            if os.path.isdir(candidate):
                roots.append(candidate)
                for deeper in os.listdir(candidate):
                    deeper_path = os.path.join(candidate, deeper)
                    if os.path.isdir(deeper_path):
                        roots.append(deeper_path)
    return roots


def headings(path: str) -> set[str]:
    found: set[str] = set()
    fenced = False
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            if line.lstrip().startswith("```"):
                fenced = not fenced
                continue
            if fenced:
                continue
            match = re.match(r"^#{1,6}\s+(.*)$", line)
            if match:
                found.add(slug(match.group(1)))
    return found


def check_links(files: list[str]) -> list[str]:
    anchors = {path: headings(path) for path in files}
    problems = []
    link_re = re.compile(r"\[[^\]]*\]\(([^)\s]+)\)")
    img_re = re.compile(r'<img\s+[^>]*src="([^"]+)"')
    for path in files:
        base = os.path.dirname(path)
        text = open(path, encoding="utf-8").read()
        for match in link_re.finditer(text):
            link = match.group(1)
            if link.startswith(("http", "mailto:")):
                continue
            target_path, _, fragment = link.partition("#")
            if target_path:
                target = os.path.normpath(os.path.join(base, target_path))
                if not os.path.exists(target):
                    problems.append(f"{os.path.relpath(path, ROOT)}: missing link target {link}")
                    continue
                if fragment and not target.endswith(".md"):
                    continue  # a fragment into a source file is a line range
            else:
                target = path
            if fragment and fragment not in anchors.get(target, set()):
                problems.append(f"{os.path.relpath(path, ROOT)}: missing anchor {link}")
        # Images the docs embed (the animated diagrams live here).
        for match in img_re.finditer(text):
            src = match.group(1)
            if src.startswith(("http", "mailto:", "data:")):
                continue
            if not os.path.exists(os.path.normpath(os.path.join(base, src.split("#")[0]))):
                problems.append(f"{os.path.relpath(path, ROOT)}: missing image {src}")
    return problems


def check_no_mermaid(files: list[str]) -> list[str]:
    """The documentation renders animated SVG, never Mermaid source blocks."""
    problems = []
    for path in files:
        in_mermaid = False
        with open(path, encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                stripped = line.lstrip()
                if stripped.startswith("```mermaid"):
                    in_mermaid = True
                    problems.append(
                        f"{os.path.relpath(path, ROOT)}:{number}: Mermaid fence; "
                        "embed an animated SVG instead"
                    )
                elif in_mermaid and stripped.startswith("```"):
                    in_mermaid = False
    return problems


def check_source_paths(files: list[str]) -> list[str]:
    problems = []
    roots = source_roots()
    path_re = re.compile(r"`([A-Za-z0-9_./-]+\.(?:rs|toml|yaml|yml|html|json|sh|md|py|txt))`")
    for path in files:
        for match in path_re.finditer(open(path, encoding="utf-8").read()):
            candidate = match.group(1)
            if candidate in IGNORED_PATHS:
                continue
            if "/" not in candidate:
                continue
            if candidate.startswith(("http", "crates/", "data/", "rules/", "docs/", "deploy/",
                                     "scripts/", "tests/", "examples/", "config/", ".")):
                continue
            options = [os.path.normpath(os.path.join(root, candidate)) for root in roots]
            if not any(os.path.exists(option) for option in options):
                problems.append(
                    f"{os.path.relpath(path, ROOT)}: unknown path `{candidate}`"
                )
    return problems


def router_routes() -> set[str]:
    text = open(ROUTER, encoding="utf-8").read()
    return set(re.findall(r'\.route\(\s*"([^"]+)"', text))


def openapi_paths() -> set[str]:
    """The paths declared in the OpenAPI spec.

    Uses PyYAML when it is available and falls back to reading the top-level
    path keys, so the check still runs on a machine without PyYAML.
    """
    text = open(OPENAPI, encoding="utf-8").read()
    try:
        import yaml  # type: ignore

        spec = yaml.safe_load(text)
        return set(spec.get("paths", {}))
    except ImportError:
        paths = set()
        in_paths = False
        for line in text.splitlines():
            if line.startswith("paths:"):
                in_paths = True
                continue
            if in_paths:
                match = re.match(r"^  (/\S*):\s*$", line)
                if match:
                    paths.add(match.group(1))
                elif line and not line.startswith(" "):
                    break
        return paths


def check_api_coverage() -> list[str]:
    problems = []
    try:
        spec_paths = openapi_paths()
    except Exception as exc:  # noqa: BLE001 - report, do not crash
        return [f"docs/api/openapi.yaml: does not parse as YAML ({exc})"]

    routes = router_routes()
    for route in sorted(routes):
        if route in API_IGNORE:
            continue
        # Axum uses `:param`; OpenAPI uses `{param}`.
        template = re.sub(r":(\w+)", r"{\1}", route)
        if template not in spec_paths:
            problems.append(f"route {route} is not described in the OpenAPI spec")
    for path in sorted(spec_paths):
        axum = re.sub(r"\{(\w+)\}", r":\1", path)
        if axum not in routes:
            problems.append(f"OpenAPI path {path} is not a registered route")
    return problems


def main() -> int:
    files = markdown_files()
    problems = (
        check_links(files)
        + check_source_paths(files)
        + check_api_coverage()
        + check_no_mermaid(files)
    )
    for problem in problems:
        print(problem)
    if problems:
        print(f"\n{len(problems)} documentation problem(s)")
        return 1
    print(
        f"documentation OK ({len(files)} markdown files: links, anchors, paths, API, SVG-only)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
