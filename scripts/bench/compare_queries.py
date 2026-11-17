#!/usr/bin/env python3
"""Compare query latency between two HTTP observability engines.

This is deliberately a *harness*, not a benchmark result: run it on one box,
against one corpus, with the same client, and publish the JSON output and host
shape. Parqtel queries use PromQL; Parseable queries use SQL (or its PromQL
surface). The query file supplies one entry per engine so the comparison is
honest about dialect.

Usage:
  ./scripts/bench/compare_queries.py queries.json \
      --parqtel http://127.0.0.1:8080 \
      --parseable http://127.0.0.1:8000 \
      --runs 10 --out results.json
"""

import argparse
import json
import statistics
import sys
import time
import urllib.parse
import urllib.request


def timed_get(url: str) -> float:
    started = time.perf_counter()
    with urllib.request.urlopen(url, timeout=120) as response:
        response.read()
    return (time.perf_counter() - started) * 1000.0


def query_url(base: str, path: str, params: dict) -> str:
    return f"{base.rstrip('/')}{path}?{urllib.parse.urlencode(params)}"


def run_engine(name: str, base: str, entries: list, runs: int) -> list:
    results = []
    for entry in entries:
        if name == "parqtel":
            url = query_url(base, "/api/v1/query", {"query": entry["query"]})
        else:
            url = query_url(base, "/api/v1/query", {"query": entry["sql"]})
        samples = []
        for _ in range(runs):
            samples.append(timed_get(url))
        results.append(
            {
                "name": entry["name"],
                "runs": runs,
                "p50_ms": round(statistics.median(samples), 3),
                "min_ms": round(min(samples), 3),
                "max_ms": round(max(samples), 3),
            }
        )
    return results


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("queries", help="JSON with {parqtel: [...], parseable: [...]}")
    parser.add_argument("--parqtel", required=True)
    parser.add_argument("--parseable", required=True)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--out", default="comparison-results.json")
    args = parser.parse_args()

    with open(args.queries, encoding="utf-8") as handle:
        queries = json.load(handle)
    output = {
        "host_shape": {
            "cpu_count": __import__("os").cpu_count(),
            "runs": args.runs,
        },
        "parqtel": run_engine(
            "parqtel", args.parqtel, queries.get("parqtel", []), args.runs
        ),
        "parseable": run_engine(
            "parseable", args.parseable, queries.get("parseable", []), args.runs
        ),
    }
    with open(args.out, "w", encoding="utf-8") as handle:
        json.dump(output, handle, indent=2)
    print(json.dumps(output, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
