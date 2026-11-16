# Raw benchmark output

Captured stdout of the benchmarks referenced by the documents in this folder.
They are kept verbatim so every number in the write-ups can be traced back to
the run that produced it.

| File | Benchmark | Written up in |
|------|-----------|---------------|
| [`baseline_before.txt`](baseline_before.txt) | Pre-optimisation baseline run | [hot-path.md](../hot-path.md) |
| [`e2e_before.txt`](e2e_before.txt) | End-to-end suite, before the series table and log skip index | [series-index.md](../series-index.md), [log-index.md](../log-index.md) |
| [`perf_bench_before.txt`](perf_bench_before.txt) | `perf_bench` harness, before the changes | [hot-path.md](../hot-path.md) |
| [`results_after.txt`](results_after.txt) | Post-change measurements | [hot-path.md](../hot-path.md) |
| [`e2e_after.txt`](e2e_after.txt) | End-to-end suite, after the series table and log skip index | [series-index.md](../series-index.md), [log-index.md](../log-index.md) |
| [`perf_bench_after.txt`](perf_bench_after.txt) | `perf_bench` harness, after the changes | [hot-path.md](../hot-path.md) |

The harnesses are reproducible:

```bash
cargo run --release -p parqtel-server --example perf_bench
```

See [the performance index](../README.md) for what each measurement means.
