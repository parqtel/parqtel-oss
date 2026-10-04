# Contributing to Parqtel

Thank you for your interest in contributing to Parqtel! We welcome contributions of all kinds: bug reports, documentation improvements, new features, and feedback.

## 1. Our Values
- **Performance**: We strive for the smallest resource footprint and fastest ingestion.
- **Safety**: We use Rust to ensure memory safety and avoid runtime panics.
- **Simplicity**: We prefer simple, explicit solutions over complex abstractions.
- **Inclusivity**: Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## 2. Reporting Issues
- **Bugs**: Use the [Bug Report](.github/ISSUE_TEMPLATE/bug_report.yml) template. The version, deployment method and a handful of `/metrics` counters are what make a report actionable.
- **Features**: Use the [Feature Request](.github/ISSUE_TEMPLATE/feature_request.md) template to propose new ideas.
- **Security**: Please report security vulnerabilities privately according to our [Security Policy](SECURITY.md).

## 3. Pull Request Process
1. **Fork and Clone**: Create your own fork and clone it locally.
2. **Branch**: Create a feature branch (`git checkout -b feat/my-awesome-feature`).
3. **Prerequisites**: Rust **1.87** (MSRV — the workspace `Cargo.toml`, `Dockerfile`, and `ci.yml` move together) and `protoc` for the OTLP schema (`make build`/`make test` auto-fetch it, or `sudo apt-get install protobuf-compiler`).
4. **Develop**: Make your changes. Ensure you follow the [Developer Guide](docs/DEVELOPER_GUIDE.md).
5. **Test**: Run `make test` (or `cargo test --workspace`) to ensure no regressions.
6. **Lint**: Run `make lint` — the exact command CI uses: `cargo fmt --check` + `cargo clippy --workspace --all-targets --locked -- -D warnings`. Note `unsafe` code is forbidden workspace-wide.
7. **Commit**: Use [Conventional Commits](https://www.conventionalcommits.org/) (e.g., `feat: add support for S3 storage`).
8. **Submit**: Open a Pull Request against the `main` branch using the [PR template](.github/PULL_REQUEST_TEMPLATE.md).

### Performance changes

If your PR claims to improve performance, it needs a measurement. State the
number, the command that produced it, and the dataset. "Improves performance"
without a before/after is not reviewable.

The repository has three benchmark harnesses, all runnable in release mode:

```bash
cargo run --release -p parqtel-core    --example bench_bloom       # pruning and projection
cargo run --release -p parqtel-core    --example probe_labels_size # labels column cost
cargo run --release -p parqtel-query   --example bench_query_concurrency
cargo run --release -p parqtel-query   --example bench_logql
cargo run --release -p parqtel-server  --example perf_bench
```

Results, and the cases where measurement showed a proposed optimisation was not
worth doing, are in [docs/benchmarks/PERFORMANCE.md](docs/benchmarks/PERFORMANCE.md).
The harness currently runs by hand — see the open item in that doc about adding a
CI gate.

### Breaking changes

Changing a query's results, the on-disk block format, a configuration default,
or the HTTP API is a breaking change even when no signature changes. Say so in
the PR, and put it under **Unreleased** in [CHANGELOG.md](CHANGELOG.md).

### Review

Every PR is reviewed by at least one maintainer. Changes to storage, the write-ahead
log, ingest durability, configuration schema, CI or packaging have explicit owners
in [.github/CODEOWNERS](.github/CODEOWNERS).

## 4. Code Review Criteria
Every PR will be reviewed by at least one maintainer. We look for:
- **Correctness**: Does the code solve the problem?
- **Performance**: Does it introduce unnecessary overhead?
- **Tests**: Are there unit/integration tests for the new logic?
- **Documentation**: Are new features or configuration flags documented?

## 5. Community

- **Discussion**: [GitHub Discussions](https://github.com/parqtel/parqtel-oss/discussions) is where questions and proposals live.
- **Support**: see [SUPPORT.md](SUPPORT.md). Parqtel is maintained by volunteers; there is no guaranteed response time.
- **Security**: report privately per [SECURITY.md](SECURITY.md), not as an issue.

We look forward to your contributions!
