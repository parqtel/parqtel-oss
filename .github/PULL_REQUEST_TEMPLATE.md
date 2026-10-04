## Description

<!-- What this PR changes, and why. Link the issue it closes: Closes #123 -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Performance
- [ ] Refactor / internal cleanup (no behaviour change)
- [ ] Documentation
- [ ] Build / CI / packaging

## What changed and why

<!--
For a bug fix: what was wrong, and what made it wrong rather than
apparent? For a perf change: the measurement, before and after.

Please do not write "improves performance" without numbers. A PR that
cannot say what it measured, and what the measurement was, will be asked
for that measurement before it is reviewed.
-->

## Verification

<!-- How you checked this. Delete what does not apply. -->

- [ ] `make lint` (`cargo fmt --check` + `cargo clippy --workspace --all-targets --locked -- -D warnings`)
- [ ] `make test` (`cargo test --workspace --locked`)
- [ ] Tested against a running instance (`make local-rebuild` + the suites in `make help`)

<!--
For anything touching ingest, storage or query, say which suites you ran
and what the data volume was. The suites are real; they have caught
regressions the unit tests did not.
-->

### Measured results (performance changes only)

<!--
Before:
After:
Method: <command> e.g. cargo run --release -p parqtel-core --example bench_bloom
Dataset / environment: <what was ingested, block sizes, hardware>
-->

## Checklist

- [ ] Tests added or updated for the new behaviour
- [ ] Documentation updated (`docs/`, `README.md`) if behaviour or configuration changed
- [ ] New configuration options documented in `docs/CONFIGURATION.md`
- [ ] No `unwrap`, `expect` or `panic` outside tests (`unsafe_code` is forbidden workspace-wide)
- [ ] Commit messages follow [Conventional Commits](https://www.conventionalcommits.org/)

## Breaking changes

<!--
Any change to the HTTP API, on-disk format, configuration schema or CLI
behaviour that would break an existing deployment. Write "None" if none.

Note that changing a query's results, the block format, or a default is
breaking even if no function signature changed.
-->

## Screenshots

<!-- UI changes only. -->
