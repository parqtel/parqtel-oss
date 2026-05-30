# Contributing to Parqtel

Thank you for your interest in contributing to Parqtel! We welcome contributions of all kinds: bug reports, documentation improvements, new features, and feedback.

## 1. Our Values
- **Performance**: We strive for the smallest resource footprint and fastest ingestion.
- **Safety**: We use Rust to ensure memory safety and avoid runtime panics.
- **Simplicity**: We prefer simple, explicit solutions over complex abstractions.
- **Inclusivity**: Please read and follow our [Code of Conduct](CODE_OF_CONDUCT.md).

## 2. Reporting Issues
- **Bugs**: Use the [Bug Report](.github/ISSUE_TEMPLATE/bug_report.md) template. Please include your OS, Rust version, and steps to reproduce.
- **Features**: Use the [Feature Request](.github/ISSUE_TEMPLATE/feature_request.md) template to propose new ideas.
- **Security**: Please report security vulnerabilities privately according to our [Security Policy](SECURITY.md).

## 3. Pull Request Process
1. **Fork and Clone**: Create your own fork and clone it locally.
2. **Branch**: Create a feature branch (`git checkout -b feat/my-awesome-feature`).
3. **Develop**: Make your changes. Ensure you follow the [Developer Guide](docs/DEVELOPER_GUIDE.md).
4. **Test**: Run `cargo test --workspace` to ensure no regressions.
5. **Lint**: Run `cargo fmt` and `cargo clippy --workspace` to ensure code style compliance.
6. **Commit**: Use [Conventional Commits](https://www.conventionalcommits.org/) (e.g., `feat: add support for S3 storage`).
7. **Submit**: Open a Pull Request against the `main` branch.

## 4. Code Review Criteria
Every PR will be reviewed by at least one maintainer. We look for:
- **Correctness**: Does the code solve the problem?
- **Performance**: Does it introduce unnecessary overhead?
- **Tests**: Are there unit/integration tests for the new logic?
- **Documentation**: Are new features or configuration flags documented?

## 5. Community
- **Discussion**: Join our [GitHub Discussions](https://github.com/parqtel/parqtel-oss/discussions).
- **Slack/Discord**: (Add links here when available).

We look forward to your contributions!
