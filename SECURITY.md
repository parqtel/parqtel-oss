# Security Policy

## Supported Versions

Security fixes land on the latest minor release only.

| Version | Supported |
|---|---|
| 0.2.x | :white_check_mark: |
| < 0.2   | :x: |

> `0.2.0` is the initial **public beta**. It has no authentication or
> authorization: do not expose the HTTP (`8080`) or OTLP gRPC (`4317`) ports
> publicly without a reverse proxy that provides it. See
> [KNOWN_ISSUES.md](docs/KNOWN_ISSUES.md).

The eight `parqtel-mcp-*` crates do not currently inherit the workspace lint
table, so `unsafe_code` is not formally forbidden there. A full audit found
**zero** `unsafe` uses in them, but the guarantee is not enforced and should be.

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do NOT** open a public GitHub issue.
2. Email **security@parqtel.com** with:
   - Description of the vulnerability
   - Steps to reproduce
   - Potential impact
3. You will receive acknowledgment within 48 hours.
4. We will work with you to understand and address the issue before public disclosure.

## Scope

This policy applies to the `parqtel-oss` repository and all crates within it.
