# Security Policy

## Reporting a Vulnerability

Please **do not** open a public GitHub issue for security vulnerabilities.

Instead, use [GitHub Security Advisories](https://github.com/jaeminst/pace/security/advisories/new) to report privately. We aim to respond within 5 business days and will coordinate a fix and disclosure timeline with you.

## Scope

pace is a client-side rate-limiting library. The primary attack surface is:

- **SQLite persistence** — file path is caller-supplied; ensure the path is not user-controlled.
- **HTTP transport** — pace forwards the caller-supplied `http.RoundTripper`; use TLS-verifying transports in production.
