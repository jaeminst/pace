# ADR 0015 — The transport package returns to the standard library

**Status:** accepted (v0.4.0)

## Context

`pace/transport` was one constructor: `New(transport.Config) *http.Transport`,
a builder over the standard library's transport with DefaultTransport-flavoured
defaults. Its reason to exist was a real footgun, stated in its own package doc:
the zero `http.Transport` is not `http.DefaultTransport` — reaching for a bare
one to change a single timeout silently drops the environment proxy and HTTP/2.

Three observations retired it:

**Nothing in pace used it.** `config.Config.Transport` is an
`http.RoundTripper`, so the package was optional sugar; its only in-module
importers were one wiring test and its own example. It was surface without a
dependent.

**The standard library already owns the fix.** `http.DefaultTransport` is an
`*http.Transport`, and `Clone()` copies it — proxy from the environment,
connection-pool defaults, and `ForceAttemptHTTP2` included. "Clone, then change
the fields you mean to" protects against exactly the footguns `transport.New`
existed for, down to the subtle one: a custom `TLSClientConfig` normally
disables automatic HTTP/2, but `ForceAttemptHTTP2` — which the clone inherits —
keeps it on, which is what the package's `DisableHTTP2` doc was guarding.

**The package was a third dialect.** Its `Config`'s zero value behaved like
DefaultTransport, which is neither what a zero `http.Transport` does nor what
its field types suggest, and it spelled "disabled" as `-1` where the standard
library spells it `0`. Anyone who already knew `http.Transport` had to learn a
second mapping onto it; anyone who did not was better served learning the real
one.

[ADR 0005](0005-pace-ships-contracts-not-backends.md) frames the same
conclusion from the other side: connection tuning is generic `net/http`
configuration, not rate limiting. It was neither a contract someone implements
against nor a part of the machine — the CONTRIBUTING guide had in fact
misfiled it as "one contract each" alongside `store`, `shared` and `observe`.

## Decision

**Delete `pace/transport`.** `Config.Transport` keeps taking any
`http.RoundTripper`; the README teaches the standard idiom in its place:

```go
tr := http.DefaultTransport.(*http.Transport).Clone()
tr.MaxIdleConnsPerHost = 10
cfg.Transport = tr
```

The footgun knowledge does not vanish — it moves into the README's HTTP
section and the mTLS example, stated against the standard library's own names
so it stays checkable against the standard library's own docs.

## What is genuinely lost

`transport.New` set `ResponseHeaderTimeout` to 30s by default.
`http.DefaultTransport` has none, so a server that accepts the connection and
never sends headers was bounded by default and now is not — for `Request.Stream`
in particular, which deliberately bypasses `Config.RequestTimeout` and leaned on
that default in its documentation.

The defence is now opt-in. `client/stream.go`, `Config.RequestTimeout`'s doc and
the README all say so and tell the caller to set
`http.Transport.ResponseHeaderTimeout` themselves. That is a weakening, accepted
with eyes open: a default that lives in an optional helper package only protects
the callers who found the helper, which is the least effective place to keep a
safety net.

## Alternatives considered

**Keep it.** The strongest case was the `ResponseHeaderTimeout` opinion and a
typed field table in godoc. Against that: a whole package, its own zero-value
semantics, and a second spelling of ten `http.Transport` fields — permanent
weight for one default a README line can teach.

**Deprecate instead of delete.** Pre-v1, this repository deletes — the SQLite
store, the durable queue, `pace/rate`, `config.Spec` and `config.Fixed` all
went the same way. A deprecated package still ships, still documents, and still
teaches its dialect.

**Move `New` into `client`.** Same builder, one fewer package, all the same
objections minus one.
