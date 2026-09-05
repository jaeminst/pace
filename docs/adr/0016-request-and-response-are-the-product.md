# ADR 0016 — Request and Response are the product

**Status:** accepted (post-v0.4.0)

## Context

[ADR 0015](0015-the-transport-package-returns-to-the-standard-library.md)
deleted `pace/transport` for being a second spelling of `http.Transport` that
nothing depended on. The obvious next question arrived immediately:
`client.Request` and `client.Response` also look like re-spellings of things
`net/http` already has. Should they go the same way?

ADR 0015 left three retirement tests behind: does anything depend on it, does
the standard library already own the fix, and is it a second dialect. This
record is those tests run against `Request` and `Response` — and coming out the
other way.

## Decision

**They stay.** `Request` and `Response` are not spellings of `net/http` types;
they are where pace's request-path behaviour lives. Deleting them would not
remove a dialect — it would transfer the library's obligations to every caller.

### Test 1 — dependents

`transport` had none: one wiring test and its own example. `Request` and
`Response` are the request path. `Pool`, `Client` and `Stream` are all built on
them, roughly 1,700 lines of tests pin their contract, and every terminal method
a caller touches returns one. The zero *external* importers mean the opposite of
what they meant for `transport`: nothing imports these types because they are
the destination, not because they are unvisited.

### Test 2 — does the standard library own it?

For `transport`, `http.DefaultTransport.(*http.Transport).Clone()` was the
answer. For the request path there is no standard-library answer to any of:

- **Build before pay, pay before timing** (`client/request.go`, `send`): a
  malformed URL fails without costing a token, and `RequestTimeout` is attached
  only after `Acquire` returns, so a throttled wait never counts against the
  round-trip's clock. `http.Client` has no notion of any of this.
- **No caller-supplied URL** (`client/request.go`, `build`): every request URL
  is `urlx.Build(baseURL, path, query)`, and `baseURL` is fixed at `New`. This
  is the structural form of the host-retargeting defence — fuzzing once found
  `"https://api.example.com" + ".evil.com/x"` becoming a request to a host the
  caller never named (`urlx/urlx.go`, `Build`'s doc), and the reason that class
  of bug cannot recur is that the request path has no entrance a full URL fits
  through. This sentence had never been written down before this record; the
  guard existed only implicitly, as the absence of an API.
- **A Response is a finished report**: by the time a caller holds one, the body
  has been drained (so the connection returns to the pool), closed (so it cannot
  leak), and bounded by `Config.MaxResponseBytes` (`readBody`, which reads one
  byte past the cap so truncation is an error rather than a plausible-looking
  body). `Body() []byte` differing from `http.Response.Body io.ReadCloser` is
  the feature, not the dialect.
- **`RetryAfter()`** handles both header forms, refuses negatives, clamps the
  delta-seconds form against int64 overflow from a hostile server, and measures
  the HTTP-date form against the injected clock. `net/http` hands back the raw
  string.
- **`Stream`'s `releasingBody`** ties the caller-owned body's `Close` to the
  shutdown barrier, so `Shutdown` is not held hostage by an unclosed download.

The raw escape hatch already exists and is one method: `Stream` returns
`*http.Response` for the case where buffering is wrong — and even it keeps the
token accounting and the barrier release.

### Test 3 — the dialect, admitted

This is the one test the types partly fail, and it is worth being exact about.
Seven builder setters — `SetHeader`, `AddHeader`, `SetQuery`, `AddQuery`,
`SetQueryValues`, `SetBody`, and the verb methods over `do` — are one-line
delegations to `http.Header` and `url.Values`. Four `Response` accessors read
stored fields `http.Response` also has.

They stay because the chaining is load-bearing, not decorative: `SetJSON` defers
its marshal error into the builder, which is what makes "building a Request
costs nothing and cannot fail — no token until a terminal method" a true
sentence. A builder you abandon on a validation failure, an early return or a
panic leaves the key's quota untouched. That invariant needs a type to live on,
and once the type exists, `SetHeader` costing one delegating line is cheaper
than every caller juggling a bare `http.Header` beside it.

## Alternatives considered

**`Client.Do(ctx, *http.Request) (*http.Response, error)`.** The reshape that
"just uses net/http". It deletes the retargeting defence — a caller-built
request names its own host, so `BaseURL` stops meaning anything and the guard
has to be rebuilt as validation that can be forgotten. It hands back an open
body, exporting the drain/close/bound obligations to every call site. And it
leaves nowhere to put the build-before-pay ordering, since the request arrives
already built.

**Trim the pass-through setters, keep the terminals.** Callers would mutate
`Header()` and call `SetQueryValues` directly. A few lines of surface saved, the
fluent path through `SetJSON`'s deferred error broken, and the dialect largely
still present — the worst of both.

**Delete, as transport was deleted.** Fails tests 1 and 2 above. `transport`
was a convenience beside the product; this is the product.
