# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.4.0]

### Removed

- **The `pace/transport` package.** It was one constructor — `transport.New`,
  a builder over `http.Transport` with DefaultTransport-flavoured defaults —
  and nothing in pace imported it: `Config.Transport` takes any
  `http.RoundTripper`, so the package was optional sugar with its own zero-value
  dialect (`-1` meant disabled; a zero `transport.Config` behaved like
  `http.DefaultTransport`, unlike a zero `http.Transport`).

  The footgun it guarded is real and the standard library owns the fix. A bare
  `&http.Transport{}` silently drops the environment proxy and HTTP/2; the
  idiom is to clone the default and change what you mean to:

  ```go
  tr := http.DefaultTransport.(*http.Transport).Clone()
  tr.MaxIdleConnsPerHost = 10
  cfg.Transport = tr
  ```

  The clone keeps `ProxyFromEnvironment` and `ForceAttemptHTTP2` — so HTTP/2
  survives a custom `TLSClientConfig`, the subtle case `transport.New`'s
  `DisableHTTP2` doc was guarding. The README's HTTP section now teaches this
  instead of the package.

  **One default is genuinely lost.** `transport.New` set
  `ResponseHeaderTimeout` to 30s; no default transport has one. That bounded a
  server that accepts the connection and never answers — the one hang
  `Request.Stream` cannot cover itself, since it bypasses `Config.RequestTimeout`
  on purpose. The defence is now opt-in: set
  `http.Transport.ResponseHeaderTimeout` on the transport you pass in. The
  Stream docs and the README both say so.

  See [ADR 0015](docs/adr/0015-the-transport-package-returns-to-the-standard-library.md).

### Fixed

- `Pool` kept private copies of the transport and the Pool-wide cookie jar
  beside the `http.Client` that already carries both. The per-key client
  assembly reads them off the client now — it is never written after `New`, so
  one storage place cannot drift from another.
- Two doc comments still pointed at `config.Config.QuotaFor`, a field two
  releases gone.

## [0.3.0]

### Added

- **`config.WithCookieJarFor`** — cookies scoped to a key. v0.2.1's
  `Config.CookieJar` is one jar for the whole Pool, which is right when the
  session is your service's and wrong when a key is an end user whose session it
  is: a cookie set while serving `alice` went back out for `bob`.

  ```go
  var jars sync.Map // key → http.CookieJar; yours to keep

  client.New(cfg, config.WithCookieJarFor(
      func(key string, def http.CookieJar) http.CookieJar {
          if j, ok := jars.Load(key); ok {
              return j.(http.CookieJar)
          }
          j, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
          actual, _ := jars.LoadOrStore(key, j)
          return actual.(http.CookieJar)
      }))
  ```

  The hook is handed the key and `Config.CookieJar` as its default — the same
  shape as `WithQuotaFor`, and for the same reason: the value being overridden
  arrives as an argument, so there is no precedence rule. Returning `def` keeps
  that key on the shared jar; returning nil sends and stores no cookies for that
  key.

  **The Pool keeps no per-key HTTP state.** With a hook, the `http.Client` is
  assembled per request around the jar the hook returns — a stateless struct;
  the connection pool lives in the shared `Transport`, so connection reuse is
  unchanged and the cost is one allocation plus the hook call. The jars
  themselves are the caller's: their growth, their eviction, and whether a
  session survives a restart are policy pace does not set, exactly as the tier
  map behind `WithQuotaFor` is. Evicting a key's rate-limiter state never
  touches a jar. See [ADR 0014](docs/adr/0014-the-pool-keeps-no-per-key-http-state.md).

  The hook runs on **every request** — unlike `WithQuotaFor`, which runs when a
  bucket is created — so keep it to a map lookup; it and the jars it returns
  must be safe for concurrent use.

  Without the option nothing changes, down to the code path: the same one shared
  `http.Client` as v0.2.1, pinned by the existing cookie tests. The v0.2.1
  section below says "one jar serves every key in the Pool" without
  qualification; as of this release that sentence describes the default rather
  than the only behaviour.

## [0.2.1]

### Added

- **`config.Config.CookieJar`** — the `*http.Client` pace builds could not carry
  cookies at all. It was constructed with `Transport` and nothing else, and
  nothing reached `http.Client.Jar`, so an upstream that authenticates with a
  session cookie could not be called through pace without carrying the header by
  hand on every request.

  ```go
  jar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})

  client.New(config.Config{
      BaseURL:   "https://api.example.com",
      Quota:     bucket.NewQuota("60/m", 10),
      CookieJar: jar,
  })
  ```

  The field is handed straight to `http.Client.Jar`, so the semantics are the
  standard library's: cookies are stored and replayed, redirects included. Both
  request paths already went through `http.Client.Do`, so nothing else changed.

  **Nil is still nil.** Without a jar pace sends and stores no cookies, exactly
  as before — a patch release that quietly started keeping cookies for callers
  who never asked would be a behaviour change wearing a bug-fix number.

  **One jar serves every key in the Pool.** A `Pool` owns one `http.Client`, so
  a cookie set while serving key `alice` goes back out for key `bob`. That is
  right when the cookie identifies your service to the upstream, and wrong when
  a key is an end user whose session it is; the field's doc says so, and
  `TestCookieJarIsSharedByEveryKey` pins it so the warning cannot quietly stop
  being true.

  pace takes no new dependency. `http.CookieJar` is a two-method standard
  library interface and the implementation is the caller's to supply — the same
  shape as `store.Store`, and for the same reason: a cookie policy is a choice
  pace should not make for you.

## [0.2.0]

Everything below is one release. v0.1.0 was a single package with the machinery
hidden under `internal/`; this is nine packages with the machinery public,
keyed on an opaque string rather than a user, and configured by a struct of
values plus options.

It is a rewrite of the surface, not of the behaviour: the token bucket, the
sharded map and the persistence policy are the same ideas, and most of the work
was finding out where they belonged.

```go
pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Quota:   bucket.NewQuota("60/m", 10),
})
defer pool.Close()

resp, err := pool.Client("alice").Get(ctx, "/items/42")
```

### The library is packages, not one file

`pace` itself declares nothing — `doc.go` alone, so the import path still
resolves to a documentation page. Everything is named for what it is:

| package | what it holds |
|---|---|
| `config` | everything a caller configures: `Config`, its validation and defaults, plus `Option` |
| `client` | `Pool`, `Client`, `Request`, `Response` — creating clients and the request path |
| `limiter` | the rate limiter and only that: no `net/http`, no URL handling |
| `bucket` | the token bucket and the rate vocabulary: `Quota`, `Limit`, `NewQuota`, `PerMinute`, `Inf` |
| `store`, `shared`, `observe` | one contract each, implemented from outside |
| `registry`, `gate`, `breaker`, `urlx`, `transport` | the parts, public because they are worth reading |

`internal/` is gone. The lower packages are public because a reader following a
stack trace should be able to open the file it names, not because a caller is
expected to assemble one.

See [ADR 0006](docs/adr/0006-the-root-is-the-composition-root.md),
[ADR 0008](docs/adr/0008-the-root-re-exports-nothing.md) and
[ADR 0009](docs/adr/0009-config-limiter-client.md).

### Keys, not users

A rate limit is keyed by a string. v0.1.0 called that string a user ID
throughout — `Client.For(userID)`, `SavedState`, `UserID` fields on every
report — which named one use of the library rather than the library.

It is `key` now, everywhere: `pool.Client(key)`, `LimitError.Key`,
`observe.ThrottleInfo.Key`, `store.KeyState`, `registry.Entry`. A tenant, an API
key, an IP, an endpoint and a user are all keys, and only one of them was in the
old name.

### One quota, written down

`Config.Quota` is a `bucket.Quota` — a rate and a burst — and it is the only
place a rate is configured:

```go
Quota: bucket.NewQuota("60/m", 10)                          // from a string
Quota: bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}  // the same value
```

`bucket.NewLimit` and `bucket.NewQuota` read a rate the way a person writes one
— `6/m`, `6/min`, `6rpm`, `1/s`, `100/hour`, `100RPH`, `2.5/s`, `inf` — and
panic on a typo, since the string is a literal in your source.
`bucket.ParseLimit` and `bucket.ParseQuota` are the same readers with an error
to check, for a rate that arrives from a file, a flag or an environment
variable. `Limit.String` round-trips through them.

`config.DefaultConfig(baseURL)` fills in 100 requests a minute with a burst of
10, so the first thing a reader writes is a URL and nothing else.

### Values are configuration; behaviour is an option

Both `New` functions take a `Config` and then options:

```go
func client.New(cfg config.Config, opts ...config.Option) (*Pool, error)
func limiter.New(cfg config.Config, opts ...config.Option) *Limiter
```

`Config` holds only values — things `Config.Resolve` can check before a request
is served, which is why a rate of zero is a construction error rather than one
key silently throttled to a standstill. Grading keys into tiers is
`config.WithQuotaFor`, and it is handed `Config.Quota` as its default argument,
so the two never compete: no precedence rule, no zero field meaning "inherit".

```go
client.New(cfg, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
    if q, ok := (*tiers.Load())[key]; ok {
        return q
    }
    return def
}))
```

Changing a rate while the process runs means swapping what that hook reads and
calling `Pool.ReloadQuotas` — or `Client.ReloadQuota` for one key, in O(1).
Applying is a separate step on purpose: a key with no bucket yet picks the
change up at once, and one already in memory keeps its accrued tokens until you
ask.

See [ADR 0012](docs/adr/0012-one-hook-holds-the-quota.md) and
[ADR 0013](docs/adr/0013-values-are-config-behaviour-is-an-option.md).

### pace ships contracts, not backends

v0.1.0 embedded a SQLite state store and a durable request queue. Both are gone.

`store.Store` is two methods — `Save` and `Load` — with `BatchStore` and
`io.Closer` as optional extensions a backend opts into by implementing them, not
by writing `func (s *S) Close() error { return nil }` because an interface said
so. `store/memory` is the reference implementation and `store/storetest` is the
contract as an executable suite: `storetest.Suite(t, factory)` and your backend
is either correct or it tells you which invariant it broke.

`shared.Backend` is the same shape for cross-replica limiting, with
`shared/quotatest` as its suite. The contract packages carry plain `float64`
and `int` so that implementing one never means compiling pace's vocabulary.

The durable queue went with the SQLite store. It was at-least-once delivery
wearing exactly-once clothing, and a queue is not what this library is for.

See [ADR 0005](docs/adr/0005-pace-ships-contracts-not-backends.md) and
[ADR 0007](docs/adr/0007-contracts-carry-numbers-not-types.md).

### Errors you can act on

- `*limiter.LimitError` is throttling: it carries the `Key`, the `Limit` and
  `Burst` in force for that key, and the `Delay` the caller would have waited.
- `limiter.ErrClosed` is shutdown. The two used to be confused for each other,
  so a caller was told the pool had closed when it was very much open.
- A non-2xx response is neither. The round-trip succeeded; check
  `Response.OK()` and `Response.RetryAfter()`, which reads upstream's own
  statement of its limit.
- `*config.Error` names the field: `pace: invalid Config.BaseURL: required`.

### Observability

`observe.Observer` is a struct of hooks — `Throttled`, `RequestFinished`,
`Evicted` — rather than an interface, so a new event is a new field instead of
a break for every implementation. Every hook takes a context.
`Pool.Stats()` reports requests, throttles, evictions and live keys.

### Fixed

- **Restoring a persisted bucket rounded the token count to a whole number**, so
  fractional credit was invented or lost on every restart and every eviction.
  With a burst of 1 a partial token restored as 0 and the key lost its credit
  outright. The restore is exact now, and fuzzed.
- **`RestoreBucket` read the wall clock** instead of the injected clock, which
  made the whole restore path untestable. Every path reads through
  `Config.Clock`.
- **A per-minute rate that does not divide 60s evenly was truncated** by routing
  it through a `time.Duration` interval. The conversion is exact.
- **A NaN or infinite rate produced a bucket holding NaN tokens** — one that
  refused every request for the life of the process. Found by fuzzing;
  `Config.Resolve` rejects it now, and what a `WithQuotaFor` hook returns is
  clamped and logged.
- **A key's rate and burst could be read as a pair nobody configured.**
  `x/time/rate` exposes them through two separately locked accessors, so a
  report could carry a combination that never existed. `Bucket.Quota()` is one
  atomic load. `-race` cannot see this one, so the guard is structural.
- **A throttle report and the quota being enforced could disagree** while a
  quota changed mid-request; the gate reads the quota off the bucket rather than
  taking it as an argument.
- **The documented way to grade keys was a data race.** Both the README and the
  package example wrote a plain map from the caller's goroutine while the hook
  read it from request goroutines. The docs say so now, and a real test — not an
  example, which `// Output:` pins to one goroutine — is the guard.
- **`Client.Tokens` reported a sentinel** for a key it had never seen, which a
  caller could not tell from a real count. It is `(float64, bool)`.
- **`sqlite` compared errors with `==` instead of `errors.Is`**, missing a
  wrapped one. The package is gone, but the mistake is worth recording.
- **CI claimed to enforce formatting via `go vet`**, which has never checked
  formatting; one file was in fact unformatted.

### Removed

- The `internal/` tree, the SQLite store, the durable queue, and the root
  re-exports. `pace` declares nothing.
- `Client.For(key)` — a `Pool` mints a `Client` per key instead.
- `Client.Durable` and everything under it.
- `StateStore` and `SavedState`, replaced by `store.Store` and `store.State`.

## [0.1.0]

Initial release.
