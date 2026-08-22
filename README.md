# pace

[![Go Reference](https://pkg.go.dev/badge/github.com/jaeminst/pace.svg)](https://pkg.go.dev/github.com/jaeminst/pace)
[![Go Report Card](https://goreportcard.com/badge/github.com/jaeminst/pace)](https://goreportcard.com/report/github.com/jaeminst/pace)
[![CI](https://github.com/jaeminst/pace/actions/workflows/test.yml/badge.svg)](https://github.com/jaeminst/pace/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**pace** is a zero-CGO Go library for per-user outbound HTTP rate limiting.

Each user gets an independent token bucket, so one user's traffic never affects another's quota. A single background goroutine handles idle-user garbage collection; the number of goroutines does not grow with the number of active users.

## Features

- **Per-user isolation** — an independent token bucket per user identity
- **Per-user quotas** — one rate for everyone, or a different one per user via `QuotaFor`
- **Optional cross-replica limiting** — delegate to a backend you supply, with a local shadow bucket that only refuses
- **Configurable bursting** — token-bucket algorithm with an adjustable burst ceiling
- **Pluggable persistence** — a context-aware `store.Store` for any backend, with the contract shipped as an executable test suite
- **Sharded user map** — lock striping across 256 shards, with no store I/O held under a lock
- **Graceful shutdown** — `Shutdown(ctx)` genuinely waits for in-flight requests
- **Observable** — `Stats()` for gauges, `Observer` hooks for metrics and tracing
- **Testable by design** — injectable `Clock` and `http.RoundTripper`

## Install

```sh
go get github.com/jaeminst/pace
```

Requires **Go 1.26.6+**.

## Quick Start

A `Limiter` owns the shared machinery and is what you create and close. A `Client` is a lightweight handle bound to one user. Both live in `pace/limiter`; `pace` itself holds `Config` and `New` and nothing else — see [Package layout](#package-layout).

```go
import (
    "github.com/jaeminst/pace"
    "github.com/jaeminst/pace/limiter"
)

lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    limiter.PerMinute(60),
    Burst:   10,
})
if err != nil {
    log.Fatal(err)
}
defer lim.Close()

alice := lim.Client("alice")
bob := lim.Client("bob")

// Each user has their own quota; alice cannot starve bob.
resp, err := alice.Get(ctx, "/v1/items/42")
resp, err = bob.Get(ctx, "/v1/items/99")
```

For anything beyond a bare verb, build the request first. Building costs nothing — the rate-limit token is taken when the request is sent, so a builder you abandon does not burn quota:

```go
resp, err := alice.Request().
    SetHeader("X-Request-ID", requestID).
    SetQuery("limit", "10").
    SetJSON(payload).
    Post(ctx, "/v1/items")
```

### Pacing work pace does not perform

`Wait` blocks for a token; `Allow` takes one without blocking.

```go
if !alice.Allow(ctx) {
    return errTooBusy // shed load rather than queue behind it
}

if err := alice.Wait(ctx); err != nil {
    return err // ctx expired, or the Limiter closed
}
```

`Reserve` is the middle ground: it tells you how long the wait would be and
lets you change your mind, which neither of the other two can do. With
`shared.Backend` configured it consults the backend like everything else, and
`Cancel` then returns only the local token — see
[ADR 0004](docs/adr/0004-shared-quota-is-approximate.md).

```go
r := alice.Reserve(ctx)
if !r.OK() || r.Delay() > tolerable {
    r.Cancel() // hand the token back; otherwise the user is charged for nothing
    return errTooBusy
}
time.Sleep(r.Delay())
// … now make the call
```

None of `Wait`, `Allow` or `Reserve` sends anything, so a `Client` paces work
pace does not perform on your behalf just as well as a request:

```go
if err := lim.Client("alice").Wait(ctx); err != nil {
    return err
}
writeToTheDatabase()
```

It is the same bucket the HTTP path draws on, so a token taken here is a token a
request will not get.

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `BaseURL` | `string` | — | **Required.** Absolute `http`/`https` URL prepended to every path. |
| `Rate` | `Limit` | — | **Required (> 0).** The default rate. Build with `PerSecond`, `PerMinute`, `PerHour`, `Every`, or `Inf`. |
| `Burst` | `int` | 1 | The default ceiling: tokens that may accumulate while a user is idle. |
| `QuotaFor` | `func(string) Quota` | nil | Overrides `Rate` and `Burst` per user. See [Per-user quotas](#per-user-quotas). |
| `IdleExpiry` | `time.Duration` | 10m | How long a user may be inactive before their state is collected. |
| `GCInterval` | `time.Duration` | 1m | How often the idle-user sweep runs. |
| `Shards` | `int` | 256 | Lock-striping width; rounded up to a power of two. |
| `Transport` | `http.RoundTripper` | `http.DefaultTransport` | See [HTTP Connection Configuration](#http-connection-configuration). |
| `RequestTimeout` | `time.Duration` | 0 (none) | Bounds one round-trip. Excludes time spent waiting for a token. |
| `MaxResponseBytes` | `int64` | 0 (unlimited) | Caps the buffered response body. |
| `Clock` | `Clock` | system | Injectable clock, for deterministic tests. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives internal warnings. |
| `Observer` | `*Observer` | nil | Hooks for throttling, requests and evictions. Every hook takes a context. |
| `Store` | `store.Store` | nil | Backend for per-user token state. Without one, a restart starts every user at a full burst. |
| `StoreTimeout` | `time.Duration` | 5s | Bounds each `store.Store` call. |
| `Shared` | `shared.Config` | zero | Cross-replica limiting; see below. Ignored unless `Shared.Quota` is set. |

## Per-user quotas

`Rate` and `Burst` set the default. `QuotaFor` grades individual users against
it — a free tier and a paying one, or one customer with a negotiated ceiling:

```go
tiers := map[string]limiter.Quota{
    "acme-corp": {Rate: limiter.PerMinute(600), Burst: 50},
    "trial-42":  {Rate: limiter.PerMinute(6)},   // Burst falls back to Config.Burst
}

cfg.QuotaFor = func(userID string) limiter.Quota {
    return tiers[userID] // an unlisted user gets the zero Quota, i.e. the defaults
}
```

Each field falls back on its own, so the zero `Quota` means "the defaults" and a
partial override changes only what it names.

`QuotaFor` is consulted when a user's bucket is created — their first request,
or the first after an eviction — never on the hot path, and never while a shard
lock is held. Keep it to a map lookup regardless; it must not do I/O.

To change a tier while the process runs, update whatever `QuotaFor` reads and
then call `ReloadQuotas`:

```go
tiers["trial-42"] = limiter.Quota{Rate: limiter.PerMinute(600), Burst: 50}
lim.ReloadQuotas() // applies to live buckets, keeping tokens already accrued
```

`ReloadQuotas` walks every shard, so it is a maintenance operation rather than
something to call per request. Users not in memory need nothing: their bucket is
built from `QuotaFor` when they next appear. For a single user, `Evict` has the
same effect.

`client.Quota()` reports what a user's bucket is enforcing now, and
`LimitError` and `ThrottleInfo` carry that same per-user quota rather than the
Limiter-wide default.

Persisted state carries no quota. A user restored from a `store.Store` gets
whatever `QuotaFor` returns at that moment, with their saved tokens capped at
the current burst — so a demotion takes effect on the next restore instead of
handing back a ceiling they no longer have.

## Rate limiting across replicas

**Read this paragraph before the code.** Most services that want "distributed
rate limiting" are better off setting `Rate` to their share of the upstream
limit and handling 429s properly. That costs nothing, adds no dependency, and is
within a constant factor of correct whenever load is roughly even across
replicas. `shared.Backend` buys accuracy when load is genuinely *uneven*, or when
the upstream limit is a contractual cap rather than a throttle — and it charges
an operational dependency on every outbound call path for it.

Still want it? Supply a backend every replica consults:

```go
cfg.Shared = shared.Config{
    Quota:     myRedisQuota,  // you implement this; see below
    Namespace: "billing-api", // so several Limiters can share one backend
}
```

### `Config.Shared`

| Field | Type | Default | Description |
|---|---|---|---|
| `Backend` | `shared.Backend` | nil | The token supply every replica consults. Nil limits per process. |
| `Namespace` | `string` | "" | Passed to the backend, so several Limiters can share one. |
| `Timeout` | `time.Duration` | 500ms | Bounds each `shared.Backend` call. |
| `OnError` | `shared.ErrorPolicy` | `shared.FallbackLocal` | What happens when the backend is unreachable. |

The local bucket stays, as a *shadow* that can only refuse. It never admits a
request the backend has not admitted, so it costs nothing in correctness — and
it saves a round-trip for every request this replica can already tell is over
its own share.

When the backend is unreachable, `Shared.OnError` decides:

| Policy | Behaviour |
|---|---|
| `shared.FallbackLocal` (default) | Serve against the local bucket — the configured rate *per replica*. Same trade pace makes for `store.Store`. |
| `shared.Deny` | Refuse with `ErrQuotaUnavailable`. For a hard contractual cap. |
| `shared.Allow` | Serve without asking. For an advisory limit where availability wins. |

pace ships no backend. A Redis implementation would be a second module to
version and support, and its correctness would live in a Lua script most users
would never read. What pace ships instead is the contract, executable:

```go
func TestMyRedisQuota(t *testing.T) {
    quotatest.Suite(t, func(t *testing.T) shared.Backend {
        return myredis.New(startRedis(t))
    })
}
```

The suite asserts what pace relies on and cannot check at run time: `Take` is
atomic under concurrency, a refusal consumes nothing, `RetryAfter` is long
enough that a retry can succeed, users and namespaces are independent, and the
context is honoured so `QuotaTimeout` means something.

Two consequences worth knowing before you adopt it. `Client.Allow` gains a
bounded backend call, which matters most when it is used as an inbound load
shedder. And with a shared quota configured the local bucket is never persisted
to a `store.Store` — it describes what *this replica* spent, not what the user
spent, so restoring one replica's snapshot into another would be wrong.

[ADR 0004](docs/adr/0004-shared-quota-is-approximate.md) states what is and is
not guaranteed. The short version: accuracy is a property of the backend you
plug in, a partition degrades to N × `Rate` by default, there is no fairness,
and upstream's 429 remains the authority regardless.

## Errors

```go
resp, err := alice.Get(ctx, "/v1/items/42")

var le *limiter.LimitError
switch {
case errors.Is(err, limiter.ErrClosed):
    // The Limiter is shutting down.
case errors.As(err, &le):
    // Throttled. le.Delay is how long the wait would have been.
}
```

A non-2xx response is **not** an error: a 404 is a successful round-trip. Check `resp.OK()` or `resp.StatusCode()`.

## Responses

```go
if !resp.OK() {
    if after, ok := resp.RetryAfter(); ok {
        // Upstream told you its real limit; that beats guessing.
        time.Sleep(after)
    }
}

var items []Item
if err := resp.JSON(&items); err != nil {
    return err
}
```

Bodies are buffered, so set `Config.MaxResponseBytes` when you do not control the far end. For a response too large to hold in memory, stream it:

```go
raw, err := alice.Request().Stream(ctx, http.MethodGet, "/v1/export")
if err != nil {
    return err
}
defer raw.Body.Close() // releases the request; Shutdown waits for it
io.Copy(dst, raw.Body)
```

## Pluggable Persistence (`store.Store`)

By default pace is in-memory only, and a restart starts every user at a full burst. pace ships no backend — implement two methods against whatever already holds your state:

```go
type Store interface {  // package store
    Save(ctx context.Context, userID string, state State) error
    // Returning (State{}, false, nil) when nothing is stored is valid.
    Load(ctx context.Context, userID string) (State, bool, error)
}
```

Two methods. If your store also needs tearing down, implement `io.Closer` —
`Close` and `Shutdown` find it by type assertion, the same way `store.BatchStore`
extends this interface. **pace closes what it finds**, so do not hand one store
to two Limiters unless you want the first shutdown to close it for both.

Every call receives a context bounded by `Config.StoreTimeout`, so a network-backed store can honour cancellation rather than block the request path. A store that fails or times out is logged and treated as "no saved state": the user starts from a fresh bucket instead of the request failing.

**Check yours against the contract.** The properties pace relies on cannot be verified at run time, and two of them fail silently when a backend gets them wrong — a miss reported as an error, and a `LastUsed` truncated to whole seconds. `store/storetest` is those properties as a test suite:

```go
func TestMyRedisStore(t *testing.T) {
    storetest.Suite(t, func(t *testing.T) store.Store {
        return myredis.New(startRedis(t))
    })
}
```

Run it with `-race`; one of the checks is there for the detector. `store/memory` is an in-memory implementation that passes it, useful as a test double and as the shortest correct answer to what implementing the contract involves — it is not persistence, since nothing it holds survives the process.

**Example — Redis backend:**

```go
type RedisStore struct {
    client *redis.Client
    prefix string
}

func (r *RedisStore) Save(ctx context.Context, userID string, st store.State) error {
    data, err := json.Marshal(st)
    if err != nil {
        return err
    }
    return r.client.Set(ctx, r.prefix+userID, data, 24*time.Hour).Err()
}

func (r *RedisStore) Load(ctx context.Context, userID string) (store.State, bool, error) {
    data, err := r.client.Get(ctx, r.prefix+userID).Bytes()
    if errors.Is(err, redis.Nil) {
        return store.State{}, false, nil
    }
    if err != nil {
        return store.State{}, false, err
    }
    var st store.State
    return st, true, json.Unmarshal(data, &st)
}


lim, _ := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    limiter.PerMinute(60),
    Store:   &RedisStore{client: redisClient, prefix: "pace:"},
})
```

The idle-user sweep can evict thousands of users at once. If a round-trip each would hurt, implement the optional `store.BatchStore` too and pace will hand you whole batches:

```go
func (r *RedisStore) SaveBatch(ctx context.Context, states []store.UserState) error { ... }
```

## Observability

`Stats()` is a cheap snapshot, suitable for a scrape interval:

```go
s := lim.Stats()
// s.Users, s.Requests, s.Throttled, s.Wait, s.Errors, s.Evictions
```

`Observer` pushes events as they happen. It is a struct of optional functions rather than an interface, so new events can be added without breaking your code:

```go
cfg.Observer = &observe.Observer{
    Throttled: func(_ context.Context, i observe.ThrottleInfo) {
        throttleDelay.WithLabelValues(i.UserID).Observe(i.Delay.Seconds())
    },
    RequestFinished: func(_ context.Context, i observe.RequestInfo) {
        latency.WithLabelValues(i.Method, strconv.Itoa(i.Status)).Observe(i.Latency.Seconds())
    },
}
```

Hooks run on the caller's goroutine, in the request path. Keep them to a counter increment.

## HTTP Connection Configuration

By default pace uses `http.DefaultTransport`. Use `transport.New` to tune connection behaviour before passing it to `Config.Transport`:

```go
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    limiter.PerMinute(60),
    Transport: transport.New(transport.Config{
        DialTimeout:           5 * time.Second,  // TCP connection timeout
        TLSHandshakeTimeout:   3 * time.Second,  // TLS handshake timeout
        ResponseHeaderTimeout: 10 * time.Second, // wait for the response headers
        KeepAlive:             30 * time.Second, // TCP keep-alive probe interval
        MaxIdleConnsPerHost:   10,               // idle connections per host
    }),
})
```

A zero `transport.Config` behaves like `http.DefaultTransport`, not like a bare `http.Transport`. That distinction matters in two places: the environment proxy is honoured unless you replace it, and HTTP/2 is attempted even when you supply a `TLSConfig`.

### `transport.Config` fields

| Field | Default | Description |
|---|---|---|
| `DialTimeout` | 30s | Maximum time to establish a TCP connection. |
| `KeepAlive` | 30s | Interval between TCP keep-alive probes. Set to `-1` to disable. |
| `TLSHandshakeTimeout` | 10s | Maximum time to complete a TLS handshake. |
| `ResponseHeaderTimeout` | 30s | Maximum time to wait for response headers. Set to `-1` to wait indefinitely. |
| `ExpectContinueTimeout` | 1s | How long to wait for `100 Continue` before sending the body. |
| `MaxIdleConns` | 100 | Maximum idle (keep-alive) connections across all hosts. |
| `MaxIdleConnsPerHost` | 2 | Maximum idle connections kept per host. |
| `MaxConnsPerHost` | 0 (no limit) | Cap on total connections per host, idle or in use. |
| `IdleConnTimeout` | 90s | How long an idle connection stays open before being closed. |
| `Proxy` | `http.ProxyFromEnvironment` | Proxy selection. Supply a function returning `(nil, nil)` to bypass proxies. |
| `TLSConfig` | nil | Custom `*tls.Config` (e.g. client certificates, custom CA). |
| `DisableHTTP2` | false | Turn off automatic HTTP/2. |
| `DisableCompression` | false | Turn off transparent gzip. |

`ResponseHeaderTimeout` is on by default because nothing else catches a server that accepts the connection and then never answers. It does not limit how long a slow response body may take to arrive.

### Custom TLS (mutual TLS / self-signed CA)

```go
cert, err := tls.LoadX509KeyPair("client.crt", "client.key")
if err != nil {
    log.Fatal(err)
}
caCert, err := os.ReadFile("ca.crt")
if err != nil {
    log.Fatal(err)
}
pool := x509.NewCertPool()
pool.AppendCertsFromPEM(caCert)

lim, err := pace.New(pace.Config{
    BaseURL: "https://internal.example.com",
    Rate:    limiter.PerMinute(60),
    Transport: transport.New(transport.Config{
        TLSHandshakeTimeout: 5 * time.Second,
        TLSConfig: &tls.Config{
            Certificates: []tls.Certificate{cert},
            RootCAs:      pool,
            MinVersion:   tls.VersionTLS12,
        },
    }),
})
```

net/http disables automatic HTTP/2 as soon as a transport carries a custom `TLSClientConfig`. `transport.New` sets `ForceAttemptHTTP2` so this configuration still negotiates HTTP/2; pass `DisableHTTP2: true` if you want HTTP/1.1.

## Graceful Shutdown

`Shutdown(ctx)` stops accepting new requests and waits for in-flight ones — including streamed responses whose bodies are still open — before flushing and closing the store. If `ctx` expires first, the remaining requests are cancelled and `Shutdown` returns `ctx.Err()`. The store is flushed either way.

```go
// On SIGTERM: give in-flight requests five seconds to finish.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := lim.Shutdown(ctx); err != nil {
    log.Printf("shutdown forced: %v", err)
}
```

`Close()` does not wait: it cancels in-flight work, then flushes. Use `Shutdown` when you want requests to finish.

## Package layout

`pace` is the front door and only that: `Config`, its validation and its
defaults, plus `New`. Four exported names.

`pace/limiter` is everything you touch after `New` — the `Limiter` it returns,
the `Client` you bind a user to, the `Request` you build, the `Response` you
read, and the rate vocabulary (`Limit`, `Quota`, `PerMinute`, `Inf`). So you
import two packages, and in exchange **every name in this library is declared
exactly once**: Go renders a type alias as a single line with no methods, so a
re-exported `Limiter` documented nothing and sent you one package over anyway.
See [ADR 0008](docs/adr/0008-the-root-re-exports-nothing.md).

Everything else you supply to a Limiter, or that it reports back, is a package
of its own — so a contract is documented where it is implemented rather than as
one line in a list of configuration fields.

| Package | What is in it |
|---|---|
| [`pace`](https://pkg.go.dev/github.com/jaeminst/pace) | the front door: `New`, `Config`, `Clock`, `ConfigError` |
| [`pace/limiter`](https://pkg.go.dev/github.com/jaeminst/pace/limiter) | the engine and the request path: `Limiter`, `Client`, `Request`, `Response`, and the rate vocabulary — `Limit`, `Quota`, `PerMinute` and friends |
| [`pace/store`](https://pkg.go.dev/github.com/jaeminst/pace/store) | `Store` — the persistence contract you implement |
| [`pace/shared`](https://pkg.go.dev/github.com/jaeminst/pace/shared) | `Backend` — the cross-replica token supply you implement |
| [`pace/shared/quotatest`](https://pkg.go.dev/github.com/jaeminst/pace/shared/quotatest) | the conformance suite for the above |
| [`pace/observe`](https://pkg.go.dev/github.com/jaeminst/pace/observe) | `Observer`, `Stats` and the event structs |
| [`pace/transport`](https://pkg.go.dev/github.com/jaeminst/pace/transport) | HTTP connection tuning |

Below those sit the pieces the engine is built from. They are public because
they are worth reading, not because you are expected to assemble one — `pace.New`
does that. Their `Config` is a required-everything vtable whose `New` panics on
a field left out, which is also true of `limiter.Spec`, the engine's own.

| Package | What is in it |
|---|---|
| [`pace/bucket`](https://pkg.go.dev/github.com/jaeminst/pace/bucket) | the token bucket, and the exact-restore arithmetic behind persistence |
| [`pace/registry`](https://pkg.go.dev/github.com/jaeminst/pace/registry) | the sharded user population, its GC sweep and state flush |
| [`pace/store/memory`](https://pkg.go.dev/github.com/jaeminst/pace/store/memory) | an in-memory `store.Store` — a reference implementation and a test double |
| [`pace/store/storetest`](https://pkg.go.dev/github.com/jaeminst/pace/store/storetest) | the persistence contract as a test suite — run your backend against it |
| [`pace/gate`](https://pkg.go.dev/github.com/jaeminst/pace/gate) | the shared-quota decision: shadow bucket, backend call, failure policy |
| [`pace/breaker`](https://pkg.go.dev/github.com/jaeminst/pace/breaker) | the shared-quota circuit breaker |
| [`pace/urlx`](https://pkg.go.dev/github.com/jaeminst/pace/urlx) | request URL construction |

No name in `pace` is an alias, because no name is published twice. `Config`,
`Clock` and `ConfigError` are declared at the root, because validating and
defaulting a configuration is the front door's job; everything else is declared
in the package that owns it. `limiter.Spec` is what `pace.New` fills a `Config`
into.

## How It Works

1. **Token bucket** — each user has a `golang.org/x/time/rate.Limiter`. Tokens refill at `Rate` per second, up to `Burst`.
2. **Sharded map** — user entries live in one of `Shards` stripes (FNV-1a hash). A hit takes a read lock; creating a user takes a write lock on one shard.
3. **GC sweep** — a background goroutine wakes every `GCInterval` and collects users idle longer than `IdleExpiry`. It snapshots under the lock, persists with no lock held, then deletes only what has not been touched since — so a slow store never blocks live traffic.
4. **Persistence** — token counts are saved on eviction and on close, and restored exactly, fractions included, accounting for time elapsed since.

## Benchmarks

Numbers are machine-specific; regenerate your own with `make bench`. A recorded baseline lives in `docs/bench/`.

What is worth knowing about the shape of the costs:

- The end-to-end benchmarks are dominated by the loopback HTTP round-trip. `BenchmarkRequest_NoHTTP` stubs the network out and is the honest measure of pace's own work — roughly 1.9µs and 21 allocations per request.
- Shard lookup is about 22ns for a 32-byte user ID, with no allocation.
- With persistence configured, a full sweep of 2,000 idle users takes ~9.6ms, none of it holding a shard lock.

## Testing

pace exposes an injectable `Clock` and accepts a custom `http.RoundTripper`, so tests need neither real time nor a real network:

```go
lim, _ := pace.New(pace.Config{
    BaseURL:    "http://example.invalid",
    Rate:       limiter.PerMinute(60),
    Clock:      myFakeClock,   // drive idle expiry and token refill directly
    Transport:  myStubTransport,
    GCInterval: time.Millisecond,
})
```

Freeze the clock when asserting on token counts: against a live one the bucket refills between readings, and "did this spend a token" stops being an exact question.

## Caveats

- **Rate limiting is per process** — the in-memory sharded map is not distributed, so multiple instances each enforce their own limit. A shared `store.Store` carries state across restarts, not across concurrent processes.

## Migrating from v0.1.0

See [MIGRATION.md](docs/MIGRATION.md).

## Contributing

See [CONTRIBUTING.md](.github/CONTRIBUTING.md).

## License

[MIT](LICENSE) © pace contributors
