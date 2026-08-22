# pace

[![Go Reference](https://pkg.go.dev/badge/github.com/jaeminst/pace.svg)](https://pkg.go.dev/github.com/jaeminst/pace)
[![Go Report Card](https://goreportcard.com/badge/github.com/jaeminst/pace)](https://goreportcard.com/report/github.com/jaeminst/pace)
[![CI](https://github.com/jaeminst/pace/actions/workflows/test.yml/badge.svg)](https://github.com/jaeminst/pace/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**pace** is a zero-CGO Go library for per-key outbound HTTP rate limiting.

Each key gets an independent token bucket, so one key's traffic never affects another's quota. A single background goroutine handles idle-user garbage collection; the number of goroutines does not grow with the number of active keys.

## Features

- **Per-key isolation** — an independent token bucket per key identity
- **Per-key quotas** — one rate for everyone, or a different one per key; one hook either way
- **Optional cross-replica limiting** — delegate to a backend you supply, with a local shadow bucket that only refuses
- **Configurable bursting** — token-bucket algorithm with an adjustable burst ceiling
- **Pluggable persistence** — a context-aware `store.Store` for any backend, with the contract shipped as an executable test suite
- **Sharded key map** — lock striping across 256 shards, with no store I/O held under a lock
- **Graceful shutdown** — `Shutdown(ctx)` genuinely waits for in-flight requests
- **Observable** — `Stats()` for gauges, `Observer` hooks for metrics and tracing
- **Testable by design** — injectable `Clock` and `http.RoundTripper`

## Install

```sh
go get github.com/jaeminst/pace
```

Requires **Go 1.26.6+**.

## Quick Start

A `Pool` owns the shared machinery and is what you create and close. A `Client` is a lightweight handle bound to one key. Both live in `pace/client`; what you configure lives in `pace/config`, and the rate you write it in comes from `pace/bucket` — see [Package layout](#package-layout).

```go
import (
    "github.com/jaeminst/pace/bucket"
    "github.com/jaeminst/pace/client"
    "github.com/jaeminst/pace/config"
)

pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Quota:   bucket.NewQuota("60/m", 10), // 60 a minute, 10 back-to-back
})
if err != nil {
    log.Fatal(err)
}
defer pool.Close()

alice := pool.Client("alice")
bob := pool.Client("bob")

// Each key has their own quota; alice cannot starve bob.
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
    r.Cancel() // hand the token back; otherwise the key is charged for nothing
    return errTooBusy
}
time.Sleep(r.Delay())
// … now make the call
```

None of `Wait`, `Allow` or `Reserve` sends anything, so a `Client` paces work
pace does not perform on your behalf just as well as a request:

```go
if err := pool.Client("alice").Wait(ctx); err != nil {
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
| `Quota` | `bucket.Quota` | — | **Required.** The rate and burst every key gets — `bucket.NewQuota("60/m", 10)`. Grade keys with the `WithQuotaFor` option; see [Per-key quotas](#per-key-quotas). |
| `IdleExpiry` | `time.Duration` | 10m | How long a key may be inactive before their state is collected. |
| `GCInterval` | `time.Duration` | 1m | How often the idle-user sweep runs. |
| `Shards` | `int` | 256 | Lock-striping width; rounded up to a power of two. |
| `Transport` | `http.RoundTripper` | `http.DefaultTransport` | See [HTTP Connection Configuration](#http-connection-configuration). |
| `RequestTimeout` | `time.Duration` | 0 (none) | Bounds one round-trip. Excludes time spent waiting for a token. |
| `MaxResponseBytes` | `int64` | 0 (unlimited) | Caps the buffered response body. |
| `Clock` | `Clock` | system | Injectable clock, for deterministic tests. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives internal warnings. |
| `Observer` | `*Observer` | nil | Hooks for throttling, requests and evictions. Every hook takes a context. |
| `Store` | `store.Store` | nil | Backend for per-key token state. Without one, a restart starts every key at a full burst. |
| `StoreTimeout` | `time.Duration` | 5s | Bounds each `store.Store` call. |
| `Shared` | `shared.Config` | zero | Cross-replica limiting; see below. Ignored unless `Shared.Quota` is set. |

## Per-key quotas

`Config.Quota` is the rate every key gets. Grading keys into tiers — a free one
and a paying one, or one customer with a negotiated ceiling — is the
`WithQuotaFor` option:

```go
// Replaced whole, never mutated in place: the hook runs on request goroutines,
// so a plain map here against a plain map write below is a race.
var tiers atomic.Pointer[map[string]bucket.Quota]
tiers.Store(&map[string]bucket.Quota{
    "acme-corp": {Rate: bucket.PerMinute(600), Burst: 50},
    "trial-42":  {Rate: bucket.PerMinute(6), Burst: 5},
})

pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Quota:   bucket.NewQuota("60/m", 10),
}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
    if q, ok := (*tiers.Load())[key]; ok {
        return q
    }
    return def
}))
```

The hook is handed `Config.Quota` as `def`, so the two never compete: there is
no precedence rule and no zero field meaning "inherit" — the value being
overridden is right there in the signature. The `Quota` you return is used as
written, zero fields included.

**Values are configuration; behaviour is an option.** `Config` holds only things
`Resolve` can check before a request is served, which is why a rate of zero in
`Config.Quota` is a construction error. What a hook returns cannot be checked
that early: an unusable rate there is clamped to zero — a bucket that never
refills — and logged at warn level naming the key.

The hook is consulted when a key's bucket is created — its first request, or the
first after an eviction — never on the hot path, and never while a shard lock is
held. Keep it to a map lookup regardless; it must not do I/O.

**It must be safe for concurrent use.** It runs on request goroutines, one per
key whose bucket is being created, and on whatever goroutine calls
`ReloadQuotas` — possibly at the same instant. Guard whatever it reads.

## Changing the rate while it runs

`Config.Quota` is fixed once `New` has run. To move a rate while the process is
up, put it behind a `WithQuotaFor` hook, change what that hook reads, then
apply:

```go
tiers.Store(&map[string]bucket.Quota{"trial-42": {Rate: bucket.PerMinute(600), Burst: 50}})
pool.ReloadQuotas()                     // every key in memory
pool.Client("trial-42").ReloadQuota()   // or just this one, in O(1)
```

**Applying is a separate step, deliberately.** The change reaches a key who
has no bucket yet — their first request, or their first after an eviction — at
once. Keys already in memory keep what they have until you reload them, because
applying it means walking the population and that is a maintenance operation
rather than something to do on a request. Reloading keeps tokens already accrued
and clamps anything over the new ceiling.

`ReloadQuotas` walks every shard; `ReloadQuota` touches one key. Neither is
`Evict`, which also **drops the accrued tokens** and writes to the store on the
way out — use it to forget a key, not to re-price one.

Swap what the hook reads and reload from the same goroutine. Racing them can
leave a population permanently split: nothing re-runs the walk, so there is no
eventual convergence, only the order you impose.

One sharp edge worth knowing before you reach for it: moving a key from
`bucket.Inf` back to a finite rate hands them a full burst, because the bucket
credits the elapsed time at the outgoing — infinite — rate. "Unlimit for five
minutes, then restore" is now one line, and that is what it does.

`client.Quota()` reports what a key's bucket is enforcing now, and
`LimitError` and `ThrottleInfo` carry that same per-key quota rather than the
Limiter-wide default.

Persisted state carries no quota. A key restored from a `store.Store` gets
whatever the quota resolves to at that moment, with its saved tokens capped at
the current burst — so a demotion takes effect on the next restore instead of
handing back a ceiling they no longer have.

## Rate limiting across replicas

**Read this paragraph before the code.** Most services that want "distributed
rate limiting" are better off setting each instance's quota to its share of the upstream
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
version and support, and its correctness would live in a Lua script most keys
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
enough that a retry can succeed, keys and namespaces are independent, and the
context is honoured so `QuotaTimeout` means something.

Two consequences worth knowing before you adopt it. `Client.Allow` gains a
bounded backend call, which matters most when it is used as an inbound load
shedder. And with a shared quota configured the local bucket is never persisted
to a `store.Store` — it describes what *this replica* spent, not what the key
spent, so restoring one replica's snapshot into another would be wrong.

[ADR 0004](docs/adr/0004-shared-quota-is-approximate.md) states what is and is
not guaranteed. The short version: accuracy is a property of the backend you
plug in, a partition degrades to N × the local quota by default, there is no fairness,
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

By default pace is in-memory only, and a restart starts every key at a full burst. pace ships no backend — implement two methods against whatever already holds your state:

```go
type Store interface {  // package store
    Save(ctx context.Context, key string, state State) error
    // Returning (State{}, false, nil) when nothing is stored is valid.
    Load(ctx context.Context, key string) (State, bool, error)
}
```

Two methods. If your store also needs tearing down, implement `io.Closer` —
`Close` and `Shutdown` find it by type assertion, the same way `store.BatchStore`
extends this interface. **pace closes what it finds**, so do not hand one store
to two Limiters unless you want the first shutdown to close it for both.

Every call receives a context bounded by `Config.StoreTimeout`, so a network-backed store can honour cancellation rather than block the request path. A store that fails or times out is logged and treated as "no saved state": the key starts from a fresh bucket instead of the request failing.

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

func (r *RedisStore) Save(ctx context.Context, key string, st store.State) error {
    data, err := json.Marshal(st)
    if err != nil {
        return err
    }
    return r.client.Set(ctx, r.prefix+key, data, 24*time.Hour).Err()
}

func (r *RedisStore) Load(ctx context.Context, key string) (store.State, bool, error) {
    data, err := r.client.Get(ctx, r.prefix+key).Bytes()
    if errors.Is(err, redis.Nil) {
        return store.State{}, false, nil
    }
    if err != nil {
        return store.State{}, false, err
    }
    var st store.State
    return st, true, json.Unmarshal(data, &st)
}


pool, _ := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Quota:   bucket.NewQuota("60/m", 10),
    Store:   &RedisStore{client: redisClient, prefix: "pace:"},
})
```

The idle-user sweep can evict thousands of keys at once. If a round-trip each would hurt, implement the optional `store.BatchStore` too and pace will hand you whole batches:

```go
func (r *RedisStore) SaveBatch(ctx context.Context, states []store.UserState) error { ... }
```

## Observability

`Stats()` is a cheap snapshot, suitable for a scrape interval:

```go
s := pool.Stats()
// s.Keys, s.Requests, s.Throttled, s.Wait, s.Errors, s.Evictions
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
pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Quota:   bucket.NewQuota("60/m", 10),
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

pool, err := client.New(config.Config{
    BaseURL: "https://internal.example.com",
    Quota:   bucket.NewQuota("60/m", 10),
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
if err := pool.Shutdown(ctx); err != nil {
    log.Printf("shutdown forced: %v", err)
}
```

`Close()` does not wait: it cancels in-flight work, then flushes. Use `Shutdown` when you want requests to finish.

## Package layout

pace is three packages with one job each, and the repository root declares
nothing — `import "github.com/jaeminst/pace"` resolves to a documentation page
telling you which of the three you want.

- **`pace/config`** — everything you configure: `Config`, `Clock`, `Error`.
- **`pace/bucket`** — the token bucket, and the vocabulary for describing one:
  `Quota`, `Limit`, `PerMinute`, `Inf`. You write a rate in these and a limiter
  reports one back in the same types.
- **`pace/limiter`** — the rate limiter and only that. It does not import
  `net/http`.
- **`pace/client`** — creating and managing clients, and the request path. A
  `Pool` owns a limiter and mints a `Client` per key.

So you import three packages for ordinary use — `client` to make requests,
`config` to configure, `bucket` to write a rate — and a fourth if you match a
`*limiter.LimitError`. In exchange **every name is declared exactly once**: Go
renders a type alias as a single line with no methods, so a convenience
re-export documents nothing and sends you one package over anyway. See
[ADR 0009](docs/adr/0009-config-limiter-client.md).

Everything else you supply to a Limiter, or that it reports back, is a package
of its own — so a contract is documented where it is implemented rather than as
one line in a list of configuration fields.

| Package | What is in it |
|---|---|
| [`pace`](https://pkg.go.dev/github.com/jaeminst/pace) | documentation only — no declarations |
| [`pace/config`](https://pkg.go.dev/github.com/jaeminst/pace/config) | `Config`, its validation and its defaults |
| [`pace/bucket`](https://pkg.go.dev/github.com/jaeminst/pace/bucket) | the token bucket, and the rate vocabulary — `Quota`, `Limit`, `PerMinute` and friends |
| [`pace/client`](https://pkg.go.dev/github.com/jaeminst/pace/client) | `New`, `Pool`, `Client`, `Request`, `Response` — the request path |
| [`pace/limiter`](https://pkg.go.dev/github.com/jaeminst/pace/limiter) | the rate limiter: `Limiter`, `Spec`, `LimitError`, `Reservation` |
| [`pace/store`](https://pkg.go.dev/github.com/jaeminst/pace/store) | `Store` — the persistence contract you implement |
| [`pace/shared`](https://pkg.go.dev/github.com/jaeminst/pace/shared) | `Backend` — the cross-replica token supply you implement |
| [`pace/shared/quotatest`](https://pkg.go.dev/github.com/jaeminst/pace/shared/quotatest) | the conformance suite for the above |
| [`pace/observe`](https://pkg.go.dev/github.com/jaeminst/pace/observe) | `Observer`, `Stats` and the event structs |
| [`pace/transport`](https://pkg.go.dev/github.com/jaeminst/pace/transport) | HTTP connection tuning |

Below those sit the pieces pace is built from — the engine mostly, though `urlx`
is the request path's. They are public because they are worth reading, not
because you are expected to assemble one; `client.New` does that. Their `Spec`
is a required-everything vtable that panics on a field left out. `config.Config`
is the opposite, and it is the only configuration you fill in.

| Package | What is in it |
|---|---|
| [`pace/registry`](https://pkg.go.dev/github.com/jaeminst/pace/registry) | the sharded key population, its GC sweep and state flush |
| [`pace/store/memory`](https://pkg.go.dev/github.com/jaeminst/pace/store/memory) | an in-memory `store.Store` — a reference implementation and a test double |
| [`pace/store/storetest`](https://pkg.go.dev/github.com/jaeminst/pace/store/storetest) | the persistence contract as a test suite — run your backend against it |
| [`pace/gate`](https://pkg.go.dev/github.com/jaeminst/pace/gate) | the shared-quota decision: shadow bucket, backend call, failure policy |
| [`pace/breaker`](https://pkg.go.dev/github.com/jaeminst/pace/breaker) | the shared-quota circuit breaker |
| [`pace/urlx`](https://pkg.go.dev/github.com/jaeminst/pace/urlx) | request URL construction |

No name in pace is an alias, because no name is published twice — each is
declared in the package whose job it is. There is one configuration type,
`config.Config`, and both `client.New` and `limiter.New` take it directly —
there is no second struct restating it.

## How It Works

1. **Token bucket** — each key has a `golang.org/x/time/rate.Limiter`. Tokens refill at their `Quota.Rate` per second, up to `Quota.Burst`.
2. **Sharded map** — key entries live in one of `Shards` stripes (FNV-1a hash). A hit takes a read lock; creating a key takes a write lock on one shard.
3. **GC sweep** — a background goroutine wakes every `GCInterval` and collects keys idle longer than `IdleExpiry`. It snapshots under the lock, persists with no lock held, then deletes only what has not been touched since — so a slow store never blocks live traffic.
4. **Persistence** — token counts are saved on eviction and on close, and restored exactly, fractions included, accounting for time elapsed since.

## Benchmarks

Numbers are machine-specific; regenerate your own with `make bench`. The recorded baseline and what it does and does not prove are in [`docs/bench/`](docs/bench/README.md).

What is worth knowing about the shape of the costs:

- The end-to-end benchmarks are dominated by the loopback HTTP round-trip. `BenchmarkRequest_NoHTTP` stubs the network out and is the honest measure of pace's own work — roughly 2.3µs and 22 allocations per request.
- Shard lookup is about 22ns for a 32-byte key, with no allocation.
- A full sweep of 2,000 idle keys takes ~80µs with no store and ~760µs through an in-memory one, none of it holding a shard lock. That last figure was 4.6 **seconds** before the sweep stopped holding locks across the store.

## Testing

pace exposes an injectable `Clock` and accepts a custom `http.RoundTripper`, so tests need neither real time nor a real network:

```go
pool, _ := client.New(config.Config{
    BaseURL:    "http://example.invalid",
    Quota:      bucket.NewQuota("60/m", 10),
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
