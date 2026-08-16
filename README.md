# pace

[![Go Reference](https://pkg.go.dev/badge/github.com/jaeminst/pace.svg)](https://pkg.go.dev/github.com/jaeminst/pace)
[![Go Report Card](https://goreportcard.com/badge/github.com/jaeminst/pace)](https://goreportcard.com/report/github.com/jaeminst/pace)
[![CI](https://github.com/jaeminst/pace/actions/workflows/test.yml/badge.svg)](https://github.com/jaeminst/pace/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**pace** is a zero-CGO Go library for per-user outbound HTTP rate limiting.

Each user gets an independent token bucket, so one user's traffic never affects another's quota. A single background goroutine handles idle-user garbage collection; the number of goroutines does not grow with the number of active users.

## Features

- **Per-user isolation** — an independent token bucket per user identity
- **Configurable bursting** — token-bucket algorithm with an adjustable burst ceiling
- **Pluggable persistence** — a context-aware `StateStore` for any backend, with SQLite built in
- **Durable request queue** — at-least-once delivery that survives restarts, with retries, backoff, and a dead-letter table
- **Sharded user map** — lock striping across 256 shards, with no store I/O held under a lock
- **Graceful shutdown** — `Shutdown(ctx)` genuinely waits for in-flight requests
- **Observable** — `Stats()` for gauges, `Observer` hooks for metrics and tracing
- **Testable by design** — injectable `Clock` and `http.RoundTripper`

## Install

```sh
go get github.com/jaeminst/pace
```

Requires **Go 1.25+**.

## Quick Start

A `Limiter` owns the shared machinery and is what you create and close. A `Client` is a lightweight handle bound to one user.

```go
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    pace.PerMinute(60),
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
if !alice.Allow() {
    return errTooBusy // shed load rather than queue behind it
}

if err := alice.Wait(ctx); err != nil {
    return err // ctx expired, or the Limiter closed
}
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `BaseURL` | `string` | — | **Required.** Absolute `http`/`https` URL prepended to every path. |
| `Rate` | `Limit` | — | **Required (> 0).** Build with `PerSecond`, `PerMinute`, `PerHour`, `Every`, or `Inf`. |
| `Burst` | `int` | 1 | Tokens that may accumulate while a user is idle. |
| `IdleExpiry` | `time.Duration` | 10m | How long a user may be inactive before their state is collected. |
| `GCInterval` | `time.Duration` | 1m | How often the idle-user sweep runs. |
| `Shards` | `int` | 256 | Lock-striping width; rounded up to a power of two. |
| `Transport` | `http.RoundTripper` | `http.DefaultTransport` | See [HTTP Connection Configuration](#http-connection-configuration). |
| `RequestTimeout` | `time.Duration` | 0 (none) | Bounds one round-trip. Excludes time spent waiting for a token. |
| `MaxResponseBytes` | `int64` | 0 (unlimited) | Caps the buffered response body. |
| `Clock` | `Clock` | system | Injectable clock, for deterministic tests. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives internal warnings. |
| `Observer` | `*Observer` | nil | Hooks for throttling, requests, evictions, job transitions. |
| `DBPath` | `string` | "" | SQLite file for persistence and the durable queue. |
| `Store` | `StateStore` | nil | Custom persistence backend. Mutually exclusive with `DBPath`. |
| `StoreTimeout` | `time.Duration` | 5s | Bounds each `StateStore` call. |
| `Queue` | `QueueConfig` | zero | The durable queue's knobs; see below. Ignored unless `DBPath` is set. |

### `Config.Queue`

Every field here is ignored unless `DBPath` is set, since that is what creates
the queue.

| Field | Type | Default | Description |
|---|---|---|---|
| `IdempotencyHeader` | `string` | `Idempotency-Key` | Sent on durable requests. `"-"` disables it. |
| `AmbiguousPolicy` | `AmbiguousPolicy` | `AmbiguousAuto` | Fate of a durable job whose outcome is unknown. |
| `Retry` | `RetryPolicy` | 5 attempts, 500ms base | Backoff for durable jobs. |
| `RetryOn` | `func(*Response) bool` | nil | Which responses are worth repeating. |
| `ResultTTL` | `time.Duration` | 24h | How long a durable job's cached response is kept. |
| `Workers` | `int` | 4 | Concurrent background retries. |
| `PollInterval` | `time.Duration` | 1s | How often the retry poller looks for due jobs. |
| `JobLease` | `time.Duration` | 5m | How long a claimed durable job stays owned. |
| `OnDeadLetter` | `func(DeadJob)` | nil | Called when a durable job is abandoned. |

## Errors

```go
resp, err := alice.Get(ctx, "/v1/items/42")

var le *pace.LimitError
switch {
case errors.Is(err, pace.ErrClosed):
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

## Durable Request Queue

`Durable` executes a rate-limited HTTP request that survives a restart, identified by a caller-supplied string ID. It records the job in SQLite before executing, caches the result afterwards, and returns that cached result on any later call with the same ID.

Requires `Config.DBPath`.

```go
lim, err := pace.New(pace.Config{
    BaseURL: "https://payments.example.com",
    Rate:    pace.PerMinute(60),
    Burst:   10,
    DBPath:  "/var/lib/pace/state.db",
})
if err != nil {
    log.Fatal(err)
}
defer lim.Close()

req, err := lim.Client("user-123").Durable(chargeID)
if err != nil {
    log.Fatal(err)
}

// First call: records the job, sends the rate-limited request, caches the result.
// The job ID travels as Idempotency-Key automatically.
resp, err := req.SetBody(chargePayload).Post(ctx, "/v1/charge")

// Second call with the same ID: returns the cached response, no new request.
req2, _ := lim.Client("user-123").Durable(chargeID)
resp, err = req2.Post(ctx, "/v1/charge")
```

### What this actually guarantees

**Delivery is at-least-once, not exactly-once.** Exactly-once delivery over HTTP is not achievable by the client alone: once bytes leave the process, a crash before the response is recorded leaves no way to know whether the server acted. Any library claiming otherwise is claiming something the network cannot provide.

What pace does is make that window small, visible, and yours to decide about:

| Property | Behaviour |
|---|---|
| **Result caching** | A job whose response *was* recorded is never sent again; every later call with that ID returns the cached response. |
| **Never-dispatched jobs replay** | A job recorded but not yet sent is replayed on the next start. This case is unambiguous. |
| **Ambiguous jobs are classified, not guessed** | A job dispatched but never recorded is reported as such and handled per `Config.Queue.AmbiguousPolicy`, rather than blindly re-sent. |
| **Exclusive send** | Claiming a job is a single conditional `UPDATE`, so two workers — including two processes sharing the database — cannot both send it. |
| **In-process deduplication** | Concurrent `Durable` calls with the same ID share one execution and one result. |
| **Bounded retries** | Delivery failures are retried with exponential backoff and full jitter, then dead-lettered. |

### Closing the ambiguous window

Set an idempotency key and let the server collapse duplicates. pace does this by default: every durable request carries `Idempotency-Key: <job id>`, configurable via `Config.Queue.IdempotencyHeader` (use `"-"` to disable).

**Against an endpoint that honours that key, delivery is effectively exactly-once.** That is the strongest honest statement available, and it depends on the server, not on pace.

When the server does not cooperate, `Config.Queue.AmbiguousPolicy` decides what happens to a job whose outcome is unknown:

| Policy | Behaviour |
|---|---|
| `AmbiguousAuto` (default) | Retry when repeating is safe — an idempotent method, or any method when an idempotency header is configured. Park anything else. |
| `AmbiguousRetry` | Always retry. Choose it when a duplicate is cheaper than a drop. |
| `AmbiguousPark` | Never retry. Choose it when a duplicate is worse than a drop — charging a card, sending a message. |

Parked and exhausted jobs go to a dead-letter table, reported through `Config.Queue.OnDeadLetter` and readable afterwards:

```go
cfg.Queue.OnDeadLetter = func(j pace.DeadJob) {
    log.Printf("abandoned %s %s for %s after %d attempts: %s",
        j.Method, j.Path, j.UserID, j.Attempts, j.Reason)
}

// After a restart, see what was abandoned while you were away.
dead, err := lim.DeadJobs(ctx, 0)
```

### Retries

The queue retries **delivery failures** — a request that did not reach the server. A response of any status means delivery succeeded, which is what the queue promises, so it is not retried unless you say so:

```go
cfg.Queue.RetryOn = func(r *pace.Response) bool {
    return r.StatusCode() == http.StatusTooManyRequests || r.StatusCode() >= 500
}
```

pace does not interpret status codes anywhere else, and does not start here. Your API knows which of its own responses are transient.

## Pluggable Persistence (`StateStore`)

By default pace is in-memory only. Use `Config.DBPath` for the built-in SQLite backend, or implement `StateStore` for any other:

```go
type StateStore interface {
    Save(ctx context.Context, userID string, state State) error
    // Returning (State{}, false, nil) when nothing is stored is valid.
    Load(ctx context.Context, userID string) (State, bool, error)
    Close() error
}
```

Every call receives a context bounded by `Config.StoreTimeout`, so a network-backed store can honour cancellation rather than block the request path. A store that fails or times out is logged and treated as "no saved state": the user starts from a fresh bucket instead of the request failing.

**Example — Redis backend:**

```go
type RedisStore struct {
    client *redis.Client
    prefix string
}

func (r *RedisStore) Save(ctx context.Context, userID string, st pace.State) error {
    data, err := json.Marshal(st)
    if err != nil {
        return err
    }
    return r.client.Set(ctx, r.prefix+userID, data, 24*time.Hour).Err()
}

func (r *RedisStore) Load(ctx context.Context, userID string) (pace.State, bool, error) {
    data, err := r.client.Get(ctx, r.prefix+userID).Bytes()
    if errors.Is(err, redis.Nil) {
        return pace.State{}, false, nil
    }
    if err != nil {
        return pace.State{}, false, err
    }
    var st pace.State
    return st, true, json.Unmarshal(data, &st)
}

func (r *RedisStore) Close() error { return nil }

lim, _ := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    pace.PerMinute(60),
    Store:   &RedisStore{client: redisClient, prefix: "pace:"},
})
```

The idle-user sweep can evict thousands of users at once. If a round-trip each would hurt, implement the optional `BatchStateStore` too and pace will hand you whole batches:

```go
func (r *RedisStore) SaveBatch(ctx context.Context, states []pace.UserState) error { ... }
```

## Observability

`Stats()` is a cheap snapshot, suitable for a scrape interval:

```go
s := lim.Stats()
// s.Users, s.Requests, s.Throttled, s.Wait, s.Errors, s.Evictions
```

`Observer` pushes events as they happen. It is a struct of optional functions rather than an interface, so new events can be added without breaking your code:

```go
cfg.Observer = &pace.Observer{
    Throttled: func(_ context.Context, i pace.ThrottleInfo) {
        throttleDelay.WithLabelValues(i.UserID).Observe(i.Delay.Seconds())
    },
    RequestFinished: func(_ context.Context, i pace.RequestInfo) {
        latency.WithLabelValues(i.Method, strconv.Itoa(i.Status)).Observe(i.Latency.Seconds())
    },
}
```

Hooks run on the caller's goroutine, in the request path. Keep them to a counter increment.

## HTTP Connection Configuration

By default pace uses `http.DefaultTransport`. Use `NewTransport` to tune connection behaviour before passing it to `Config.Transport`:

```go
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    pace.PerMinute(60),
    Transport: pace.NewTransport(pace.TransportConfig{
        DialTimeout:           5 * time.Second,  // TCP connection timeout
        TLSHandshakeTimeout:   3 * time.Second,  // TLS handshake timeout
        ResponseHeaderTimeout: 10 * time.Second, // wait for the response headers
        KeepAlive:             30 * time.Second, // TCP keep-alive probe interval
        MaxIdleConnsPerHost:   10,               // idle connections per host
    }),
})
```

A zero `TransportConfig` behaves like `http.DefaultTransport`, not like a bare `http.Transport`. That distinction matters in two places: the environment proxy is honoured unless you replace it, and HTTP/2 is attempted even when you supply a `TLSConfig`.

### `TransportConfig` fields

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
    Rate:    pace.PerMinute(60),
    Transport: pace.NewTransport(pace.TransportConfig{
        TLSHandshakeTimeout: 5 * time.Second,
        TLSConfig: &tls.Config{
            Certificates: []tls.Certificate{cert},
            RootCAs:      pool,
            MinVersion:   tls.VersionTLS12,
        },
    }),
})
```

net/http disables automatic HTTP/2 as soon as a transport carries a custom `TLSClientConfig`. `NewTransport` sets `ForceAttemptHTTP2` so this configuration still negotiates HTTP/2; pass `DisableHTTP2: true` if you want HTTP/1.1.

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

## How It Works

1. **Token bucket** — each user has a `golang.org/x/time/rate.Limiter`. Tokens refill at `Rate` per second, up to `Burst`.
2. **Sharded map** — user entries live in one of `Shards` stripes (FNV-1a hash). A hit takes a read lock; creating a user takes a write lock on one shard.
3. **GC sweep** — a background goroutine wakes every `GCInterval` and collects users idle longer than `IdleExpiry`. It snapshots under the lock, persists with no lock held, then deletes only what has not been touched since — so a slow store never blocks live traffic.
4. **Persistence** — token counts are saved on eviction and on close, and restored exactly, fractions included, accounting for time elapsed since.

## Benchmarks

Numbers are machine-specific; regenerate your own with `make bench`. A recorded baseline lives in `docs/bench/`.

What is worth knowing about the shape of the costs:

- The end-to-end benchmarks are dominated by the loopback HTTP round-trip. `BenchmarkRequest_NoHTTP` stubs the network out and is the honest measure of pace's own work — roughly 1.8µs and 21 allocations per request.
- Shard lookup is about 20ns for a 32-byte user ID, with no allocation.
- With SQLite persistence, a full sweep of 2,000 idle users takes ~12ms, none of it holding a shard lock.

## Testing

pace exposes an injectable `Clock` and accepts a custom `http.RoundTripper`, so tests need neither real time nor a real network:

```go
lim, _ := pace.New(pace.Config{
    BaseURL:    "http://example.invalid",
    Rate:       pace.PerMinute(60),
    Clock:      myFakeClock,   // drive idle expiry and token refill directly
    Transport:  myStubTransport,
    GCInterval: time.Millisecond,
})
```

Freeze the clock when asserting on token counts: against a live one the bucket refills between readings, and "did this spend a token" stops being an exact question.

## Caveats

- **Rate limiting is per process** — the in-memory sharded map is not distributed, so multiple instances each enforce their own limit. A shared `StateStore` carries state across restarts, not across concurrent processes.
- **The durable queue is multi-process safe** — jobs are claimed with a conditional `UPDATE`, so two processes sharing one database file will not send the same request twice.
- **SQLite specifics** — the database runs in WAL mode, which keeps `-wal` and `-shm` files beside it and is unsafe on a network filesystem. Point `DBPath` at local storage.
- **Delivery is at-least-once** — see [What this actually guarantees](#what-this-actually-guarantees).

## Migrating from v0.1.0

See [MIGRATION.md](MIGRATION.md).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) © pace contributors
