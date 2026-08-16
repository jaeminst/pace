# pace

[![Go Reference](https://pkg.go.dev/badge/github.com/jaeminst/pace.svg)](https://pkg.go.dev/github.com/jaeminst/pace)
[![Go Report Card](https://goreportcard.com/badge/github.com/jaeminst/pace)](https://goreportcard.com/report/github.com/jaeminst/pace)
[![CI](https://github.com/jaeminst/pace/actions/workflows/test.yml/badge.svg)](https://github.com/jaeminst/pace/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**pace** is a zero-CGO Go library for per-user outbound HTTP rate limiting.

Each user gets an independent token bucket — one user's traffic never affects another's quota. A single background goroutine handles idle-user garbage collection; the number of goroutines does not grow with the number of active users.

## Features

- **Per-user isolation** — independent token bucket per user identity
- **Configurable bursting** — token-bucket algorithm with adjustable burst ceiling
- **Pluggable persistence** — optional `StateStore` interface for any backend (built-in SQLite, or bring your own Redis/Postgres)
- **Sharded user map** — 256 FNV-32a shards minimise lock contention under high concurrency
- **Goroutine-efficient** — `context.AfterFunc` eliminates per-request goroutine allocation in the hot path
- **Graceful shutdown** — `Shutdown(ctx)` drains in-flight requests before closing
- **Testable by design** — injectable `Clock` and `http.RoundTripper` for deterministic unit tests

## Install

```sh
go get github.com/jaeminst/pace
```

Requires **Go 1.25.7+**.

## Quick Start

```go
// Bind a user identity at creation time via Config.Name.
alice, err := pace.New(pace.Config{
    Name:          "alice",
    BaseURL:       "https://api.example.com",
    RatePerMinute: 60,
    Burst:         10,
})
if err != nil {
    log.Fatal(err)
}
defer alice.Close()

// One-liner GET (token consumed + HTTP round-trip).
resp, err := alice.Get(ctx, "/v1/items/42")

// Chainable builder for more control.
req, err := alice.Request(ctx)
resp, err = req.
    SetHeader("Authorization", "Bearer "+token).
    SetBody(payload).
    Post("/v1/orders")
```

### Sharing one rate-limiter across users

Omit `Config.Name` and use `For` to derive per-user clients that share the same underlying limiter and configuration:

```go
client, err := pace.New(pace.Config{
    BaseURL:       "https://api.example.com",
    RatePerMinute: 60,
    Burst:         10,
})
if err != nil {
    log.Fatal(err)
}
defer client.Close()

resp, err := client.For("alice").Get(ctx, "/v1/items/42")
resp, err  = client.For("bob").Get(ctx, "/v1/items/99")
```

`For` is lightweight (allocates only the thin `*Client` wrapper) and safe for concurrent use.

## Configuration

### `Config`

| Field | Type | Default | Description |
|---|---|---|---|
| `Name` | `string` | "" | User identity bound to this Client. Optional — omit when you prefer `For(userID)`. |
| `BaseURL` | `string` | — | **Required.** Base URL prepended to every request path. |
| `RatePerMinute` | `int` | — | **Required (> 0).** Maximum requests per user per minute. |
| `Burst` | `int` | 1 | Maximum token accumulation when idle. |
| `IdleExpiry` | `time.Duration` | 10m | How long a user can be inactive before in-memory state is GC'd. |
| `GCInterval` | `time.Duration` | 1m | How often the GC sweep runs. |
| `Transport` | `http.RoundTripper` | `http.DefaultTransport` | HTTP transport for all requests. |
| `Clock` | `Clock` | system clock | Override for deterministic testing. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives internal warnings. |
| `DBPath` | `string` | "" (disabled) | Path to SQLite file for state persistence. Mutually exclusive with `Store`. |
| `Store` | `StateStore` | nil (disabled) | Custom persistence backend. Mutually exclusive with `DBPath`. |
| `OnThrottle` | `func(userID string)` | nil | Called when a request must wait for a token. |

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
// The job ID is sent as Idempotency-Key automatically.
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
| **Ambiguous jobs are classified, not guessed** | A job dispatched but never recorded is reported as such and handled per `Config.AmbiguousPolicy`, rather than blindly re-sent. |
| **Exclusive send** | Claiming a job is a single conditional `UPDATE`, so two workers — including two processes sharing the database — cannot both send it. |
| **In-process deduplication** | Concurrent `Durable` calls with the same ID share one execution and one result. |

### Closing the ambiguous window

Set an idempotency key and let the server collapse duplicates. pace does this by default: every durable request carries `Idempotency-Key: <job id>`, configurable via `Config.IdempotencyHeader` (use `"-"` to disable).

**Against an endpoint that honours that key, delivery is effectively exactly-once.** That is the strongest honest statement available, and it depends on the server, not on pace.

When the server does not cooperate, `Config.AmbiguousPolicy` decides what happens to a job whose outcome is unknown:

| Policy | Behaviour |
|---|---|
| `AmbiguousAuto` (default) | Retry when repeating is safe — an idempotent method, or any method when an idempotency header is configured. Park anything else. |
| `AmbiguousRetry` | Always retry. Choose it when a duplicate is cheaper than a drop. |
| `AmbiguousPark` | Never retry. Choose it when a duplicate is worse than a drop — charging a card, sending a message. |

Parked jobs go to a dead-letter table and are reported through `Config.OnDeadLetter`:

```go
cfg.OnDeadLetter = func(j pace.DeadJob) {
    log.Printf("abandoned %s %s for %s after %d attempts: %s",
        j.Method, j.Path, j.UserID, j.Attempts, j.Reason)
}
```

## Pluggable Persistence (`StateStore`)

By default pace is in-memory only. Use `Config.DBPath` for the built-in SQLite backend, or implement `StateStore` for any other backend:

```go
type StateStore interface {
    Save(userID string, state SavedState) error
    // Returning (zero, false, nil) when no prior state exists is valid.
    Load(userID string) (SavedState, bool, error)
    Close() error
}
```

**Example — Redis backend:**

```go
type RedisStore struct{ client *redis.Client; prefix string }

func (r *RedisStore) Save(userID string, state pace.SavedState) error {
    data, _ := json.Marshal(state)
    return r.client.Set(ctx, r.prefix+userID, data, 24*time.Hour).Err()
}

func (r *RedisStore) Load(userID string) (pace.SavedState, bool, error) {
    data, err := r.client.Get(ctx, r.prefix+userID).Bytes()
    if errors.Is(err, redis.Nil) {
        return pace.SavedState{}, false, nil
    }
    if err != nil {
        return pace.SavedState{}, false, err
    }
    var state pace.SavedState
    return state, true, json.Unmarshal(data, &state)
}

func (r *RedisStore) Close() error { return nil }

// Usage:
client, _ := pace.New(pace.Config{
    BaseURL:       "https://api.example.com",
    RatePerMinute: 60,
    Store:         &RedisStore{client: redisClient, prefix: "pace:"},
})
```

## HTTP Connection Configuration

By default pace uses `http.DefaultTransport`. Use `NewTransport` to tune connection behaviour before passing it to `Config.Transport`:

```go
client, err := pace.New(pace.Config{
    BaseURL:       "https://api.example.com",
    RatePerMinute: 60,
    Transport: pace.NewTransport(pace.TransportConfig{
        DialTimeout:           5 * time.Second,  // TCP connection timeout
        TLSHandshakeTimeout:   3 * time.Second,  // TLS handshake timeout
        ResponseHeaderTimeout: 10 * time.Second, // wait for first response byte
        KeepAlive:             30 * time.Second, // TCP keep-alive probe interval
        MaxIdleConns:          100,              // total idle connections
        MaxIdleConnsPerHost:   10,               // idle connections per host
        IdleConnTimeout:       90 * time.Second, // how long to keep idle connections
    }),
})
```

### `TransportConfig` fields

| Field | Default | Description |
|---|---|---|
| `DialTimeout` | 30s | Maximum time to establish a TCP connection. |
| `KeepAlive` | 30s | Interval between TCP keep-alive probes. Set to `-1` to disable. |
| `TLSHandshakeTimeout` | 10s | Maximum time to complete a TLS handshake. |
| `ResponseHeaderTimeout` | 0 (disabled) | Maximum time to wait for response headers after the request is sent. |
| `MaxIdleConns` | 100 | Maximum idle (keep-alive) connections across all hosts. |
| `MaxIdleConnsPerHost` | 2 | Maximum idle connections kept per host. |
| `IdleConnTimeout` | 90s | How long an idle connection stays open before being closed. |
| `TLSConfig` | nil | Custom `*tls.Config` (e.g. client certificates, custom CA). |

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

client, err := pace.New(pace.Config{
    BaseURL: "https://internal.example.com",
    Transport: pace.NewTransport(pace.TransportConfig{
        TLSHandshakeTimeout: 5 * time.Second,
        TLSConfig: &tls.Config{
            Certificates: []tls.Certificate{cert},
            RootCAs:      pool,
        },
    }),
})
```

## Graceful Shutdown

`Shutdown(ctx)` prevents new requests and waits for in-flight `Wait` calls to complete before flushing and closing the store. If `ctx` expires first, remaining waiters are force-cancelled.

```go
// On SIGTERM: give 5 seconds for in-flight requests to finish.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := client.Shutdown(ctx); err != nil {
    log.Printf("shutdown forced: %v", err)
}
```

`Close()` cancels immediately (no grace period). Use `Shutdown` when you need to drain requests cleanly.

## How It Works

1. **Token bucket** — each user has a `golang.org/x/time/rate.Limiter`. Tokens refill at `RatePerMinute/60` per second up to `Burst`.
2. **Sharded map** — user entries live in one of 256 shards (FNV-32a hash). Read-heavy workloads acquire only a read lock; new-user creation takes a write lock on a single shard.
3. **GC sweep** — a background goroutine wakes every `GCInterval` and evicts users whose `lastUsed` timestamp is older than `IdleExpiry`. Evicted state is flushed to the store if configured.
4. **Persistence** — on eviction (and on `Close`/`Shutdown`), the current token count and timestamp are saved. On next access, `RestoreBucket` re-creates the limiter accounting for elapsed time.

## Benchmarks

Measured on `linux/amd64` with an Intel Xeon @ 2.80 GHz (`go test -bench=. -benchmem -count=3 -benchtime=2s`):

| Benchmark | ops/s | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| `Request_HotPath` (existing user) | ~9 000 | ~112 000 | 5 578 | 63 |
| `Request_NewUser` (cold path) | ~9 000 | ~113 000 | 6 047 | 70 |
| `ConcurrentUsers_256` (256 goroutines) | ~22 000 | ~57 000 | 16 557 | 114 |

> The hot-path cost is dominated by the actual HTTP round-trip to a local httptest server. The token-bucket and shard-lock overhead is sub-microsecond.

## Testing

pace exposes an injectable `Clock` interface and accepts a custom `http.RoundTripper`, making it straightforward to write fast, deterministic tests:

```go
type fakeClock struct{ now time.Time }
func (c *fakeClock) Now() time.Time { return c.now }

client, _ := pace.New(pace.Config{
    BaseURL:       "http://x",
    RatePerMinute: 1,
    Clock:         &fakeClock{now: time.Unix(0, 0)},
    Transport:     &mockTransport{},
    GCInterval:    time.Millisecond,
    IdleExpiry:    time.Second,
})
```

The `export_test.go` file (package `pace`) exposes `collectIdle` via `pace.CollectIdle` so white-box tests can trigger GC sweeps without waiting for the ticker.

## Caveats

- **Single process** — the in-memory sharded map is not distributed. For multi-instance deployments implement `StateStore` with a shared backend (Redis, etc.).
- **SQLite write serialisation** — all SQLite writes share a single `*sql.DB`. This is fine for GC eviction and shutdown; it is not designed for high-frequency per-request persistence.
- **No retry logic** — pace enforces rate limits by blocking; it does not retry failed HTTP requests.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) © pace contributors
