# pace

[![Go Reference](https://pkg.go.dev/badge/github.com/jaeminst/pace.svg)](https://pkg.go.dev/github.com/jaeminst/pace)
[![Go Report Card](https://goreportcard.com/badge/github.com/jaeminst/pace)](https://goreportcard.com/report/github.com/jaeminst/pace)
[![CI](https://github.com/jaeminst/pace/actions/workflows/test.yml/badge.svg)](https://github.com/jaeminst/pace/actions/workflows/test.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

**pace** is a zero-CGO Go library for per-user, per-endpoint outbound HTTP rate limiting.

Each user gets an independent token bucket per endpoint — one user's traffic never affects another's quota. A single background goroutine handles idle-user garbage collection; the number of goroutines does not grow with the number of active users.

## Features

- **Per-user isolation** — independent token buckets per `(user, endpoint)` pair
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
mgr, err := pace.New(pace.Config{
    Endpoints: map[string]pace.Endpoint{
        "payments": {
            BaseURL:       "https://payments.example.com",
            RatePerMinute: 60,
            Burst:         10,
        },
        "notifications": {
            BaseURL:       "https://notify.example.com",
            RatePerMinute: 600,
            Burst:         50,
        },
    },
})
if err != nil {
    log.Fatal(err)
}
defer mgr.Close()

// One-liner GET (token consumed + HTTP round-trip)
resp, err := mgr.Get(ctx, "user-123", "payments", "/v1/charge")

// Chainable builder for more control
resp, err = mgr.Request(ctx, "user-123", "notifications", "api").
    SetHeader("Authorization", "Bearer "+token).
    SetBody(payload).
    Post("/v1/send")
```

## Configuration

### `Config`

| Field | Type | Default | Description |
|---|---|---|---|
| `Endpoints` | `map[string]Endpoint` | — | **Required.** Named endpoint map. |
| `IdleExpiry` | `time.Duration` | 10m | How long a user can be inactive before in-memory state is GC'd. |
| `GCInterval` | `time.Duration` | 1m | How often the GC sweep runs. |
| `Transport` | `http.RoundTripper` | `http.DefaultTransport` | HTTP transport for all requests. |
| `Clock` | `Clock` | system clock | Override for deterministic testing. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives internal warnings. |
| `DBPath` | `string` | "" (disabled) | Path to SQLite file for state persistence. Mutually exclusive with `Store`. |
| `Store` | `StateStore` | nil (disabled) | Custom persistence backend. Mutually exclusive with `DBPath`. |
| `OnThrottle` | `func(userID, endpoint string)` | nil | Called when a request must wait for a token. |

### `Endpoint`

| Field | Type | Description |
|---|---|---|
| `BaseURL` | `string` | **Required.** Base URL prepended to every request path. |
| `RatePerMinute` | `int` | **Required (> 0).** Maximum requests per user per minute. |
| `Burst` | `int` | Maximum token accumulation when idle. Defaults to 1. |

## Durable Request Queue (`Once`)

`Once` executes a rate-limited HTTP request with **exactly-once semantics**, identified by a caller-supplied string ID. It persists the job to SQLite before executing and caches the result afterwards. Restarting the process automatically replays any jobs that were in-flight when the process exited.

Requires `Config.DBPath`.

```go
spec := pace.RequestSpec{
    UserID:   "user-123",
    Endpoint: "payments",
    Method:   "POST",
    Path:     "/v1/charge",
    Headers:  map[string]string{"Idempotency-Key": chargeID},
    Body:     chargePayload,
}

// First call: enqueues to SQLite, executes rate-limited HTTP, caches result.
resp, err := mgr.Once(ctx, chargeID, spec)

// Second call (same id): returns the cached response without a new HTTP call.
resp, err = mgr.Once(ctx, chargeID, spec)

// On process restart: the new Manager replays any pending jobs automatically.
```

### Guarantees

| Property | Behaviour |
|---|---|
| **Exactly-once (success)** | A job that received an HTTP response is never retried; the cached response is returned on every subsequent call. |
| **At-least-once (failure)** | A job whose HTTP call returned a network error stays pending and is replayed on the next restart. |
| **In-process deduplication** | Concurrent `Once` calls with the same ID share a single in-flight execution (singleflight). |

### Types

```go
// RequestSpec describes the HTTP request to be executed and persisted.
type RequestSpec struct {
    UserID   string            // rate-limit identity key; required by Once
    Endpoint string            // named endpoint from Config.Endpoints; required by Once
    Method   string            // HTTP method; defaults to "GET"
    Path     string            // appended to endpoint BaseURL
    Headers  map[string]string // outbound request headers
    Body     []byte            // request body; may be nil
}
```

## Pluggable Persistence (`StateStore`)

By default pace is in-memory only. Use `Config.DBPath` for the built-in SQLite backend, or implement `StateStore` for any other backend:

```go
type StateStore interface {
    Save(userID string, states map[string]SavedState) error
    Load(userID string) (map[string]SavedState, error)
    Close() error
}
```

**Example — Redis backend:**

```go
type RedisStore struct{ client *redis.Client; prefix string }

func (r *RedisStore) Save(userID string, states map[string]pace.SavedState) error {
    data, _ := json.Marshal(states)
    return r.client.Set(ctx, r.prefix+userID, data, 24*time.Hour).Err()
}

func (r *RedisStore) Load(userID string) (map[string]pace.SavedState, error) {
    data, err := r.client.Get(ctx, r.prefix+userID).Bytes()
    if errors.Is(err, redis.Nil) {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    var states map[string]pace.SavedState
    return states, json.Unmarshal(data, &states)
}

func (r *RedisStore) Close() error { return nil }

// Usage:
mgr, _ := pace.New(pace.Config{
    Endpoints: ...,
    Store:     &RedisStore{client: redisClient, prefix: "pace:"},
})
```

## Graceful Shutdown

`Shutdown(ctx)` prevents new requests and waits for in-flight `Wait` calls to complete before flushing and closing the store. If `ctx` expires first, remaining waiters are force-cancelled.

```go
// On SIGTERM: give 5 seconds for in-flight requests to finish.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := mgr.Shutdown(ctx); err != nil {
    log.Printf("shutdown forced: %v", err)
}
```

`Close()` cancels immediately (no grace period). Use `Shutdown` when you need to drain requests cleanly.

## How It Works

1. **Token bucket** — each `(user, endpoint)` pair has a `golang.org/x/time/rate.Limiter`. Tokens refill at `RatePerMinute/60` per second up to `Burst`.
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

transport := &mockTransport{...}

mgr, _ := pace.New(pace.Config{
    Endpoints:  map[string]pace.Endpoint{"api": {BaseURL: "http://x", RatePerMinute: 1}},
    Clock:      &fakeClock{now: time.Unix(0, 0)},
    Transport:  transport,
    GCInterval: time.Millisecond,
    IdleExpiry: time.Second,
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
