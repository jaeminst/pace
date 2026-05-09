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
- **SQLite persistence** — optional state survival across process restarts (pure Go, no CGO)
- **Sharded user map** — 256 FNV-32a shards minimise lock contention under high concurrency
- **Goroutine-efficient** — `context.AfterFunc` eliminates per-request goroutine allocation in the hot path
- **Testable by design** — injectable `Clock` and `http.RoundTripper` for deterministic unit tests

## Install

```sh
go get github.com/jaeminst/pace
```

Requires **Go 1.25+**.

## Quick Start

```go
mgr, err := pace.New(pace.Config{
    Endpoints: map[string]pace.EndpointConfig{
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
| `Endpoints` | `map[string]EndpointConfig` | — | **Required.** Named endpoint map. |
| `IdleExpiry` | `time.Duration` | 10m | How long a user can be inactive before in-memory state is GC'd. |
| `GCInterval` | `time.Duration` | 1m | How often the GC sweep runs. |
| `Transport` | `http.RoundTripper` | `http.DefaultTransport` | HTTP transport for all requests. |
| `Clock` | `Clock` | system clock | Override for deterministic testing. |
| `Logger` | `*slog.Logger` | `slog.Default()` | Receives internal warnings. |
| `DBPath` | `string` | "" (disabled) | Path to SQLite file for state persistence. |

### `EndpointConfig`

| Field | Type | Description |
|---|---|---|
| `BaseURL` | `string` | **Required.** Base URL prepended to every request path. |
| `RatePerMinute` | `int` | **Required (> 0).** Maximum requests per user per minute. |
| `Burst` | `int` | Maximum token accumulation when idle. Defaults to 1. |

## How It Works

1. **Token bucket** — each `(user, endpoint)` pair has a `golang.org/x/time/rate.Limiter`. Tokens refill at `RatePerMinute/60` per second up to `Burst`.
2. **Sharded map** — user entries live in one of 256 shards (FNV-32a hash). Read-heavy workloads acquire only a read lock; new-user creation takes a write lock on a single shard.
3. **GC sweep** — a background goroutine wakes every `GCInterval` and evicts users whose `lastUsed` timestamp is older than `IdleExpiry`. Evicted state is flushed to SQLite if `DBPath` is set.
4. **Persistence** — on eviction (and on `Close`), the current token count and timestamp are saved to SQLite. On next access, `restoreBucket` re-creates the limiter accounting for elapsed time.

## Testing

pace exposes an injectable `Clock` interface and accepts a custom `http.RoundTripper`, making it straightforward to write fast, deterministic tests:

```go
type fakeClock struct{ now time.Time }
func (c *fakeClock) Now() time.Time { return c.now }

transport := &mockTransport{...}

mgr, _ := pace.New(pace.Config{
    Endpoints:  map[string]pace.EndpointConfig{"api": {BaseURL: "http://x", RatePerMinute: 1}},
    Clock:      &fakeClock{now: time.Unix(0, 0)},
    Transport:  transport,
    GCInterval: time.Millisecond,
    IdleExpiry: time.Second,
})
```

The `export_test.go` file (package `pace`) exposes `collectIdle` via `pace.CollectIdle` so white-box tests can trigger GC sweeps without waiting for the ticker.

## Caveats

- **Single process** — the in-memory sharded map is not distributed. For multi-instance deployments, use a shared backing store (e.g. Redis) instead of pace's built-in SQLite.
- **SQLite write serialisation** — all SQLite writes share a single `*sql.DB`. This is fine for GC eviction and shutdown; it is not designed for high-frequency per-request persistence.
- **No retry logic** — pace enforces rate limits by blocking; it does not retry failed HTTP requests.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

[MIT](LICENSE) © pace contributors
