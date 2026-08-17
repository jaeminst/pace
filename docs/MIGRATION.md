# Migrating

While the version is below 1.0.0, any release may break the API. The freeze
begins at v1.0.0; until then, expect a section here for every release.

- [From v0.3.0 to v0.4.0](#migrating-from-v030)
- [From v0.2.0 to v0.3.0](#migrating-from-v020)
- [From v0.1.0 to v0.2.0](#migrating-from-v010)

# Migrating from v0.3.0

An audit before tagging found defects that cannot be fixed additively once
v1.0.0 freezes the API, so they are fixed here.

## Every change, in one table

| Before | After | Why |
|---|---|---|
| `Config.SharedQuota`, `.QuotaNamespace`, `.QuotaTimeout`, `.OnQuotaError` | `Config.Shared.Quota`, `.Namespace`, `.Timeout`, `.OnError` | Four top-level fields configuring one optional subsystem, three of them documented as ignored when the fourth is nil — verbatim the situation v0.3.0 fixed for the queue, with the same argument, in the same release. It also stops `Config.QuotaFor` (per-user tiering, which works with no backend at all) reading as if `QuotaTimeout` and `OnQuotaError` governed it. |
| `client.Allow()`, `client.Reserve()` | `Allow(ctx)`, `Reserve(ctx)` | Both do real I/O — a store load on a user's cold path, and a backend round-trip when `Config.Shared.Quota` is set — and were the only two entry points in the package that did so without a context. `Wait`, every request method, `Evict`, `DeadJobs`, and every method of `StateStore` and `SharedQuota` all take one; MIGRATION's own justification for `req.Get(ctx, path)` was that "the context belongs to the operation that does I/O". These are the load-shedding methods an inbound handler calls with a request context already in hand. |
| `StateStore` had `Close() error` | dropped; implement `io.Closer` if you need it | A lifecycle method inside a persistence contract, forcing every implementation to carry one whether it had resources or not — the README's own example wrote `func (r *RedisStore) Close() error { return nil }` because the interface demanded it. The optional-extension-by-assertion pattern is this codebase's own idiom (`BatchStateStore`, `WaitingSharedQuota`), and `io.Closer` is the most assertable interface in Go. Narrowing an interface is impossible after v1. Also newly documented: **pace closes a `Config.Store` that implements `io.Closer`**, which it always did and never said. |
| `QueueConfig.RetryOn func(resp *Response) bool` | `func(ctx context.Context, d RetryDecision) bool` | The one hook whose whole job is judgement, frozen at a single input forever. `RetryDecision` carries `Response`, `Method`, `Path` and `Attempt` — the last of which "retry a 503 twice, not five times" needs and could never have been added. |
| `Grant.Tokens float64`, negative meaning "not tracked" | `*float64`, nil meaning "not tracked" | v0.2.0 removed exactly this pattern from `Client.Tokens`, with the reason recorded: a sentinel could not be told apart from a legitimately negative count. pace's own buckets go negative while a reservation is outstanding, so a backend modelled the same way reports a real negative that the sentinel swallowed. |
| `Limiter.DeadJobs(ctx, limit int)` | `DeadJobs(ctx, DeadJobQuery{Limit, Before, UserID})` | A bare limit could read the dead-letter table from the top and nowhere else, so anything past the newest N rows was unreachable — on the one table whose stated purpose is "the ones a human has to decide about" and the one table nothing bounds. `DeadJobQuery{}` is the old behaviour; pass the last job's `DiedAt` as the next query's `Before` to page. |
| `Stats.Requests`, `.Throttled`, `.Errors`, `.Evictions` were `uint64` | all `int64` | Field types cannot change after v1. Metrics code diffs consecutive snapshots, and unsigned subtraction wraps silently on the reset a restarted Limiter produces — a negative delta is a visible bug where `18446744073709551615` is a mystery. `Users` was already `int64`, so the struct was mixing signedness. |
| `Observer.UserEvicted func(userID string, reason EvictReason)` | `func(ctx context.Context, info EvictInfo)` | Two loose positional parameters, so the one hook in the struct that could never gain a field — while the compatibility promise's entire safety argument is that the Info structs grow. `EvictInfo` carries `UserID`, `Reason`, and now `Tokens` and `LastUsed`, which are what an operator wants when a store is slow. |
| `Observer.JobTransition func(info JobInfo)` | `func(ctx context.Context, info JobInfo)` | The other two hooks take a context. These fire from background goroutines, where the Limiter's own context is genuinely useful: it is cancelled at `Close`, so a hook doing bounded work can bail instead of holding up shutdown. |
| `Config.Queue.OnDeadLetter func(job DeadJob)` | `func(ctx context.Context, job DeadJob)` | As above. |
| `Stats.Wait` | `Stats.WaitTotal` | It is a running sum, not a current or average value, and the name did not say so. Divide by `Throttled` for a mean. |

| `pacetest.NewQuota` (type), `QuotaSuite(t, newQuota)` | `pacetest.QuotaFactory`, `QuotaSuite(t, newQuota, opts ...SuiteOption)` | `NewQuota` as a *type* name reads like a constructor function. The variadic is empty today and costs nothing; adding it later would be a signature change, which this package's own promise — "no exported identifier changes meaning or signature within v1" — forbids. |

**Compatibility promise, generalised.** `doc.go` used to list the structs
allowed to gain fields, which meant `RetryPolicy`, `TransportConfig`, `DeadJob`,
`LimitError`, `ConfigError` and `UserState` were outside it by omission. The
rule is now stated once for every exported struct — with one carve-out:
**`State` is frozen.** It is the wire format between pace and a third-party
`StateStore`, so adding a field would compile everywhere and silently break
every existing store.

**New, additive:** `DeadJob.DiedAt` — the dead-letter table has always stored
`died_at` and never exposed it, so an operator reading dead jobs could not tell
when anything died. Schema v3 also indexes that column, which `DeadJobs` has
always ordered by without one.

**New, additive:** `Stats.QuotaTakes`, `.QuotaRefused` and `.QuotaErrors` report
what `Config.SharedQuota` is doing. Nothing previously said whether the backend
was being reached at all, so an operator whose Redis was down saw a
healthy-looking snapshot while every replica quietly fell back to enforcing the
rate per process. `QuotaErrors` is the one to alert on; it counts both failed
calls and the ones the circuit breaker short-circuited.


# Migrating from v0.2.0

v0.3.0 is the last release that may break the API. v1.0.0 freezes it, and after
that a breaking change costs a `/v2` import path permanently — so what is left
here is only what becomes impossible later. Everything additive was deferred.

## Every change, in one table

| Before | After | Why |
|---|---|---|
| `Config.IdempotencyHeader`, `.AmbiguousPolicy`, `.OnDeadLetter`, `.Retry`, `.RetryOn`, `.QueueWorkers`, `.QueuePollInterval`, `.JobLease`, `.ResultTTL` | `Config.Queue.IdempotencyHeader`, `.AmbiguousPolicy`, `.OnDeadLetter`, `.Retry`, `.RetryOn`, `.Workers`, `.PollInterval`, `.JobLease`, `.ResultTTL` | Nine of `Config`'s fields configured one optional subsystem, sharing a namespace with `Rate` and `Burst`. Grouping them is impossible after v1, and not grouping means every future queue knob inflates the top-level struct forever. The `Queue` prefixes drop, since the nesting supplies them. |
| `Config.Store` and `Config.DBPath` were mutually exclusive | both may be set | They persist different things. `Store` owns per-user token state; `DBPath` owns the durable queue. Forbidding both meant a Redis- or Postgres-backed caller could never have a queue, with no signal at `New`. Setting only `Store` still means no queue — now the `ErrNoQueue` message says so. |
| `client.Durable(id)` → `(*Request, error)` | → `*Request` | Building a Request is documented, twice, as free and infallible; `Durable` was the one exception, and it cost every durable call site a four-line error block for two conditions that are constant for the life of the process. `ErrNoQueue` and `ErrInvalidID` now surface from `Get`/`Post`/`Stream`/… , where you are already checking an error. Delete the `if err != nil` after `Durable`; keep the one on the terminal call. |

```go
// Before
cfg := pace.Config{
    BaseURL:           "https://payments.example.com",
    Rate:              pace.PerMinute(60),
    DBPath:            "/var/lib/pace/state.db",
    QueueWorkers:      8,
    QueuePollInterval: 500 * time.Millisecond,
    AmbiguousPolicy:   pace.AmbiguousPark,
}

// After
cfg := pace.Config{
    BaseURL: "https://payments.example.com",
    Rate:    pace.PerMinute(60),
    DBPath:  "/var/lib/pace/state.db",
    Queue: pace.QueueConfig{
        Workers:         8,
        PollInterval:    500 * time.Millisecond,
        AmbiguousPolicy: pace.AmbiguousPark,
    },
}
```

If you never set any of the queue fields, nothing changes: the zero
`QueueConfig` resolves to the same defaults the flat fields did.

```go
// Before
req, err := lim.Client("user-123").Durable(chargeID)
if err != nil {
    return err
}
resp, err := req.SetBody(body).Post(ctx, "/v1/charge")

// After
resp, err := lim.Client("user-123").Durable(chargeID).
    SetBody(body).
    Post(ctx, "/v1/charge")
```

## New: per-user quotas

`Config.Rate` and `Config.Burst` are now the *default* rather than the only
rate. `Config.QuotaFor func(string) Quota` overrides them per user, and
`Limiter.ReloadQuotas()` re-applies it to buckets already in memory. Nothing
changes if you leave `QuotaFor` nil.

`LimitError.Limit`/`Burst` and `ThrottleInfo.Limit`/`Burst` now report the quota
that user's bucket is enforcing. Their documentation always said "the
configuration in force for that user"; until `QuotaFor` existed there was only
one configuration, so reading `Config.Rate` happened to be right.

Also new: `Client.Quota()` reports the rate and burst in force for a user.

## New: Client.Reserve

`Reserve` holds a token and reports how long until it may be used, without
blocking and without committing you to the request:

```go
r := alice.Reserve()
if !r.OK() || r.Delay() > tolerable {
    r.Cancel() // hand the token back
    return errTooBusy
}
time.Sleep(r.Delay())
```

It fills the gap between `Allow`, which refuses rather than waits, and `Wait`,
which waits and cannot refund. Nothing about the existing two changes.

## New: optional cross-replica limiting

`Config.SharedQuota` delegates the decision to a backend every replica
consults, so the limit binds across processes rather than per process. You
supply the backend; pace ships `pacetest.QuotaSuite` to check it against the
contract. Nothing changes if you leave it nil.

Before adopting it, read [ADR 0004](adr/0004-shared-quota-is-approximate.md)
— and the paragraph in the README that argues most services should not.

One behavioural note if you do: `Client.Allow` gains a backend call bounded by
`Config.QuotaTimeout`, and with a shared quota set the local bucket is no
longer written to or read from `Config.Store`.

# Migrating from v0.1.0

v0.2.0 is a single consolidated breaking release. Everything that was ever going
to break breaks here, so that v1.0.0 can freeze the API — after v1, a breaking
change costs a `/v2` import path permanently.

There are no deprecation shims. A shim added now would become permanent v1
baggage, and the compiler finds every call site anyway.

**The common path does not change.** `client.Get(ctx, "/path")` and its siblings
have the same signature they always had. What changes is how you get the client,
how you configure it, and what a few methods return.

## The 90% case

```go
// Before
client, err := pace.New(pace.Config{
    Name:          "alice",
    BaseURL:       "https://api.example.com",
    RatePerMinute: 60,
    Burst:         10,
})
defer client.Close()
resp, err := client.Get(ctx, "/items/42")

// After
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    pace.PerMinute(60),
    Burst:   10,
})
defer lim.Close()
resp, err := lim.Client("alice").Get(ctx, "/items/42")
```

## Every change, in one table

| Before | After | Why |
|---|---|---|
| `pace.New(cfg)` returns `*Client` | returns `*Limiter` | Lifecycle belonged to a per-user handle, so `alice.For("bob").Close()` tore down alice's limiter too. |
| `client.For("alice")` | `lim.Client("alice")` | Same idea, correct owner. |
| `Config.Name` | *(removed)* | When unset, every call was rate-limited as one anonymous `""` user. Identity is now always explicit. |
| `client.Close()` / `client.Shutdown(ctx)` | `lim.Close()` / `lim.Shutdown(ctx)` | They release Limiter resources, not per-user state. |
| `Close()` returns nothing | returns `error` | The store's close error was swallowed into a log line. `*Limiter` now satisfies `io.Closer`. |
| `RatePerMinute: 60` | `Rate: pace.PerMinute(60)` | The old field truncated any rate that did not divide 60s evenly. |
| `client.Request(ctx)` → `(*Request, error)` | `client.Request()` → `*Request` | Building is free now; the token is taken when the request is sent. |
| `req.Get("/path")` | `req.Get(ctx, "/path")` | The context belongs to the operation that does I/O. |
| `client.Request(ctx)` used only to take a token | `client.Wait(ctx)` | That is what it was doing. |
| `client.Durable(ctx, id)` → `*Request` | `client.Durable(id)` → `(*Request, error)` | `Durable("")` used to skip rate limiting entirely. |
| `client.Tokens()` → `float64` (-1 sentinel) | → `(float64, bool)` | -1 could not be told apart from a real negative count. |
| `client.Evict()` → `bool` | `client.Evict(ctx)` → `(bool, error)` | It does store I/O; the error was being discarded. |
| `Config.OnThrottle func(userID string)` | `Config.Observer.Throttled func(ctx, ThrottleInfo)` | The old callback reported only *that* throttling happened. |
| `pace.SavedState` | `pace.State` (with `LastUsed time.Time`) | Unix nanoseconds were a serialisation detail in a public interface. |
| `StateStore.Save(userID, state)` | `Save(ctx, userID, state)` | The README advertised Redis and Postgres backends the old signature could not support. |
| `StateStore.Load(userID)` | `Load(ctx, userID)` | As above. |
| `ErrNoPersistence` | `ErrNoQueue` | It reports a missing durable *queue*, not a missing store. |
| `req.SetHeader` on a `map[string]string` | headers are `http.Header` | The old type could not express a header that repeats. |
| `New` returns opaque errors | returns `*ConfigError` | So you can tell which field was rejected. |

## Custom `StateStore` implementations

The interface gained a context and `SavedState` became `State`:

```go
// Before
func (s *MyStore) Save(userID string, state pace.SavedState) error
func (s *MyStore) Load(userID string) (pace.SavedState, bool, error)

// After
func (s *MyStore) Save(ctx context.Context, userID string, st pace.State) error
func (s *MyStore) Load(ctx context.Context, userID string) (pace.State, bool, error)
```

`State.LastUsed` is a `time.Time` rather than unix nanoseconds. Each call arrives
with a context bounded by `Config.StoreTimeout` (5s by default).

If your backend can write many users at once, implement the optional
`BatchStateStore` as well — the idle sweep can evict thousands at a time:

```go
func (s *MyStore) SaveBatch(ctx context.Context, states []pace.UserState) error
```

## Durable requests

`Durable` now reports setup errors instead of deferring them, and takes the
context at the terminal call:

```go
// Before
resp, err := client.Durable(ctx, chargeID).SetBody(body).Post("/v1/charge")

// After
req, err := lim.Client("user-123").Durable(chargeID)
if err != nil {
    return err // ErrNoQueue, or ErrInvalidID for an empty ID
}
resp, err := req.SetBody(body).Post(ctx, "/v1/charge")
```

**Read the guarantees again before you rely on them.** v0.1.0 documented
"exactly-once semantics"; that was not true, and a job dispatched but never
recorded was re-sent on restart. Delivery is at-least-once, the ambiguous window
is now detectable rather than silent, and `Config.Queue.AmbiguousPolicy` decides what
happens to a job caught in it. See the README.

If you were setting `Idempotency-Key` yourself, you can stop: it is sent
automatically, carrying the job ID.

## Your database upgrades in place

A SQLite file written by v0.1.0 is migrated on open. Stored durable-job headers
are converted to the new representation, and the schema is stamped with a
version so that a rolled-back deploy refuses to open it rather than writing
through columns it does not understand.

WAL mode is now on, which means two extra files beside the one you named:

```
state.db  state.db-wal  state.db-shm
```

Back them up and delete them together, and keep `DBPath` on local storage — WAL
is unsafe on NFS and SMB.

## Worth adopting while you are here

None of these are required, but they close gaps the old API left open:

- `Config.MaxResponseBytes` — an unbounded `io.ReadAll` is how a misbehaving
  upstream takes your process down.
- `Config.RequestTimeout` — bounds a round-trip without counting the wait for a
  token against it.
- `Limiter.Stats()` and `Config.Observer` — the only observability before this
  was one callback and some log lines.
- `resp.RetryAfter()` — you throttle outbound requests because upstream limits
  you; this is upstream telling you its real limit.
