# Migrating

While the version is below 1.0.0, any release may break the API. The freeze
begins at v1.0.0; until then, expect a section here for every release.

Sections below quote the names as they were at the time. `pace.PerMinute` became
`limiter.PerMinute` in v0.11.0 and `config.PerMinute` in v0.12.0; the root
declares nothing at all now. Read the older sections as history, not as code to
copy.

- [From v0.13.0 to v0.14.0](#migrating-from-v0130) — one hook holds the quota
- [From v0.12.0 to v0.13.0](#migrating-from-v0120) — the rate is adjustable at run time
- [From v0.11.0 to v0.12.0](#migrating-from-v0110) — three packages: config, limiter, client
- [From v0.10.0 to v0.11.0](#migrating-from-v0100) — the root re-exports nothing
- [From v0.9.0 to v0.10.0](#migrating-from-v090) — names that say what they are
- [From v0.8.0 to v0.9.0](#migrating-from-v080) — one import, and tests where they belong
- [From v0.7.0 to v0.8.0](#migrating-from-v070) — the library ships contracts, not backends
- [From v0.5.0 to v0.7.0](#migrating-from-v050) — the library splits into packages
- [From v0.3.0 to v0.4.0](#migrating-from-v030)
- [From v0.2.0 to v0.3.0](#migrating-from-v020)
- [From v0.1.0 to v0.2.0](#migrating-from-v010)

# Migrating from v0.13.0

Breaking, and the compiler finds all of it. `Config.Rate` and `Config.Burst` are
gone; `Config.QuotaFor` is required and is the only place a rate is configured.

## The mechanical change

```go
// before
cfg := config.Config{
    BaseURL: "https://api.example.com",
    Rate:    bucket.PerMinute(60),
    Burst:   10,
}

// after
cfg := config.Config{
    BaseURL:  "https://api.example.com",
    QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}),
}
```

`config.Fixed` gives every user the same quota. It is a convenience over the one
hook, not a second place to configure a rate.

## If you used QuotaFor, write your own fallback

**This is the one that is not a compile error.** A `Quota` returned from
`QuotaFor` used to have each field fall back to `Config.Rate` / `Config.Burst`
when it was zero, so a map lookup that missed produced the defaults. There are
no defaults now — the zero `Quota` is a rate of zero, which is a bucket that
never refills.

```go
// before: an unlisted user got Config.Rate and Config.Burst
cfg.QuotaFor = func(userID string) bucket.Quota {
    return (*tiers.Load())[userID]
}

// after: say what an unlisted user gets
free := bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}
cfg.QuotaFor = func(userID string) bucket.Quota {
    if q, ok := (*tiers.Load())[userID]; ok {
        return q
    }
    return free
}
```

A partial override — a tier that set only `Rate` and inherited the burst — has
to name both fields now.

If you miss one, the user is throttled to a standstill and pace logs a warning
at `Logger` naming them. That is the failure mode of this release: it fails
closed and quietly, where `Config.Rate` used to fail at `client.New`.

## SetDefaultQuota is gone

Change what `QuotaFor` reads, then reload. This is what per-user changes already
did, and it covers the population-wide default too:

```go
// before
pool.SetDefaultQuota(bucket.Quota{Rate: bucket.PerMinute(120), Burst: 20})
pool.ReloadQuotas()

// after
live.Store(&bucket.Quota{Rate: bucket.PerMinute(120), Burst: 20})  // whatever QuotaFor reads
pool.ReloadQuotas()
```

The semantics are unchanged: a user with no bucket yet picks the new value up
without a reload, because they are about to call `QuotaFor`; a user already in
memory keeps what they have until `ReloadQuotas` or `ReloadQuota`. Swap and
reload from the same goroutine — racing them can leave a population split, with
nothing to re-run the walk.

`Pool.DefaultQuota` is gone with it. `pool.Client(id).Quota()` reports what one
user is enforcing, which is the question that still has an answer.

## Resolve no longer rejects a bad rate

`Config.Resolve` returned a `*config.Error` on `Field: "Rate"` for a rate at or
below zero or a NaN. It cannot: at Resolve time there is a function, not a rate.
It returns `Field: "QuotaFor"` if the hook is missing, and `limiter.New` panics
on a nil one.

The clamping still happens, one user at a time, in the engine — burst below one
raised to one, infinity to `bucket.Inf`, and anything unusable to zero with a
warning. If you had a test asserting `client.New` rejects a NaN rate, it now
asserts the clamp instead.

## If you implement against registry or bucket directly

Neither is a package most callers touch, and both are pace's own rather than
contracts a third party implements — `store`, `shared` and `observe` are
unchanged and still carry plain `float64` and `int`.

| before | after |
|---|---|
| `registry.Spec.QuotaFor func(string) (float64, int)` | `func(string) bucket.Quota` |
| `bucket.NewBucket(perSec float64, burst int)` | `bucket.NewBucket(q bucket.Quota)` |
| `bucket.RestoreBucket(perSec, burst, tokens, savedAt, now)` | `bucket.RestoreBucket(q, tokens, savedAt, now)` |
| `bucket.SetQuotaAt(t, perSec, burst)` | `bucket.SetQuotaAt(t, q)` |

# Migrating from v0.12.0

Additive, apart from two signatures in `pace/gate`. If you use `pace/client` and
`pace/config` — which is the normal case — nothing here breaks you, and the new
part is that the rate is adjustable while the process runs.

## Check your QuotaFor

**This is the one worth acting on even though it is not a compile error.**

`config.Config.QuotaFor` is called from request goroutines, one per user whose
bucket is being created. It always was; nothing said so, and the README and the
package example both showed a plain map being written from the caller's
goroutine while the closure read it. If you copied that shape, you have a data
race that `go test -race` will not have shown you unless your tests are
concurrent.

```go
// racy — a plain map read on request goroutines, written on yours
tiers := map[string]config.Quota{...}
cfg.QuotaFor = func(id string) config.Quota { return tiers[id] }
tiers["trial-42"] = next

// safe — swap the whole table behind a pointer
var tiers atomic.Pointer[map[string]config.Quota]
cfg.QuotaFor = func(id string) config.Quota { return (*tiers.Load())[id] }
tiers.Store(&next)
```

A mutex is equally fine. What is not fine is an unguarded map.

## New: change the rate at run time

```go
// the default, for every user QuotaFor does not name
err := pool.SetDefaultQuota(config.Quota{Rate: config.PerMinute(120), Burst: 20})
q := pool.DefaultQuota()

// one user, in O(1) instead of a walk of every shard
ok := pool.Client("trial-42").ReloadQuota()
```

`config.Config.Rate` and `Burst` are the *initial* default now. Reading them back
off a `config.Config` you kept tells you what a **new** Limiter would use, not
what a running one is using — ask `Pool.DefaultQuota` for that.

Applying is a separate step, as it already was for `QuotaFor`: a change reaches a
user with no bucket at once, and users already in memory when you call
`ReloadQuotas` or `ReloadQuota`. Call `SetDefaultQuota` and the reload from the
same goroutine; racing them can leave a population permanently split.

`ReloadQuota` is not `Evict`. The README used to say `Evict` "has the same
effect" for a single user; it does not — it also drops the accrued tokens and
writes to the store.

## The rate vocabulary moved to `pace/bucket`

`config.Quota` is `bucket.Quota`, and so are `Limit`, `Inf`, `Finite` and the
four constructors. A Quota is a rate and a ceiling, which is what a bucket is —
and until now `Bucket.Quota()` had to hand back two loose numbers because
`bucket` could not name a type in `config` without an import cycle.

```go
// v0.12.0
config.Config{Rate: config.PerMinute(60), Burst: 10}
var q config.Quota

// v0.13.0
config.Config{Rate: bucket.PerMinute(60), Burst: 10}
var q bucket.Quota
```

| v0.12.0 | v0.13.0 |
|---|---|
| `config.Limit` | `bucket.Limit` |
| `config.Quota` | `bucket.Quota` |
| `config.Inf` `config.Finite` | `bucket.Inf` `bucket.Finite` |
| `config.PerSecond` `PerMinute` `PerHour` `Every` | `bucket.PerSecond` … |

`config` keeps `Config`, `Clock` and `Error`. Every break is a compile error, and
`goimports` will add the new import for you.

The cost is one more import in any file that writes a rate. See
[ADR 0011](adr/0011-the-vocabulary-belongs-to-the-bucket.md) for why that was
taken over two hand-written conversions on the path every throttle report takes.

## If you import `pace/gate` or `pace/bucket`

| v0.12.0 | v0.13.0 |
|---|---|
| `gate.Acquire(ctx, userID, b, rateLimit, burst)` | `gate.Acquire(ctx, userID, b)` |
| `gate.Allow(ctx, userID, b, rateLimit, burst, now)` | `gate.Allow(ctx, userID, b, now)` |
| `bucket.Limit()` + `bucket.Burst()` | `bucket.Quota()` returns a `bucket.Quota` |

The bucket carries its quota now, so passing it alongside meant two sources for
one number — which is how `Acquire`'s poll loop came to tell a backend the quota
a request started with while the shadow bucket had already moved on.
`bucket.Quota()` returns the pair in one load, which is the point: reading the
two separately could give a combination nobody configured.

`gate.Take` is unchanged. It has no bucket, so it still takes numbers.

# Migrating from v0.11.0

**This is the largest break so far, and unlike v0.11.0 it is not spelling-only.**

pace is three packages now: `config` for everything you configure, `limiter` for
the rate limiter, `client` for creating clients and making requests. The
repository root declares nothing. See
[ADR 0009](adr/0009-config-limiter-client.md).

v0.11.0 moved type *aliases*, so a value crossed the boundary unchanged. This
time `Client`, `Request`, `Response`, `ErrBodyTooLarge` and the rate vocabulary
are **declarations moving between packages**. A caller holding a
`*limiter.Client` in their own struct field has a type change, not a spelling
change. Every break is still a compile error — nothing goes silently wrong.

## The shape of it

```go
// v0.11.0
import (
    "github.com/jaeminst/pace"
    "github.com/jaeminst/pace/limiter"
)

lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    limiter.PerMinute(60),
})
defer lim.Close()
resp, err := lim.Client("alice").Get(ctx, "/items/42")

// v0.12.0
import (
    "github.com/jaeminst/pace/client"
    "github.com/jaeminst/pace/config"
)

pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Rate:    config.PerMinute(60),
})
defer pool.Close()
resp, err := pool.Client("alice").Get(ctx, "/items/42")
```

## `import "github.com/jaeminst/pace"` — watch this one

The path still resolves: the root is a `doc.go` with `package pace` and no
declarations, so pkg.go.dev keeps a landing page. But the import becomes
**unused**, and `goimports` deletes an unused import without saying anything. If
you run it on save you will not see the message. Nothing breaks either way — it
is only worth knowing so the silent edit is not a surprise.

## The table

| v0.11.0 | v0.12.0 |
|---|---|
| `pace.New` | `client.New` |
| `*pace.Limiter` (what `New` returned) | `*client.Pool` |
| `pace.Config` | `config.Config` |
| `pace.Clock` | `config.Clock` |
| `pace.ConfigError` | `config.Error` |
| `limiter.Limit` | `config.Limit` |
| `limiter.Quota` | `config.Quota` |
| `limiter.Inf` `limiter.Finite` | `config.Inf` `config.Finite` |
| `limiter.PerSecond` `PerMinute` `PerHour` `Every` | `config.PerSecond` … |
| `limiter.Client` | `client.Client` |
| `limiter.Request` | `client.Request` |
| `limiter.Response` | `client.Response` |
| `limiter.ErrBodyTooLarge` | `client.ErrBodyTooLarge` |
| `limiter.LimitError` | unchanged |
| `limiter.ErrClosed` | unchanged |
| `limiter.Reservation` | unchanged |

`Config`'s two typed fields follow the vocabulary:

```go
Rate     config.Limit                        // was limiter.Limit
QuotaFor func(userID string) config.Quota    // was func(string) limiter.Quota
```

`config.ConfigError` would have stuttered — revive says so and suggests `Error`
itself — so it is `config.Error`, with `net/url.Error` as the precedent. The
message text is unchanged: it still reads `pace: invalid Config.Rate (0): …`,
because it names your struct field rather than the Go type.

## `ErrBodyTooLarge` moved, and it is a sentinel

```go
// v0.11.0
if errors.Is(err, limiter.ErrBodyTooLarge) { … }

// v0.12.0
if errors.Is(err, client.ErrBodyTooLarge) { … }
```

This is safe only because the old declaration is **deleted** rather than left
behind. If you are tempted to add a compatibility shim —
`var ErrBodyTooLarge = client.ErrBodyTooLarge` somewhere — that is fine; a
second `errors.New` with the same text is not. It would compile and be
permanently false. `errors.Is` compares identity, not message.

## `Limiter` is not what `New` returns any more

`client.New` returns a `*client.Pool`. The engine is underneath it:

```go
pool, _ := client.New(cfg)

pool.Client("alice")   // *client.Client — a per-user HTTP handle
pool.Limiter()         // *limiter.Limiter — the engine, keyed by user ID
pool.Close()           // Close / Shutdown / Stats / ReloadQuotas / SetDefaultQuota forward to it
```

Reach for `pool.Limiter()` to pace work pace does not perform for you. It takes
the user ID per call, so you do not need a Client whose base URL you never use:

```go
if err := pool.Limiter().Wait(ctx, "alice"); err != nil {
    return err
}
writeToTheDatabase()
```

## If you build a `limiter.Spec` by hand

There is no `Spec`. `limiter.New` takes the `config.Config` itself:

```go
// v0.11.0
lim := limiter.New(limiter.Spec{ /* ten fields */ })

// v0.12.0
cfg, err := cfg.Resolve()   // or let client.New do both
lim := limiter.New(cfg)
```

The ten `Spec` fields were ten `Config` fields under the same names, so there is
nothing to map. `Resolve` is what fills in the optional ones; `limiter.New`
panics on a Config it cannot use, naming the field, exactly as the old
`Spec.validate` did.

`HTTPClient`, `BaseURL`, `RequestTimeout` and `MaxResponseBytes` were the four
`Spec` fields with no `Config` counterpart to keep — the engine makes no
requests, so it ignores them and `client.New` keeps them. `limiter.New` no
longer panics on a missing `BaseURL` or `HTTPClient`; there is nothing to
check.

## If you validate a configuration yourself

Two methods are exported now, which is new capability rather than a break:

```go
resolved, err := cfg.Resolve()   // validate, then fill in every optional field
q := resolved.Quota("alice")     // the quota that user would get
```

`Resolve` is what `client.New` calls. Call it directly to check a configuration
at startup — against a file you have just parsed, say — without building an
engine. Call `Quota` only on a resolved Config: on an unresolved one it returns
`{0, 0}`, which is a bucket that refuses everything.

# Migrating from v0.10.0

**This one touches every caller**, which no release since v0.8.0 has done. The
root re-exports nothing now: `pace` holds `Config`, `Clock`, `ConfigError` and
`New`, and every other name you use is spelled `limiter.`.

Because the deleted names were *type aliases*, the change is spelling-only and
never type-identity. Nothing changes meaning, nothing changes signature, and
every break is a compile error rather than a silent one. Code you already wrote
as `limiter.PerMinute(60)` needs no edit.

Why: an alias renders in godoc as a single line with no methods and no fields,
so `go doc pace Limiter` printed `type Limiter = limiter.Limiter` and stopped.
See [ADR 0008](adr/0008-the-root-re-exports-nothing.md).

## The one-line change

```go
import (
    "github.com/jaeminst/pace"
    "github.com/jaeminst/pace/limiter"   // add this
)
```

## The table

| v0.10.0 | v0.11.0 |
|---|---|
| `*pace.Limiter` | `*limiter.Limiter` |
| `*pace.Client` | `*limiter.Client` |
| `*pace.Request` | `*limiter.Request` |
| `*pace.Response` | `*limiter.Response` |
| `*pace.Reservation` | `*limiter.Reservation` |
| `*pace.LimitError` | `*limiter.LimitError` |
| `pace.Limit` | `limiter.Limit` |
| `pace.Quota` | `limiter.Quota` |
| `pace.Inf` | `limiter.Inf` |
| `pace.PerSecond` `pace.PerMinute` `pace.PerHour` `pace.Every` | `limiter.PerSecond` … |
| `pace.ErrClosed` | `limiter.ErrClosed` |
| `pace.ErrBodyTooLarge` | `limiter.ErrBodyTooLarge` |

`pace.Config`, `pace.Clock`, `pace.ConfigError` and `pace.New` are unchanged.
`New` returns `*limiter.Limiter` — the engine's own type, not a wrapper.

Two `Config` fields change type for the same reason, and again the type is the
same type:

```go
Rate     limiter.Limit                        // was pace.Limit
QuotaFor func(userID string) limiter.Quota    // was func(string) pace.Quota
```

## Before and after

```go
// v0.10.0
lim, err := pace.New(pace.Config{BaseURL: base, Rate: pace.PerMinute(60)})
var le *pace.LimitError
if errors.As(err, &le) { … }

// v0.11.0
lim, err := pace.New(pace.Config{BaseURL: base, Rate: limiter.PerMinute(60)})
var le *limiter.LimitError
if errors.As(err, &le) { … }
```

## If you import `pace/response`

The package is gone. `limiter.Response` is the same type with the same methods.

```go
// v0.10.0
import "github.com/jaeminst/pace/response"
func handle(r *response.Response) { … }

// v0.11.0
import "github.com/jaeminst/pace/limiter"
func handle(r *limiter.Response) { … }
```

`response.New` has no replacement, deliberately: it was a public constructor for
a type a caller only ever receives. Build a `Response` by making a request; a
test that needs a canned one should serve it from an `httptest.Server`, which is
what pace's own tests do.

## One behaviour change

`Client.Evict` now runs its store write under the Limiter's lifetime as well as
the caller's context, so closing the Limiter ends an eviction that is still
waiting on a wedged backend. Previously the caller's context was passed through
raw and a `Close` could not reach it. Nothing to change on your side unless you
relied on an Evict outliving the Limiter that issued it.

# Migrating from v0.9.0

Renames only. Nothing changed behaviour, and every break is a compile error
rather than a silent one.

## If you implement a cross-replica backend

```go
// v0.9.0
var _ shared.Quota = (*myBackend)(nil)
cfg.Shared = shared.Config{Quota: b}

// v0.10.0 — the Take method itself is unchanged
var _ shared.Backend = (*myBackend)(nil)
cfg.Shared = shared.Config{Backend: b}
```

| v0.9.0 | v0.10.0 |
|---|---|
| `shared.Quota` | `shared.Backend` |
| `shared.Config.Quota` | `shared.Config.Backend` |
| `quotatest.QuotaSuite` | `quotatest.Suite` |
| `quotatest.QuotaFactory` | `quotatest.Factory` |

`Quota` meant nine things across the module — an interface, a struct, three
config fields, a method, a factory. `shared.Backend` is what this one is.
`pace.Quota`, the `{Rate, Burst}` pair, keeps the name.

## If you import a support package directly

| v0.9.0 | v0.10.0 |
|---|---|
| `gate.Config` | `gate.Spec` |
| `registry.Config` | `registry.Spec` |
| `pace/persist` | gone; folded into `pace/limiter` |
| `gate.ErrUnsatisfiable` | unexported |

Options are `Config`, vtables are `Spec`. The three `Config` types that remain —
`pace.Config`, `shared.Config`, `transport.Config` — are all options a caller
writes.

`persist` exported seven names and every one existed so that one caller could
wire one value. Nothing outside `limiter` constructed an `Adapter` or called a
method on one. `gate.ErrUnsatisfiable` was produced at one site and matched by
nothing; it arrives wrapped in a `gate.WaitError` as it always did.

## Everything a normal caller writes is unchanged

`pace.Config`, `pace.New`, `Client`, `Request`, `Reservation`, the errors, the
rate vocabulary. If your imports are `github.com/jaeminst/pace` plus some of
`store`, `shared`, `observe` and `transport`, the only line that can break is a
`shared.Quota` you named.

# Migrating from v0.8.0

Two breaking changes, both small, and one of them removes an import from your
code rather than adding one.

## `pace/rate` is gone; the root has the vocabulary

```go
// v0.8.0
import (
    "github.com/jaeminst/pace"
    "github.com/jaeminst/pace/rate"
)
cfg := pace.Config{Rate: rate.PerMinute(60)}

// v0.9.0
import "github.com/jaeminst/pace"

cfg := pace.Config{Rate: pace.PerMinute(60)}
```

| v0.8.0 | v0.9.0 |
|---|---|
| `rate.Limit`, `rate.Quota` | `pace.Limit`, `pace.Quota` |
| `rate.PerSecond`, `PerMinute`, `PerHour`, `Every`, `Inf` | same names on `pace` |
| `rate.Finite` | `limiter.Finite` — plumbing, not re-exported |

The types are the same types, so a `pace.Quota` you already hold still works
everywhere.

**Two contract fields changed shape**, and this is the part that can break a
build silently rather than loudly:

| | v0.8.0 | v0.9.0 |
|---|---|---|
| `observe.ThrottleInfo.Limit` | `rate.Limit` | `float64` (requests per second) |
| `shared.TakeRequest.Quota` | `rate.Quota` | `Rate float64` and `Burst int` |

If you format `ThrottleInfo.Limit` for a log line you lose `"60/min"` and get the
raw number; format it yourself if you want the unit. If you implement
`shared.Quota`, read `req.Rate` and `req.Burst` where you read `req.Quota.Rate`
and `req.Quota.Burst` — both are compile errors, not silent ones.

[ADR 0007](adr/0007-contracts-carry-numbers-not-types.md) has the reasoning: a
package a third party implements should not make them compile pace's types to
read two numbers.

## `limiter.Config` is `limiter.Spec`

Only if you import `pace/limiter` directly. `pace.Config` is untouched.

```go
// v0.8.0
limiter.New(limiter.Config{ … })

// v0.9.0
limiter.New(limiter.Spec{ … })
```

Same fields, same required-everything vtable, same panic naming the field it
cannot work with. It is renamed because `pace.Config` and `limiter.Config`
shared eleven field names and nothing said which was which.

## Nothing else moved

Every other exported name is where it was. What changed underneath is that
roughly thirty tests now live in the package whose behaviour they assert —
`urlx`, `response`, `shared` — which shows up as coverage rather than API.

# Migrating from v0.7.0

v0.8.0 removes two features and moves the configuration to the front door. It is
the largest break so far, and one of the removals has no replacement.

## The durable request queue is gone

`Client.Durable`, `Limiter.DeadJobs`, `Config.Queue`, the whole `pace/queue` and
`pace/runner` packages, `ErrNoQueue`, `ErrJobClaimed`, `ErrInvalidID`,
`ErrStreamDurable`, `observe.JobInfo`, `observe.JobPhase`,
`observe.Observer.JobTransition` and `observe.RequestInfo.Durable`.

**There is no replacement in the library.** If you use durable requests, stay on
v0.7.0. Nothing here will reproduce them, and the reasoning — a queue whose
correctness is cross-process atomicity should not be published as an interface
with no implementation and no way to check one — is in
[ADR 0005](adr/0005-pace-ships-contracts-not-backends.md).

Building one on top of pace is a supported shape: pace paces the sends, your
queue owns the jobs. That is the direction `Client.Reserve` and `Client.Allow`
exist for.

## `Config.DBPath` is gone; implement `store.Store`

The SQLite backend went with the queue. `Config.Store` is the only way to
persist now.

```go
// v0.7.0
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    rate.PerMinute(60),
    DBPath:  "/var/lib/pace/state.db",
})

// v0.8.0
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    rate.PerMinute(60),
    Store:   myStore, // two methods; see examples/store
})
```

Two methods, against whatever already holds your state:

```go
Save(ctx context.Context, userID string, state store.State) error
Load(ctx context.Context, userID string) (store.State, bool, error)
```

Check yours against the contract — the properties pace relies on cannot be
verified at run time and two of them fail silently:

```go
func TestMyStore(t *testing.T) {
    storetest.Suite(t, func(t *testing.T) store.Store { return myStore(t) })
}
```

`examples/store` is a working JSON-file implementation in forty lines.
`store/memory` is an in-memory one for tests — it is not persistence, since
nothing it holds survives the process.

**Migrating existing data.** There is no tool. A v0.7.0 database holds token
state in a `user_state` table of `(user_id TEXT, tokens REAL, last_used INTEGER
nanoseconds)`; read it with any SQLite client and write it wherever your new
store keeps it. The cost of not migrating is bounded and small: every user
starts at a full burst once, on the first run of the new version.

## `Config` moved to the root

`pace.Config`, `pace.Clock` and `pace.ConfigError` are declared in the root
package now rather than aliased from `pace/limiter`. **If you import
`github.com/jaeminst/pace` and write `pace.Config`, nothing changes.**

What changes is for callers who imported `pace/limiter` directly:

| v0.7.0 | v0.8.0 |
|---|---|
| `limiter.Config` — what you write | `pace.Config` |
| `limiter.Clock` | `pace.Clock` |
| `limiter.ConfigError` | `pace.ConfigError` |
| `limiter.New(cfg) (*Limiter, error)` | `pace.New(cfg) (*Limiter, error)` |

The engine still takes a required-everything vtable, but it is called
`limiter.Spec` as of v0.9.0 — two types named `Config` was a question every
reader had to ask once. `pace.New` is what turns the configuration you write into it. Reach
for `limiter.New` only if you are assembling the pieces yourself.
[ADR 0006](adr/0006-the-root-is-the-composition-root.md) has the reasoning.

## Other removals

- `observe.RequestInfo.Durable` — every request is a plain one now.
- The `pace/sqlite` package, and with it the last non-stdlib dependency but
  `golang.org/x/time`. `go.sum` is two lines.

# Migrating from v0.5.0

The library is one package per concern now, in the style of go-micro. `pace`
stays the front door — `pace.New`, `pace.Config`, `pace.Limiter`, `pace.Client`
and the errors are unchanged — and everything you supply to a Limiter, or that
it reports back, moved to a package that documents it.

Every name still exists. Thirteen were renamed to suit the package qualifier,
which is the whole of the churn:

| Before | After | Import |
|---|---|---|
| `pace.Limit`, `pace.Quota` | `rate.Limit`, `rate.Quota` | `pace/rate` |
| `pace.PerSecond`, `PerMinute`, `PerHour`, `Every`, `Inf` | same names | `pace/rate` |
| `pace.StateStore` | `store.Store` | `pace/store` |
| `pace.BatchStateStore` | `store.BatchStore` | `pace/store` |
| `pace.State`, `pace.UserState` | same names | `pace/store` |
| `pace.SharedQuota` | `shared.Quota` | `pace/shared` |
| `pace.WaitingSharedQuota` | `shared.Waiter` | `pace/shared` |
| `pace.SharedConfig` | `shared.Config` | `pace/shared` |
| `pace.QuotaErrorPolicy` | `shared.ErrorPolicy` | `pace/shared` |
| `pace.QuotaFallbackLocal`, `QuotaDeny`, `QuotaAllow` | `shared.FallbackLocal`, `Deny`, `Allow` | `pace/shared` |
| `pace.ErrQuotaUnavailable` | `shared.ErrUnavailable` | `pace/shared` |
| `pace.TakeRequest`, `pace.Grant` | same names | `pace/shared` |
| `pacetest.QuotaSuite` | `quotatest.QuotaSuite` | `pace/shared/quotatest` |
| `pace.Observer`, `Stats`, `*Info`, `EvictReason`, `JobPhase` | same names | `pace/observe` |
| `pace.QueueConfig` | `queue.Config` | `pace/queue` |
| `pace.DeadJob`, `DeadJobQuery`, `RetryPolicy`, `RetryDecision`, `AmbiguousPolicy` | same names | `pace/queue` |
| `pace.TransportConfig` | `transport.Config` | `pace/transport` |
| `pace.NewTransport` | `transport.New` | `pace/transport` |

`Response` is still there. (It was an alias for `response.Response` until
v0.11.0, where the package folded into `pace/limiter`; there is no `pace.Response`
now — see the v0.11.0 section above.)

The package holding rates is `pace/rate`, not `pace/limit`: one letter from
`pace/limiter` was too close, and `rate.Limit` does not stutter the way
`limit.Limit` did.

There is no `internal/` any more — `bucket`, `registry`, `runner`, `sqlite`,
`breaker` and `urlx` are public. Nothing you import changes because of it;
nothing was reachable from the public API before.

Two behavioural changes came with the release, both breaking:

- **`DeadJobQuery.Before` is a `*DeadJob`, not a `time.Time`.** Pass the last
  job of the previous page. The old cursor carried only the instant, and
  `died_at` is not unique — a replay parks every stranded job in one loop — so
  paging silently skipped every row that shared a page boundary. Passing the
  whole job keeps the two halves of the cursor together.
- **`RetryPolicy.Backoff`, `RetryPolicy.WithDefaults`, `Config.WithDefaults` and
  `AmbiguousPolicy.Resolve` are exported.** They were unexported methods whose
  only caller is now in another package.

The mechanical part is imports and qualifiers. `gofmt -r` does it:

```sh
gofmt -r 'pace.PerMinute -> rate.PerMinute' -w .
gofmt -r 'pace.StateStore -> store.Store' -w .
# ...one per row of the table above
```

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
