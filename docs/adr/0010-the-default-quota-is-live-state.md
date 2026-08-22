# ADR 0010 — The default quota is state, not a field of the Config

**Status:** superseded by
[ADR 0012](0012-one-hook-holds-the-quota.md) (v0.14.0)

> **Superseded.** `SetDefaultQuota` is deleted. The requirement this ADR was
> written for — a rate an operator can change in a running process — still
> stands and is still met: swap what `Config.QuotaFor` reads, then reload. What
> is gone is the *separate* handle for the population-wide default, which was a
> second way to say something `QuotaFor` could already say, and with it the
> second copy of Resolve's normalisation that section "The setter re-checks what
> Resolve checked" below was uneasy about.
>
> The explicit-apply contract this ADR argued for is unchanged and now belongs
> to the one hook. Read the reasoning below as still correct about *when* a
> quota change should land; only the handle it lands through has moved.

## Context

A rate limiter that cannot be re-rated while it runs is a rate limiter you
restart to change. pace was half of one.

**Per-user rates were already adjustable**, and adjustable well: change what
`config.Config.QuotaFor` reads, call `ReloadQuotas`, and every live bucket picks
up the new quota keeping the tokens it had accrued. That path reads the clock
per user rather than once for the walk, because a captured instant rewound
buckets touched after it and a rewound interval refills twice — a regression
fixed in v0.9.0 and still tested.

**The default was not.** `Config.Rate` and `Config.Burst` were a value copy
taken at `New` (`limiter.go`), so raising every user's ceiling meant restarting
the process or routing all of them through `QuotaFor` — which makes the required
`Config.Rate` a value you set and then shadow.

And the documented way to do the half that worked **was a data race.** `QuotaFor`
is called from request goroutines, one per user whose bucket is being created.
The README and `ExamplePool_ReloadQuotas` both wrote a plain
`map[string]config.Quota` from the caller's goroutine while the closure read it.
Nothing in the docs said it had to be safe for concurrent use; what they did say
was "keep it to a map lookup". The test file had it right — `peruserquota_test.go`
guards its tiers map with a mutex — so the tests and the examples disagreed, and
the examples were the ones people copy.

## Decision

**The default quota is live state the Limiter owns. Per-user quotas stay a
callback the caller owns.**

```go
func (l *Limiter) SetDefaultQuota(q config.Quota) error
func (l *Limiter) DefaultQuota() config.Quota
func (l *Limiter) ReloadQuota(userID string) bool
```

The asymmetry is the decision, and it comes from eviction. A default is one
value: the Limiter can hold it with no bookkeeping. Per-user overrides are keyed
by user, and a Limiter holding those would have to choose between two bad
outcomes — keep them past eviction, and the map grows without bound, defeating
the GC that exists to stop exactly that; drop them with the user, and a
`SetQuota("alice", …)` silently expires ten minutes later. `QuotaFor` has neither
problem *because* it is stateless on our side: we ask, the caller answers.

### One atomic pointer, not two numbers, not a mutex

`Limiter.quota` is an `atomic.Pointer[config.Quota]`.

The property that matters is not atomicity of each field but **coherence of the
pair**. Resolving a quota needs the rate and the burst to have come from the same
`SetDefaultQuota`; two `atomic.Uint64`s would let a reader combine halves of two.
This is the same argument `registry` already makes about reading a user's tokens
and last-used stamp as one snapshot, and it is why that code builds a `Snapshot`
instead of reading two fields.

It follows `hooks atomic.Pointer[hooks]`, already on the same struct — with the
opposite invariant, worth saying because the two sit three lines apart: `hooks`
is deliberately nil in production, `quota` is seeded by `New` before anything can
load it.

**It must stay a whole-value store.** A `SetRate` — or a `SetDefaultQuota` that
read a zero field as "leave that one alone" — is read-modify-write, and two
concurrent callers lose an update. Taking a whole `config.Quota` is what keeps a
plain `Store` honest instead of a compare-and-swap loop.

`SetDefaultQuota` re-applies the normalisation `Config.Resolve` performs —
`Finite`, `Burst <= 0 → 1`, and a `*config.Error` for a Rate that is zero,
negative or NaN. The value arrives after `Resolve` has run and nothing downstream
re-checks it, so this is the only thing between a caller and a bucket that
refuses every request forever.

### Applying is explicit, and the walk does not snapshot the default

A change reaches a user with no bucket at once. A user already in memory keeps
what they have until `ReloadQuotas` or `ReloadQuota`. That is the same step a
`QuotaFor` change already needed, for the same reason: applying it means walking
the population.

The reload walk reads the default **live, per user** — it does not capture it
once. The per-user clock read is *not* the argument for this, and it is worth
being clear about that: the clock is read per user because a captured instant was
*wrong*, and a captured default is merely different. The arguments are:

- The walk already tears in two other dimensions. Membership: it is a series of
  per-shard snapshots, and a user created in an already-visited shard is simply
  not reloaded. And the per-user source tears too — the caller's map behind
  `QuotaFor` can be mutated mid-walk and nothing holds it still. Giving the
  default a single-instant guarantee that the thing it is a fallback *for* does
  not have is an inconsistency we would then have to document.
- Snapshotting makes the walk internally consistent and **externally**
  inconsistent: a bucket created concurrently on the cold path reads the live
  default, so two different defaults would be in force at the same instant.

What the caller gets instead is an **ordering contract**: `SetDefaultQuota` then
`ReloadQuotas`, from one goroutine, applies uniformly — the atomic store is a
release. Racing them can leave a population permanently split, because nothing
re-runs the walk. There is no eventual convergence here, only the order you
impose, and that is stated on both methods.

### `QuotaFor` must be safe for concurrent use

Now documented as a contract on the field, in the README, and in the example.
`TestQuotaForIsCalledConcurrently` is the guard.

It has to be a test rather than the example. An `Example` with an `// Output:`
block cannot contain real concurrency and stay deterministic, and the race
detector has nothing to report about a program with one goroutine. So the
example is fixed for pedagogy — it swaps a whole map behind an `atomic.Pointer`
— and the guard parks eight cold-path entrants on a barrier, with one shard so
they contend, while a writer swaps the table fifty times. Replacing the pointer
with the plain map the example used to teach makes it fail under `-race`.

## Consequences

**Two defects fixed that a run-time change makes reachable.**

`bucket.Bucket` now holds its rate and ceiling as one immutable pair behind an
`atomic.Pointer`, and `Bucket.Quota()` returns both in one load. `rate.Limiter`
reports them through two separately locked methods, so `quotaOf` and
`reportBucketTokens` — which read both — could return a pair nobody configured,
and that pair is what `LimitError`, `ThrottleInfo`, `Client.Quota` and
`shared.TakeRequest` are built from. A backend sizing its bucket from
`TakeRequest` could be told to enforce a quota that never existed. The engine's
own comment claimed this was impossible: "Everything else comes from one place so
the five fields cannot drift apart across the seven sites that report a
throttle." It is true now. It is also cheaper — one atomic load replaces two
mutex acquisitions on the path every throttled request takes.

`Gate.Acquire`'s polling loop re-reads the quota per round instead of carrying
the one it was called with. That loop can run for minutes; the shadow bucket it
reserves against already reflected a change while the `TakeRequest` it sent did
not, which made `admit.go`'s stated invariant — "the shadow and the shared bucket
are configured with the same rate and burst" — false during any reload. This cost
`Gate.Acquire` and `Gate.Allow` their `rateLimit, burst` parameters, a breaking
change taken deliberately: they have the bucket, and two sources for one number
in one function is how they came to disagree.

**Three documented rather than fixed.**

- `Gate.wait` cannot re-read. It is one blocking call the backend owns, so a
  caller parked there finishes under the quota in force when it arrived.
- `bucket.Wait` arms one timer and does not re-arm it, so a raise does not
  shorten a wait already under way. This is upstream-sanctioned —
  `rate.SetLimitAt` says as much — and fixing it means pace owning the wait loop,
  which is its own release and interacts with `Cancel`'s semantics below.
  **Explicitly out of scope**, said once on `ReloadQuotas`.
- Moving a user from `config.Inf` back to a finite rate hands them a full burst,
  because the bucket credits the elapsed interval at the outgoing infinite rate.
  Previously unreachable in practice; `SetDefaultQuota` makes "unlimit for five
  minutes, then restore" one line, so it is now a warning in the README.

**One promise sharpened.** `Reservation.Cancel` said it returns the token unless
the delay has elapsed. A reservation that needed no wait has a delay of zero, so
it is *already* at its deadline — whether cancelling it refunds anything depends
on whether the clock advanced between the two calls, which at Windows timer
granularity it often has not. The docs now say a caller cannot rely on it either
way, and a test pins the deterministic half by advancing a fake clock. Every
other reservation test freezes the clock, which tests the refund arithmetic
correctly and hides *when* a refund happens.

**Not taken: observability.** There is no counter or hook for a reload. It was
considered — `Stats` may gain fields under the compatibility policy, and
`observe` documents the same — and left out because nothing asked for it. Noting
it so it is not rediscovered as an oversight.
