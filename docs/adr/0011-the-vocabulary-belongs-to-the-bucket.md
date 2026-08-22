# ADR 0011 — The rate vocabulary belongs to the bucket

**Status:** accepted (v0.2.0)

## Context

`Quota` is a rate and a ceiling. That is also, exactly, what a token bucket is.

It lived in `config` — moved there in v0.12.0 from `limiter`, and to `limiter`
from a `pace/rate` package deleted in v0.9.0. Three homes in four releases is a
signal that the question had not been answered, only relocated.

The tell was in the code. `bucket.Bucket.Quota()` returned two loose numbers,
and both callers immediately reassembled them:

```go
perSec, burst := b.Quota()
q := config.Quota{Rate: config.Limit(perSec), Burst: burst}   // twice
```

It returned numbers because it *had* to: `bucket` cannot name `config.Quota`.
`config` imports `registry`, `registry` imports `bucket`, so a `bucket → config`
edge closes a loop. So the type that describes a bucket could not be used by the
bucket, and every report pace makes about a user's limit — `LimitError`,
`ThrottleInfo`, `Client.Quota`, `shared.TakeRequest` — went through a
hand-written conversion on the way out.

## Decision

**`Limit`, `Quota`, `Inf`, `Finite` and the constructors `PerSecond`,
`PerMinute`, `PerHour` and `Every` move to `pace/bucket`. `config.Quota` is
deleted.**

`Quota` cannot travel alone: `Quota.Rate` is a `Limit`, so putting `Quota` in
`bucket` with `Limit` in `config` recreates the same cycle. They move together
or not at all.

`bucket` imports nothing of pace's and keeps it that way — the moved file was
stdlib-only. That is what makes the move legal: `config → bucket` is a new edge
pointing down, and `registry → bucket` was already there.

`registry.Spec.QuotaFor` keeps returning `(float64, int)`. It could name
`bucket.Quota` now — `registry` imports `bucket` — but the de-typing is
[ADR 0007](0007-contracts-carry-numbers-not-types.md)'s and it costs nothing to
leave in place.

> **Amended.** "Costs nothing" was wrong. It cost a round trip: a
> `bucket.Quota` taken apart into two numbers and rebuilt into a `bucket.Quota`,
> on the create path and again on the reload path. It also misattributed the
> reason — the de-typing was
> [ADR 0006](0006-the-root-is-the-composition-root.md)'s, so that `registry`
> need not import the vocabulary package, and *this* ADR is what removed that
> constraint by moving the vocabulary below `registry`. Reverted in
> [ADR 0012](0012-one-hook-holds-the-quota.md); ADR 0007's contract-package
> argument is untouched.

### What it buys

`Bucket.Quota()` returns a `Quota`. The two conversions are gone, and so is
`limiter.quotaOf`, which had become `return u.Bucket().Quota()` — a one-line
forward whose only remaining content was its doc comment. That comment said *"the
bucket is the source of truth, not the configuration"*, and it now sits on
`Bucket.Quota`, which is where the value actually comes from.

The caller-facing quota and the enforced quota are one type. Before, the type a
caller wrote in `Config.QuotaFor` and the numbers a bucket reported were related
only by two conversions someone had to keep correct.

### What it costs

A `Config` literal names two packages:

```go
config.Config{BaseURL: "…", Rate: bucket.PerMinute(60), Burst: 10}
```

v0.12.0 moved the vocabulary *into* `config` specifically to avoid that, and
[ADR 0009](0009-config-limiter-client.md) recorded the reason: *"writing one
should not mean naming a second package in the middle of a Config literal."*
That sentence is now overruled, and it is worth being explicit that it was a real
argument rather than an oversight — this is a trade, not a correction.

What tips it: the alternative put the cost on every throttle report instead of on
one import line, and it split one concept across two packages so that the halves
could disagree. An import is paid once per file; a conversion is paid at every
site that reports a limit, forever, and each one is a place to get it wrong.

`bucket` also stops being a package a caller only reads. Its doc used to say the
lower-level packages are "public because they are worth reading, not because you
are expected to assemble one". That still holds for `registry`, `gate`, `breaker`
and `urlx`; `bucket` is now something a caller imports on purpose, and its
package doc leads with the vocabulary rather than with the arithmetic.

### The rule this is the third application of

ADR 0007 deleted `pace/rate` because five packages compiled a leaf vocabulary
package, two of them contract packages a third party implements a backend
against. The test it left behind:

> The vocabulary may live wherever it reads best, provided no package implemented
> against from outside has to compile it.

`store`, `shared` and `observe` still carry plain `float64` and `int` and import
nothing of pace's. `bucket` is imported by `config`, `registry`, `limiter` and
`client` — all of them pace's own. The test passes, and it is the same test that
allowed the move to `limiter` and then to `config`. What changed is the answer to
"where does it read best", and the code was saying `bucket` in two places.

## Consequences

**Every caller adds an import.** `bucket.PerMinute(60)` where it was
`config.PerMinute(60)`. A compile error, and mechanical.

**`config` is three names**: `Config`, `Clock`, `Error`. It is the package that
holds what a caller writes and validates it, and nothing else.

**`limiter.quotaOf` is gone**, inlined to `u.Bucket().Quota()` at four sites.

**Two `finite`s met in one package.** `bucket` had an unexported
`finite(float64) float64` — NaN → 0, ±Inf → 0 or MaxFloat64 — and the vocabulary
brought `Finite(Limit) Limit`, which maps ±Inf onto `Inf` and deliberately lets
NaN through for an upstream `> 0` test to catch. They differ, and two names one
capital apart is a mistake this repo has rejected twice. The unexported one is
`usableRate` now, documented as the floor beneath `Finite` rather than a variant
of it.

## Alternatives considered

**Move only the `Quota` struct, with `Rate float64`.** No cycle, and no
vocabulary either: a caller would write `bucket.Quota{Rate: float64(...)}` and
lose `PerMinute`. It also leaves `Limit` in `config` describing a field of a type
in `bucket`.

**Cut the `config → registry` edge instead** — it exists only for
`registry.DefaultShards`, one constant — and then move `Quota` alone with `Rate`
staying `config.Limit`. That removes one cycle and creates another:
`config.Config.QuotaFor` returns `bucket.Quota`, so `config → bucket` and
`bucket → config` both exist. The vocabulary and the struct are one unit however
the graph is cut.

**Leave it in `config`.** Four lines of conversion, and the trade above run the
other way. Defensible, and it is what the previous release chose; the code kept
pointing the other direction.
