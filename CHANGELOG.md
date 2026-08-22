# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.12.0]

Three packages, one job each. The repository root declares nothing.

```go
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

| | v0.11.0 | v0.12.0 |
|---|--:|--:|
| Packages (incl. examples) | 16 | **18** |
| `limiter` non-test lines | 1,988 | **1,271** |
| `limiter.Spec` fields | 14 | **10** |
| Declarations in the root | 4 | **0** |
| Coverage | 97.0% | **97.0%** |

`config` holds everything a caller configures and the vocabulary they write it
in. `limiter` is the rate limiter and only that — it no longer imports
`net/http` or `urlx`. `client` creates and manages clients and owns the request
path. `docs/MIGRATION.md` has the mapping.

### The layout v0.11.0 was looking for

v0.11.0 split the engine from the request path and reverted it a day later. The
reason was exactly one thing: the HTTP half went into the **root**, and
`lim.Client("alice")` cannot return a root type from a method on
`limiter.Limiter` without `limiter` importing the root.

That constraint is on *where the return type lives*, not on where the request
path lives. Give the HTTP half its own package and it evaporates —
`Pool.Client` returns a `*client.Client`, same package, no cycle. So
`limiter/api.go` and the 10-field `Spec` come back from the reverted commit
verbatim rather than being rewritten.

[ADR 0008](docs/adr/0008-the-root-re-exports-nothing.md) had claimed "the only
two coherent layouts" were the root-facade and one giant package. There was a
third. That is the one claim v0.12.0 falsifies; the rest of that ADR gets
stronger, because the root now declares nothing at all.

### What could not move, checked rather than assumed

- **`limiter.Spec` cannot live in `config`.** `Spec.Quota` returns a
  `config.Quota`, so `limiter` imports `config`, so `config` cannot import
  `limiter`. `func (Config) Spec() limiter.Spec` is the obvious API and the one
  Go forbids; `client.New` performs the translation.
- **`registry.Spec` cannot either** — four of its callbacks take or return
  `registry.Snapshot`/`Eviction`. Its `QuotaFor func(string) (float64, int)` is
  written in bare numbers precisely to avoid this.
- **An interface breaks neither.** They are data cycles. ADR 0007 already wrote
  that argument down and ADR 0009 cites it rather than restating it.

### `config` is not `pace/rate` again

The fair objection: v0.9.0 deleted a leaf package holding exactly this
vocabulary, and here it is under a new name.

ADR 0007's own numbers answer it. `rate` existed "because **five** other
packages shared it", two of them contract packages a third party implements a
backend against — so a Redis author compiled `pace/rate` to read two numbers out
of a request. That ADR de-typed those fields to `float64` and `int`, and they
still are. `config` is imported by `limiter` and `client`, both of them pace's
own, and by nothing a third party writes.

The test worth keeping: **the vocabulary may live wherever it reads best,
provided no package implemented against from outside has to compile it.**

### `Spec` lives in `config` too

`limiter.Spec` is `config.Spec`. Both configuration types are in one package
now, with `Config.Spec` as the only translation between them, and `client.New`
shrinks from a ten-field literal to `limiter.New(cfg.Spec())`.

This corrects a claim ADR 0009 made and got wrong. `Spec`'s ten fields name
`config`, `observe`, `shared`, `store` and stdlib — **nothing from `limiter`** —
so moving the type needs no new import and closes no loop. What is genuinely
forbidden is `func (Config) Spec() limiter.Spec`, a *method* naming a type in
the package that imports `config`; the ADR conflated the two and bolded the
false one. Correcting it makes the method it said was impossible ordinary:

```go
func (cfg Config) Spec() Spec
```

`Spec.validate` is exported `Spec.Validate`, since `limiter.New` calls it across
a boundary — which is what a caller assembling the pieces by hand wants anyway.
The panics say `config:` and name `Spec.Quota` rather than a bare `Quota`, which
would have been ambiguous next to `Config.QuotaFor`.

The cost, stated plainly: the vtable is no longer declared by the package that
consumes it, which is a change to what ADR 0006 described. `registry.Spec` and
`gate.Spec` keep the old arrangement — the rule that decides is whether the
vtable names any of its consumer's own types. `registry.Spec` does, in four
callbacks, and cannot move for that reason.

Side effect: `limiter` no longer imports `shared` either, since `Spec.Shared`
went with the type.

### Two new names, and why not three

`client.New` needed three unexported methods on the old root `Config`. They
collapse to two:

```go
func (cfg Config) Resolve() (Config, error)      // validate, then default
func (cfg Config) Quota(userID string) Quota     // becomes Spec.Quota
```

Not exported `Validate` and `WithDefaults` as peers, because that publishes an
ordering contract as API: `Quota` called on an unresolved `Config` returns
`{0, 0}` — a bucket that refuses every request, silently, forever. Behind
`Resolve` the ordering stays private. It also means `config`'s own tests check
validation without building an engine, so `config` has no test-time dependency
on `client` at all.

The method is `Quota`, not `QuotaOf`, next to the `Config.QuotaFor` field:
`QuotaOf` beside `QuotaFor` is a one-syllable difference no reviewer catches.
(`QuotaFor` itself is unavailable — Go forbids a field and a method of one name.)

### The root keeps one file

`doc.go`: `package pace`, zero declarations. `import "github.com/jaeminst/pace"`
still compiles, pkg.go.dev keeps a landing page, and the Go Reference badge keeps
resolving. Deleting the file would drop the directory from `go list ./...` and
with it from `-coverpkg`, from `go vet ./...` and from the fuzz sweep — one doc
file is cheaper than four exclusions to remember.

### The fuzz job would have gone green having fuzzed nothing

`FuzzLimitString` followed `Limit` to `config`; `FuzzRetryAfter` followed
`Response` to `client`. That is the third release running in which a fuzz target
moved, and this time the stale path would **not** have failed:

```
$ go test ./limiter/ -run=NONE -fuzz='^FuzzNoSuchTarget$'
PASS   ok  github.com/jaeminst/pace/limiter   0.959s   (exit 0)
```

The previous two breaks were loud only because the old package had been deleted
outright (`[setup failed]`). A target that merely *moves* leaves the package
there, so `set -euo pipefail` sees success. Both `make fuzz` and the CI job now
derive the matrix from `go test -list='^Fuzz'` and fail if the count is zero, so
a moved target cannot be skipped. The artifact path is `**/testdata/fuzz` rather
than four named directories, one of which had been wrong.

### Also

- **11 doc comments were emitted twice and 2 were detached** from the
  declaration they document — damage from the scripted file moves in v0.11.0's
  two commits. Neither `godot`, `go vet` nor `golangci-lint` says anything about
  either shape.
- `pace.ConfigError` is `config.Error`. Not cosmetic: `config.ConfigError` trips
  revive's stutter check, which the repo had never hit because every other
  offender is saved by the length test (`config.Config`, `client.Client`,
  `store.Store` all short-circuit on `len(name) <= len(pkg)`). `net/url.Error`
  is the precedent.
- `ExampleConfig_quotaFor` is `ExampleConfig_Quota`. A **lowercase** example
  suffix is never validated by `go vet` at all, so the old name had been naming
  an unexported method with nothing complaining. Examples now: 1 in `config`,
  9 in `client`, 1 in `limiter`, 0 orphans.
- `Client.Evict` keeps the lifetime binding it gained in v0.11.0.
- Test fixtures are duplicated between `limiter_test` and `client_test`
  deliberately. A shared `pacetest` package would be filtered out of
  `-coverpkg` by the `grep -v test$` in the coverage command, and go silently
  unmeasured.

## [0.11.0]

Every name in this library is declared exactly once. The root holds `Config`,
`Clock`, `ConfigError` and `New`, and re-exports nothing.

| | v0.10.0 | v0.11.0 |
|---|--:|--:|
| Names re-exported from the root | 10 | **0** |
| Type aliases in the module | 4 | **0** |
| Packages | 17 | **16** |
| Root exported names | 14 | **4** |
| Coverage | 96.8% | **97.0%** |

**This breaks every caller**, which no release since v0.8.0 has done. You now
write two imports and spell the vocabulary `limiter.`:

```go
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    limiter.PerMinute(60),   // was pace.PerMinute
})              // *limiter.Limiter — the engine's own type, not a wrapper
```

Because the deleted names were *aliases*, the change is spelling-only and never
type-identity: code already written as `limiter.PerMinute(60)` needs no edit,
and every break is a compile error. `docs/MIGRATION.md` has the ten-row table.

### An alias documented nothing

Go renders a type alias as a single line with no methods and no fields.
`go doc pace Limiter` printed `type Limiter = limiter.Limiter` and stopped —
and the caller who never leaves `pace`, the one the re-export existed for, was
exactly the caller who could not find out what a `Limiter` does.

This is not a new discovery. v0.6.0 put sixty-three aliases in the root and
v0.7.0 spent a release unwinding them for this reason. The four biggest
survived that unwinding: `Limiter`, `Client`, `Request`, `Response`.

**An alias is for a type whose owner is elsewhere. It is not a way to publish a
name in two places.** [ADR 0008](docs/adr/0008-the-root-re-exports-nothing.md).

### The other repair, and why it was not taken

The request path can be hoisted *up* to the root instead, so those four names
are declared there. That works — and it costs a wrapper struct, a root `Limiter`
holding the engine plus five HTTP fields, with four one-line forwarding methods,
because `lim.Client()` has to keep working and a method on `limiter.Limiter`
cannot return a root type. Trading four undocumented aliases for one wrapper and
four forwards is not obviously a trade.

What cannot be done at all, checked file by file rather than assumed: `rate.go`
cannot move to the root, because `Spec.Quota` returns a `Quota` and
`LimitError.Limit` is a `Limit` — a *data* cycle. `lifecycle.go` cannot move,
because Go forbids declaring a method on another package's type. **An interface
does not rescue either.** An interface breaks a *behaviour* cycle; you cannot
divide by an interface. ADR 0007 had already written that argument down, and the
new ADR cites it rather than restating it.

### `pace/response` is gone

It was a package for one reason: the root aliased `Response` and the engine
returned it, so neither could import the other. With no alias the reason
evaporates. It folds into `limiter/response.go`, and `response.New` goes with
it — a public constructor for a type a caller only ever *receives*. A `Response`
is a report, not something to assemble.

### Also

- **`Client.Evict` now runs under the Limiter's lifetime as well as the
  caller's**, so closing the Limiter ends an eviction still waiting on a wedged
  backend. Previously it took the caller's context raw and a `Close` could not
  reach it. This is the one behaviour change in the release, and it is here
  rather than buried because a refactor that smuggles one is worse than one that
  states it.
- **The fuzz job has been broken on `main` since `response/` was deleted.**
  `make fuzz` and the CI step both ran `go test ./response/`, which fails
  `[setup failed]` under `set -euo pipefail`. Nothing caught it: `make ci` does
  not include `fuzz`, and in CI the job is separate from `test`. Both now run
  `FuzzRetryAfter` against `./limiter/`.
- `facade_test.go` is `new_test.go`. Most of it pinned aliases in both
  directions and now asserts nothing; what survives is
  `var _ func(pace.Config) (*limiter.Limiter, error) = pace.New`, which is the
  entire boundary, plus the four tests of what `New` wired.
  `TestASentinelMatchesWhatTheLimiterReturns` is deleted — there is no root
  sentinel left that could disagree with the engine's.
- Ten of eleven examples moved to `limiter/`, following the types they name.
  Left at the root they would attach to nothing and render nowhere, and **`go
  vet` does not catch it here**: `checkExampleName` resolves against the test
  package's *imports*, so a stranded `ExampleClient_Get` in `pace_test` resolves
  happily to `limiter.Client`. A lowercase suffix is never checked at all. The
  check that works is a `go/doc` pass asserting every `ExampleX` has `X`
  declared in a non-test file of the same directory: 1 at the root, 10 in
  `limiter`, 0 orphans.
- `statestore_test.go` is `persistence_test.go`, and `StateStore` — renamed
  `store.Store` in v0.8.0 — left six test names with it. The tests did not move
  to `store/`, which was asked for twice: all thirteen assert what *pace* does
  with a backend, and `store` is 61 lines of interface declarations whose
  contract is already executable as `store/storetest`. Tests of a contract
  belong with the contract; tests of a policy over it belong with the policy.
- `_internal_test.go` is gone as a suffix. Whether a file is `package limiter`
  or `package limiter_test` is on its first line, so the name carried nothing —
  and there are five of the former now.

## [0.10.0]

Same-thing-twice removal, after a three-way audit — configuration, package
structure, logic and prose.

The headline finding was that almost nothing was duplicated *configuration*. The
logger travels four layers, the clock is declared five times, `StoreTimeout`
three times under two names — and with one exception every one of those is a
straight copy that never once differs from the layer above. That is wiring, and
it reads the same either way. So the work went where the duplication was real.

| | v0.9.0 | v0.10.0 |
|---|--:|--:|
| Packages | 18 | **17** |
| Coverage | 96.3% | **96.8%** |
| Unresolved godoc links | 8 | **0** |

### Eight references named things that do not exist

Found by extracting every `[Ident]` from every Go comment and resolving it
against the package it sits in. Seven resolved to nothing — `[SharedQuota]`,
`[QuotaFallbackLocal]`, `[TransportConfig.ResponseHeaderTimeout]` and four more.

The eighth is the one worth knowing about. `shared.Config`'s doc said the
timeout was "much shorter than `[Config.StoreTimeout]`" — and `Config` does
exist in `shared`, so nothing flagged it, but `shared.Config` has no such field.
The sentence was reaching for the root's.

Prose naming deleted identifiers went with them. `runner`, dropped in v0.8.0,
was still cited in `registry`'s package doc as the precedent for its own design.

### `shared.Quota` is `shared.Backend`; the vtables are `Spec`

`Quota` meant nine things — an interface a third party implements, a struct of a
rate and a burst, three config fields, a method, a factory. `shared.Backend` is
what the interface is: a token supply every replica consults. `pace.Quota`, the
`{Rate, Burst}` pair, keeps the name, because it is the one that genuinely is a
quota.

`gate.Config` and `registry.Config` are `Spec`, which is what v0.9.0 should have
done to all three rather than only `limiter`'s. Six types named `Config` in two
categories, with nothing at the call site separating them, was the problem. The
rule needs no paragraph now: **options are `Config`, vtables are `Spec`.**

### `persist` is gone

Seven exported names, every one of which existed so that one caller could wire
one value. The reason its package doc gave for the split — keeping `registry`
from seeing a `store` — survives intact, because it was never the package
boundary that did that: `limiter` imports both, so the wall stands wherever the
adapter lives.

Its vtable ceremony went with it. `persist.New`'s two panics existed because a
third party could hand a public constructor a zero value, and
`limiter.Spec.validate` already rejects both before any of it is reached.

### `gate` has the test the other vtables had

496 non-test lines against 45 test lines, with everything else about it
exercised indirectly through a 1,100-line file in `limiter`. It has a zero-value
test now, nine cases, and `gate.New` goes from partial coverage to 100%.

The test says in its own doc that none of the panics it drives is reachable
through `pace.New`. That is the point: they are the contract for a caller
assembling the pieces directly, so a test is the only thing that holds them to
it.

### Logic said once

- The retry-delay rule — the backend's `RetryAfter`, or `FallbackDelay` when it
  gave none — lived in three places, one of them in another package. It is
  `gate.RetryDelay`.
- The `shared.TakeRequest` literal appeared twice, so `Take` and `Wait` could
  have come to ask the backend different questions. It is `gate.request`.
- `registry` built the "tokens left, last seen" pair at six sites, and `Evict`
  built it twice for the same user — where `lastUsed` is an atomic load, so the
  two copies could legally disagree.
- `send` and `Stream` each spelled out the same twelve-line observation block.
  That one had already cost something: the comment still in `Stream` records
  that leaving the counting out of one copy made `Stats.Requests` and
  `Stats.Errors` count different populations.

**`StoreTimeout` is applied in one place.** It used to wrap the whole
`GetOrCreate` in `allow` and `Reserve`, not at all in `acquire`, and again
underneath all three inside the persistence adapter. An audit called the
`acquire` case a missing bound; checked, and it is not — the inner wrap is what
bounds the store call, so `store.Store`'s documented promise held on every path.
What the outer wraps added was a *store* timeout around lock acquisition. Both
are gone.

### Files and fixtures

`limiter.go` was assembling a Limiter, minting Clients, and being one for the
rest of its life; the third is `lifecycle.go`. `request.go` was a builder, a
dispatch and a streaming path; the last is `stream.go`. `quota.go` held three
unrelated things and is gone.

Six test helpers built a Limiter and all six ended with the same eight lines.
There is one `build`. One fake clock instead of two. `registry`'s bench config
changes the two fields it needs instead of repeating twelve.

### Also

- `gate.ErrUnsatisfiable` and both `SuiteOption`s are unexported. The first was
  produced at one site and matched at none. The second could not be constructed
  by a caller at all, because its parameter type is unexported.
- Reconsidered and left alone: the second `maxShards` clamp inside
  `roundUpPowerOfTwo`. It is unreachable through `New`, and it is what makes the
  helper total on its own.
- Left alone with the reason in the commit: `evictReasonOf`, which ADR 0007 pays
  for; the `DropAll`/`sweepInPlace` merge, which would cost a documented 57KB
  per sweep; the full `gate.Allow`/`Acquire` merge, whose doc gives the correct
  reason they differ; and the sixteen inline test servers, where a helper hiding
  three obvious lines makes a test harder to read, not easier.

## [0.9.0]

Tests now live in the package whose behaviour they assert, and two names stopped
lying about what they are.

It started with one observation about `limiter/transport_test.go` and turned into
an audit of all eighteen test files in that package. The finding underneath: **14
of the 16 external test files never named `limiter.New` or `limiter.Config`.**
They all built a Limiter with `pace.New(pace.Config{…})`. `limiter/`'s test suite
was, structurally, the root's suite living in the engine's directory.

| | v0.8.0 | v0.9.0 |
|---|--:|--:|
| Packages | 19 | **18** |
| Direct dependencies | 1 | 1 |
| Coverage | 96.3% | **96.3%** |
| Files in `limiter/` named after another package | 4 | **0** |

### Tests moved to the package they are about

**`urlx` had no unit test file at all** — one fuzz target, nothing else. Six
tests in `limiter/url_test.go` asserted things about `urlx.Build`: the seam
between base and path, the inline query that must not be escaped, the query
merge, and the guard against a relative path running into the authority. Each
stood up an httptest server and a Limiter to watch a pure string function. They
are direct calls now, and the move paid for itself: every `Validate` refusal is
checked by the message a caller sees, and `Build`'s extra-query branch — which
`FuzzBuild` never reaches, because it always passes nil — is covered.

Checked that they are not decoration: reverting `Build`'s separator to plain
concatenation, the request-forgery defect fuzzing originally found, fails the
guard and three seam rows.

**Five `response` tests got stronger by moving.**
`TestRetryAfterHTTPDate` had no clock to inject through a Limiter, so it asserted
`d > 55 minutes && d <= 1 hour`; `response.New` takes a `now` func, so it asserts
exactly one hour. `TestOK` carried a comment saying 1xx could not be tested that
way — net/http swallows an informational response — and 1xx is two more rows now.

**`shared` had no test file either.** It has one, for the one piece of behaviour
it owns.

### Three godoc examples rendered nowhere

`ExampleObserver` and `ExampleResponse_RetryAfter` lived in `limiter/`, which
declares neither `Observer` nor `Response`. go/doc attaches an example by
matching its name against the documented package's own symbols, so both compiled,
ran under `go test`, and appeared in no documentation at all.

`go vet` does not catch this, which is worth knowing: its example check resolves
the name against imported packages too, so `ExampleObserver` found
`observe.Observer` and was accepted. Verified by driving `go/doc` directly over
every package and listing which examples come back attached.

`ExampleConfig_quotaFor` was the third and was worse than orphaned: it rendered,
attached to `limiter.Config`, a type with no `QuotaFor` field. All three are
filed under identifiers that exist, and go/doc now reports zero unattached
examples anywhere.

### `pace/rate` is gone — one import configures a Limiter

```go
import "github.com/jaeminst/pace"

pace.New(pace.Config{Rate: pace.PerMinute(60)})
```

Absorbing it needed three cycles broken, because `observe`, `shared` and `gate`
all named `rate`'s types. Measured, that was **two struct fields and three
function signatures** — `observe.ThrottleInfo.Limit` becomes a `float64`,
`shared.TakeRequest.Quota` becomes `Rate float64` + `Burst int`, and `gate` takes
the numbers. `gate.Enabled` is deleted rather than de-typed: it was
`q.Rate != rate.Inf`, and with `Inf` in `limiter` the caller compares directly.

The two contract fields lose their type permanently — both freeze at v1. What
`observe` loses is `Limit.String`'s `"60/min"`; what `shared` loses is the
nesting. What both gain is not compiling anything of pace's to read two numbers,
which for a package a third party implements is the point. See
[ADR 0007](docs/adr/0007-contracts-carry-numbers-not-types.md), including the
alternative — merging into `bucket`, which needs no de-typing — and why it was
worse.

### `limiter.Config` is `limiter.Spec`

Two types named `Config`, eleven of whose field names are identical, one written
by a caller and one taken by the engine. `registry`, `gate` and `persist` keep
`Config`: they are vtables a caller never meets. `limiter` is the one someone
might import beside the root. ADR 0006 carries the amendment.

### Also

- `limiter/registry.go` and `limiter/response.go` are dissolved, the way
  `limiter/gate.go` was in v0.8.0 — neither could move to the package it was
  named after without closing a cycle, and each piece went to the file that owns
  what it produces. `limiter/` is thirteen files, all named for limiter concerns.
- `beforeQuotaTake` had no setter in `export_test.go`, so it was nil for the life
  of every process and `fireBeforeQuotaTake` was a permanent no-op. Removed; a
  hook nothing can install reads as a seam and is not one.
- Three test files were renamed for their contents: `config_test.go` held no
  Config test, `quota_test.go` held nothing about a Quota, and `response_test.go`
  is `body_test.go` now that the Response tests have left.
- No new benchmark baseline. The v0.9.0 run measured 3–20% slower than v0.8.0's
  across every benchmark, including `bucket` and `registry`, which have had no
  commit since — with allocation counts identical to the byte. That is the
  machine, not the code, and recording it would have made the next comparison
  read as an improvement that never happened. `docs/bench/README.md` says so.
- Doc links that v0.7.0 and v0.8.0 left dangling: `[SharedConfig.Timeout]` and
  `[TransportConfig.ResponseHeaderTimeout]` name types that stopped existing in
  v0.7.0, and every contract package still told callers to supply their
  implementation as `limiter.Config.Store` or `.Shared` or `.Observer` or
  `.Transport` — fields that moved to the root in v0.8.0.

## [0.8.0]

pace is a rate limiter. This release removes everything that was not.

6,670 lines of Go were deleted against 1,566 added — a net 5,104, or 26% of the
repository — and the module now depends on `golang.org/x/time` and nothing else.
`go.sum` is two lines, down from 52.

| | v0.7.0 | v0.8.0 |
|---|--:|--:|
| Go source | 19,369 lines | **14,265** |
| …excluding tests | 7,203 | **5,400** |
| Direct dependencies | 2 | **1** |
| Indirect dependencies | 8 | **0** |
| Packages | 20 | 19 |
| Exported declarations | 235 | **175** |
| Coverage | 93.5% | **96.3%** |

The reasoning is in two ADRs:
[0005 — pace ships contracts, not backends](docs/adr/0005-pace-ships-contracts-not-backends.md)
and [0006 — the root is the composition root](docs/adr/0006-the-root-is-the-composition-root.md).
[MIGRATION.md](docs/MIGRATION.md) has the upgrade path, including the one break
that has no replacement.

### The durable request queue is gone

`Client.Durable`, `Limiter.DeadJobs`, `Config.Queue`, the `pace/queue` and
`pace/runner` packages, four error sentinels, and `observe`'s four job types.

**There is no replacement. If you use durable requests, stay on v0.7.0.**

It went because its only implementation was going, and because what it needed
next was worse than the feature. The queue's correctness came from SQL
semantics: `Claim` is one conditional `UPDATE`, and that single statement is the
whole reason two workers racing for a job cannot both win. Keeping the feature
without SQLite meant publishing an eleven-method interface whose contract is
cross-process atomicity, with nothing implementing it and nothing to check an
implementation against — the opposite of what `shared/quotatest` exists to do.

A contract nobody can be expected to satisfy correctly is worse than no feature.

### The SQLite backend is gone

`Config.DBPath` and the `pace/sqlite` package. `Config.Store` is the only way to
persist now, and without one a Limiter is in-memory: a restart starts every user
at a full burst.

This is the doctrine `shared/quotatest` already stated, applied to the package
that was exempt from it — *what it ships instead is the contract, executable*.
SQLite was 1,104 lines, four schema migrations, a WAL configuration, a
reader/writer pool split, an entry in the v1 compatibility carve-out and ten
dependencies, in order to keep two numbers under a key.

In its place:

- **`store/storetest`** — the contract as a runnable suite. Eight checks, each
  failing with the guarantee rather than the assertion. Two of them exist
  because they fail silently otherwise: a miss must report `found == false` and
  no error (`sql.ErrNoRows` and `redis.Nil` both want to be returned), and
  `LastUsed` must round-trip to the nanosecond (a backend storing whole seconds
  passes everything else and then restores a bucket up to a second stale).
  It is pointed at itself by a mutation test: one correct store, six one-line
  deviations, each asserted to fail in a re-executed child process.
- **`store/memory`** — an in-memory `store.Store` and `BatchStore` that passes
  that suite. Documented as a reference implementation and a test double, not
  persistence. It has no `Close`, deliberately: closing a store releases a
  handle, it does not destroy what the store holds.
- **`examples/store`** — the old `examples/persistent`, rewritten to implement
  the contract against a JSON file in forty lines. Unlike an in-memory stand-in
  it genuinely survives the restart the example claims.

Coverage went **up**, 93.5% to 96.3%, and the CI gate with it — 93% to 95%.
The gate's own comment had blamed unreachable SQL error branches for most of
the shortfall, and it was right. The measurement now excludes the two
conformance suites, whose failure arms only run against a broken backend in a
re-executed child process that no coverage profile sees; they are checked by a
test per break instead, which is the stronger assertion.

### `Config` and `New` moved to the root

The root was a facade of aliases and a forwarding function; the assembly
happened inside `limiter.New`. Now the root is the composition root, and
`limiter` joins `registry`, `gate` and `persist` as a package with a vtable
`Config`.

**If you import `github.com/jaeminst/pace` and write `pace.Config`, nothing
changes.** What changed is that `pace.Config`, `pace.Clock` and
`pace.ConfigError` are declared there rather than aliased, and `limiter.Config`
is now a different type — the vtable the engine takes, every field required,
`limiter.New` panicking on one it cannot work with.

`pace.New` is the one place the two meet, and the translation is the whole of
the difference:

| `pace.Config` | `limiter.Config` |
|---|---|
| `Transport http.RoundTripper` | `HTTPClient *http.Client` |
| `Clock Clock` | `Now func() time.Time` |
| `Rate`, `Burst`, `QuotaFor` | `Quota func(userID string) rate.Quota` |

Passing the answer rather than the type is what `registry.Config.QuotaFor`
already did to avoid importing `rate`. Eleven fields are still declared twice,
which is the real cost and is stated in ADR 0006 rather than hidden.

What is assembled where is decided by one line: a piece is built at the root if
it can be built before the Limiter exists. The registry and the gate cannot —
their `Config`s want method values on a Limiter that does not exist yet — so
they stay inside `limiter.New`.

### Also

- `limiter/gate.go` and `limiter/queue.go` are gone. `limiter/` is fourteen
  files, all named for limiter concerns, at 1,784 lines from 2,377. `gate.go`
  was 66 lines of glue in a file named after a package it could not live in;
  its constructor joined the other constructors, its error translation moved to
  `errors.go`, and its throttle delegate disappeared because
  `reportBucketTokens` already had the signature `gate.Config` wanted.
- `limiter/zero_test.go` proves the vtable rule for the new `Config`: ten
  fields, each wrong on its own, each panicking with the package name and the
  field.
- ADR 0002 (SQLite WAL) and ADR 0003 (at-least-once) are marked superseded
  rather than deleted. 0003 records that v0.1.0 shipped a false "exactly-once"
  claim, which is worth keeping.
- Two stale references fixed in CI: the fuzz artifact path still said `limit/`
  after the v0.7.0 rename to `rate/`, and the coverage comment named a package
  that has been `shared/quotatest` since v0.5.0.
- ADR 0004 still called the field `Config.SharedQuota`; it has been `Config.Shared`
  since v0.7.0.

## [0.7.0]

The library is one package per concern, in the style of
[go-micro](https://github.com/micro/go-micro): the root is a front door and each
contract lives in a package that documents it.

v0.6.0 got the file count right and the structure wrong. It put sixty-three
aliases in the repository root over one 3,700-line internal package, which
satisfied "three files in the root" and changed nothing about how the library
was organised — and cost the whole of the published API documentation, because
Go renders an alias as a single line and internal packages are not published at
all.

That is now inverted. The root holds ten names and three files. Everything they
name is public and documented:

| Package | What is in it |
|---|---|
| `pace` | `New`, `Config`, `Limiter`, `Client`, `Response`, the errors |
| `pace/limiter` | the Limiter and the request path, and its 42 exported methods |
| `pace/rate` | `Limit`, `Quota`, `PerMinute` and friends |
| `pace/bucket` `pace/registry` `pace/persist` `pace/runner` `pace/gate` `pace/sqlite` `pace/breaker` `pace/urlx` | the pieces the Limiter is built from |
| `pace/store` | `Store` — the persistence contract |
| `pace/shared` | `Quota` — the cross-replica backend |
| `pace/shared/quotatest` | the conformance suite for it |
| `pace/observe` | `Observer`, `Stats`, the event structs |
| `pace/queue` | the durable queue's configuration |
| `pace/response` | `Response` |
| `pace/transport` | HTTP connection tuning |

Nothing was lost in the move: every one of the 63 exported names still exists,
and the thirteen that were renamed are named for their package now —
`StateStore` is `store.Store`, `SharedQuota` is `shared.Quota`, `QueueConfig` is
`queue.Config`. See [MIGRATION.md](docs/MIGRATION.md) for the table.

### `internal/` is gone

Every package that was under `internal/` is public: `bucket`, `registry`,
`runner`, `sqlite` (was `internal/store` — the name is taken by the persistence
contract), `breaker` and `urlx`. The repository has no `internal/` directory.

That is a real cost and worth stating rather than presenting as tidying. It adds
144 frozen identifiers — 75 top-level types, funcs and methods, a +65% increase
on the 115 the ten contract packages expose — and deletes one of the five
exclusions the compatibility promise rested on. Among what is now frozen are
four identifiers documented as test seams (`registry.Config.OnGetOrCreate` and
`.AfterSweep`, `runner.Config.AfterPoll`, `runner.Queue.WaitReplay`), and two
`Config`s that are required-everything vtables rather than option structs.

Those two now validate. `registry.New(registry.Config{})` used to build a
zero-length shard slice with a mask of `0xFFFFFFFF` and fail on the first lookup
with an out-of-range index; `runner.New` returned a Queue whose `Start`
dereferenced a nil store on a background goroutine. Both panic naming the
missing field now, and both have a test that proves it — which is the minimum a
constructor owes a caller once it is public.

Nothing was leaking beforehand: no public signature mentioned an internal type,
so this is purely additive and nothing was forced.

`gate` is the shared-quota decision — the shadow bucket that may only refuse,
the backend round-trip, the circuit breaker in front of it, the failure policy
and the poll schedule. It leaves `limiter` at 2,498 lines rather than 2,788, and
takes the breaker field and three counters out of the Limiter struct entirely,
since nothing else wrote them. Its Config has 10 fields of which 4 are functions
and only one — `Throttled` — does real work: a wait is reported *before* it
happens, so that one cannot become a return value. `Allow` and `Take` return
their delay and token count instead, which is why they need no callback at all.
It is a sibling package rather than part of `shared/` for the reason `runner` is
not part of `queue`: a backend author implementing `shared.Quota` should not
have to compile a token bucket and a circuit breaker to do it.

Two test seams disappeared with it. `QuotaPollDelay` and `SleepFor` existed only
to reach two unexported functions, and `SleepFor` fabricated an empty Limiter to
call a method on; both are now ordinary internal tests in `gate`.

Persistence became a package for the same reason. `persist.Adapter` is the
answer to *when* per-user state is written, how long a write may take, whether
it goes out as a batch and what a failure means — four questions the registry
asks and none it should answer, since it is holding shard locks at the time. Its
four methods are what `registry.Config`'s four persistence fields want, and it
has no callbacks at all: everything it decides is a function of its Config and
the argument in hand. Holding no state is what lets an owner rebuild one rather
than reach inside it when the backing store changes.

`sqlite.StateStore` moved with it. It is the one adapter pace ships — the
built-in backend seen as a `store.Store` — and the translation it performs, a
`time.Time` to the Unix nanoseconds the schema holds, is a fact about that
schema rather than about anyone's configuration. It sat in `limiter/config.go`,
which is neither.

The durable queue's SQL moved with the queue rather than with the database.
`Enqueue`, `Claim`, `Kill`, `Dead` and the rest are `runner.Jobs` now, next to
the poller that calls them, because what those statements guarantee is queue
behaviour: `Claim` is one conditional UPDATE, and that statement is the whole
reason two workers racing for the same job cannot both win. `sqlite` keeps the
file, its connections, its schema and per-user state, and lends the queue four
narrow seams — `Exec`, `Query`, `QueryRow`, `Tx` — that route to the right pool
so the single-writer cap survives. The cost is a coupling nothing checks: a
column added in one package for a query in the other, like the `(died_at, id)`
index `Dead` orders by.

### The one thing that could not be cut cleanly

`response` is a package because `queue.RetryDecision` carries a `*Response` so
`RetryOn` can judge the status, while `queue.Config` is part of the Limiter's
`Config`. One of the three had to sit below the other two, and `Response` is the
one with no dependencies of its own.

Worth recording honestly: the decomposition moved 644 lines of declarations and
no behaviour. Every implementation in this library is a method on `*Limiter`
reading `l.cfg`, and a sub-package cannot have one — go-micro's root is thin
because its `store/` owns the interface *and* `memory.go`, `file.go`, `noop.go`,
where here the enforcement is a single piece. `pace/limiter` is about 3,100
lines and that is the shape of the thing, not a failure to finish.

### Also

- `pace.Response` is an alias for `response.Response`, and `facade_test.go`
  pins every re-export as an *alias* rather than a defined type. Getting that
  wrong still passes `go build ./...` and silently breaks `errors.As` and every
  caller's struct literal.
- `RetryPolicy.Backoff`, `RetryPolicy.WithDefaults`, `queue.Config.WithDefaults`
  and `AmbiguousPolicy.Resolve` are exported, because the package boundary now
  runs between their definition and their caller. That deleted a test seam
  rather than adding one.

## [0.6.0]

Structural, and it makes one real trade rather than none.

The repository root holds **four** `.go` files: `doc.go`, `pace.go`,
`example_test.go`, `facade_test.go`. The package that was 47 files in the root
is now `internal/pace`, and the root re-exports it.

`import "github.com/jaeminst/pace"` and `pace.New(...)` are unchanged, and so is
every type, method, field and error value. The aliases are aliases, not defined
types, so a `Config` still crosses the boundary without conversion,
`errors.As(err, &target)` still matches `*pace.LimitError`, and a `StateStore`
you implement still satisfies what the implementation asks for.

### The trade

Go renders a type alias as a single line. The published documentation for this
package therefore shows each type's name and its opening paragraph, but **not
its methods (54), not its struct fields (123), and not the method signatures of
the interfaces you implement (6)**. That is measured, not estimated, and the
same thing happens to `os.FileMode`: `io/fs.FileMode` documents five methods and
`os.FileMode` documents none.

Nothing is missing from the code, only from the rendered page, and two places
still have all of it: editors resolve aliases, so hovering `pace.Config` lists
every field with its documentation; and `internal/pace` carries every comment.
`doc.go` and the README say so where a caller will read it.

### Changed

- `internal/pace` keeps the package name `pace`, so `%T` still prints
  `*pace.Limiter`, panic messages and reflection are unchanged, and the 27 test
  files needed one edited import line each rather than a rewrite.
- Documentation now lives wherever it renders, never in both places: the package
  doc and the six package-level functions moved to the root, and `internal/pace`
  carries a pointer. A comment kept in two copies drifts, and this project has
  spent three releases fixing documentation that had come to lie.
- `example_test.go` and `example_more_test.go` merged. Examples must sit in the
  package directory to appear in its documentation, so they stayed in the root
  and now exercise the facade. Two of them dropped a `//nolint:gocritic` for
  `log.Fatal` in favour of the file's existing `must`, which does not skip the
  deferred cleanup the examples rely on.
- `facade_test.go` is new and is the only test in the root. It pins all 63
  exported names at compile time, and asserts each of the 36 types is an *alias*
  by assigning the zero value both ways with no conversion. That check earns its
  place: changing `type Stats = impl.Stats` to `type Stats impl.Stats` still
  passes `go build ./...`, and would break `errors.As` and every caller's struct
  literal silently.

## [0.5.0]

Structural. The exported API is unchanged — `go doc -all .` is identical before
and after, which is the claim this release is making.

The repository root held 56 `.go` files — 25 production, 31 test. It now holds
47 (18 and 29), and 630 lines of machinery moved into packages that can be
tested without building a Limiter.

The ceiling is lower than it looks, and worth recording so the next attempt
starts from it: 26 of the 31 root test files are black-box, and half of them use
seams from `export_test.go` that only exist for test files in the same
directory. Moving those would mean promoting 17 test-only helpers to public API.
Tests follow production code out of the root or they do not move at all.

### Changed

- **`registry`** owns the user population: the sharded map, each user's
  bucket, when their state is read and written, and when they are evicted
  (`shard.go`, `gc.go`, `state.go`). It takes plain values and function fields,
  so it never imports the parent. Ten declarations that were merely unexported
  are now genuinely package-private, and `Stats`, `Limiter` and `Config` no
  longer reach into shard internals. What persisting a user *means* stayed
  behind: `StateStore`, the `BatchStateStore` assertion, and the batching flush.
- **`breaker`** is the shared-quota circuit breaker, which referenced
  nothing from pace — `sync`, `time`, and a failure count. Its half-open state
  exists to handle a dead backend, so every transition through the old path cost
  a real timeout; it now has eight unit tests, two of which cover arms that were
  unreachable before. It also removes a duplicated constant:
  `sharedquota_test.go` carried `const quotaBreakerTrips = 5` copied from
  production, because a black-box test could not see the unexported original.
- **`urlx`** is the request-URL string surgery. Both functions have been
  the site of a defect found by fuzzing, and the fuzz target now exercises them
  directly instead of building a Limiter per iteration — about a thousand times
  more input in the same thirty seconds.
- Four root files merged into what they describe: `clock.go` and
  `sqlitestore.go` into `config.go`, `limit.go` into `quota.go`, `hooks.go` into
  `limiter.go`. `MIGRATION.md` moved to `docs/`, `CONTRIBUTING.md` and
  `SECURITY.md` to `.github/`.
- `Grant.Tokens` is now reported as `ThrottleInfo.Tokens` when a shared backend
  refuses. It was a field pace asked every backend to populate and read nowhere,
  while the throttle report carried the local shadow's count — this replica's
  fraction of the quota, which [ADR 0004](docs/adr/0004-shared-quota-is-approximate.md)
  states is never authoritative.

### Fixed

- **Two tests asserted a rule that was removed in v0.3.0.** `TestNew_StoreBothSet`
  and `TestNew_StoreMutuallyExclusive` both required `New` to reject `Store` and
  `DBPath` together. `validate` has had no such check since, and both passed only
  because `/tmp/both.db` cannot be opened on Windows — they were reading an
  unopenable-path error as a validation error. Given a real path `New` succeeds,
  which `TestStoreAndDBPathCoexist` in the same suite already asserted. On a
  Linux runner they would have failed.
- Four comments and test names referred to functions that do not exist
  (`waitFailure`, `createUserBuckets`, `getOrCreateUser`, a `Config.Endpoints`
  field that never existed), and `sweep`'s doc comment was attached to
  `sweepInPlace` with `sweep` left undocumented — all artifacts of the v0.4.0
  extraction, and two of them rendered by godoc.
- **The changelog claimed twice that a release was the last one that may break
  the API**, in v0.3.0 and again in v0.4.0. Neither was. Replaced with the rule
  that holds: below 1.0.0 any release may break, and the freeze begins at v1.0.0
  — which is what `doc.go` has said all along.
- The sweep benchmarks set `IdleExpiry` to zero, so a user created inside the
  same clock tick as the sweep compared equal to the cutoff rather than less and
  was not collected. On a coarse clock that swept a varying fraction of the
  population, making the number unrepeatable. They backdate now.

## [0.4.0]

An audit before tagging found defects that cannot be fixed additively once
v1.0.0 freezes the API, so they are fixed here. See
[MIGRATION.md](docs/MIGRATION.md) for an old/new table.

While the version is below 1.0.0 any release may break the API; the freeze
begins at v1.0.0, which is the promise `doc.go` makes and the only one this
project is in a position to keep. Two earlier entries in this file each called
themselves the last breaking release. Neither was, so this one does not say it.

Requires **Go 1.26.6+**, and the CI matrix's floor leg now tracks that exactly
so the claim is tested rather than assumed.

### Fixed

- **`Wait` never returned when a shared backend refused without saying when to
  retry.** Not a spin that ends at the deadline — an unbounded loop. The refusal
  path cancels the shadow reservation, which puts the token back, so the
  local-estimate fallback was *structurally* guaranteed to compute zero on
  exactly that path; zero then flowed into a sleep that returned without
  consulting the context, so nothing in the loop could notice the caller had
  given up. `Grant.RetryAfter` documents zero as legal and `pacetest` accepted
  such a backend as conformant, so the shipped conformance suite passed a
  backend that would hang every `Wait`.
- **`QuotaFallbackLocal` admitted without limit on the waiting path.** With a
  `WaitingSharedQuota`, every failure path returned nil — so the *default*
  policy, the conservative one, became `QuotaAllow`: a backend goes down, five
  failures open the breaker, and for the next five seconds every user is served
  instantly at unbounded rate. The test could not have caught it; its
  `QuotaFallbackLocal` and `QuotaAllow` rows asserted the identical thing.
- **Caller cancellation was charged to the circuit breaker and returned as
  success.** A conformant backend honours the context, so an expired caller
  deadline produced an error pace recorded as a *backend* failure and then
  converted into "proceed" — `Wait` returning nil on a dead context, where the
  non-shared path returns a `LimitError`.
- **`Client.Reserve` ignored `SharedQuota` entirely**, making the shadow bucket
  authoritative — which [ADR 0004](docs/adr/0004-shared-quota-is-approximate.md)
  states it can never be — and admitting requests with zero `Take` calls, where
  the same ADR promises exactly one.
- The circuit breaker had no half-open probe despite its documentation promising
  one, so the cooldown expiring released the whole backlog at once; and opening
  reset the failure count, so re-opening needed five more failures rather than
  one. The recovery half was entirely untested.
- `ReloadQuotas` stamped every bucket with one instant captured before the walk
  began, rewinding the clock of any user whose bucket had advanced past it — and
  a rewound interval is refilled twice.
- `Observer.Throttled` fired for every request on the `WaitingSharedQuota` path,
  making `Stats.Throttled` equal `Stats.Requests` identically.
- **The guarantees table said the durable result cache never expires.** It
  expires after `Queue.ResultTTL`, 24h by default. The row sat directly under
  ADR 0003, whose closing line asks the next person writing a guarantees table
  to argue with it first.
- `StateStore`'s documentation still said `Store` and `DBPath` were mutually
  exclusive, which v0.3.0 reversed — in the paragraph a `StateStore` implementer
  reads first.
- **The coverage gate passed when it could not measure anything.** Without
  `pipefail` the step's exit status was awk's, so a failed `go tool cover`
  produced no `total:` line, matched nothing, and exited 0.
- The release workflow ran only `vet` and the tests — no lint, no format check —
  so a tag could ship code `main`'s own CI would have rejected. It now runs the
  same gates and refuses a tag whose commit is not an ancestor of `main`.

### Changed

- `Observer.UserEvicted` takes `(ctx, EvictInfo)`; `EvictInfo` adds `Tokens` and
  `LastUsed`. `Observer.JobTransition` and `Queue.OnDeadLetter` take a context.
- `Config.SharedQuota`, `.QuotaNamespace`, `.QuotaTimeout` and `.OnQuotaError`
  become `Config.Shared.{Quota,Namespace,Timeout,OnError}`.
- `Client.Allow` and `Client.Reserve` take a context. Both do real I/O and were
  the only entry points in the package that did so without one.
- `StateStore` drops `Close`; implement `io.Closer` if you need it. Newly
  documented: pace closes a `Config.Store` that implements it, which it always
  did and never said.
- `Queue.RetryOn` takes `(ctx, RetryDecision)`, which carries the attempt number.
- `Limiter.DeadJobs` takes a `DeadJobQuery` with `Limit`, `Before` and `UserID`,
  so the dead-letter table can be drained past the newest page.
- `Stats` counters are all `int64`; `Wait` becomes `WaitTotal`.
- `Grant.Tokens` is a `*float64`; nil means "not tracked".
- `pacetest.QuotaSuite` takes variadic options, and `NewQuota` becomes
  `QuotaFactory`.

### Added

- `Stats.QuotaTakes`, `.QuotaRefused` and `.QuotaErrors`. Nothing previously
  revealed whether the shared backend was being reached at all, so an operator
  whose Redis was down saw a healthy-looking snapshot while every replica
  quietly fell back to limiting per process. `QuotaErrors` is the one to alert
  on.
- `DeadJob.DiedAt`. The table has stored `died_at` since v0.2.0 and never
  exposed it. Schema v3 indexes that column, which `DeadJobs` has always ordered
  by without one.
- Every GitHub Action pinned to a commit SHA.

### Internal

- The durable queue's background half moved to `internal/queue`, behind a single
  injected `Dispatcher` func — the one thing that exists to break the import
  cycle. `*Response` never crosses the boundary; the in-process singleflight
  stays with the parent, because it is request deduplication rather than queue
  state.
- The root package is one responsibility per file. `limiter.go` went from 1,058
  lines to about 300.
- Three shapes that had been written out repeatedly — the quota read off a
  bucket, the `LimitError` construction, and the `ThrottleInfo` literal — are
  each in one place. Two of the seven `ThrottleInfo` sites had already drifted.
- `pacetest` is now tested against six deliberately-broken backends, one per
  guarantee. It caught a hole immediately: the context check asserted only that
  `Take` returned, so a backend ignoring the context passed if it was fast.

## [0.3.0]

Breaking. v1.0.0 freezes the API, so what is here is what becomes impossible
afterwards — everything merely additive was deferred.
See [MIGRATION.md](docs/MIGRATION.md) for an old/new table.

### Changed

- **`Config`'s nine durable-queue fields move into `Config.Queue`.**
  `IdempotencyHeader`, `AmbiguousPolicy`, `OnDeadLetter`, `Retry`, `RetryOn`,
  `QueueWorkers`, `QueuePollInterval`, `JobLease`, and `ResultTTL` become
  `Queue.IdempotencyHeader`, `.AmbiguousPolicy`, `.OnDeadLetter`, `.Retry`,
  `.RetryOn`, `.Workers`, `.PollInterval`, `.JobLease`, `.ResultTTL`. Nine
  fields configured one optional subsystem while sharing a namespace with
  `Rate` and `Burst`; grouping them is impossible after v1, and not grouping
  means every future queue knob inflates the top-level struct forever. A caller
  who set none of them sees no change: the zero `QueueConfig` resolves to the
  same defaults.
- **`Client.Durable(id)` returns `*Request`, not `(*Request, error)`.** Building
  a Request is documented twice as free and infallible; `Durable` was the sole
  exception, charging every call site a four-line error block for two conditions
  that never change during a process's life. `ErrNoQueue` and `ErrInvalidID` now
  surface from the terminal method, where an error is already being checked.
- `Config.Store` and `Config.DBPath` are no longer mutually exclusive. They
  persist different things, and forbidding both meant a Redis- or
  Postgres-backed caller could never have a durable queue — silently, with no
  signal at `New`. `ErrNoQueue`'s message now says which field provides one.

### Added

- **Per-user quotas.** `Config.QuotaFor func(string) Quota` overrides `Rate` and
  `Burst` for individual users, which is the feature the package name implies
  and did not have: pace could isolate users from each other but not tell a free
  tier from a paying one. Each `Quota` field falls back independently, so the
  zero value means "the defaults" and a map is a complete implementation.
  `Limiter.ReloadQuotas()` re-applies it to live buckets while keeping accrued
  tokens, and `Client.Quota()` reports what a user's bucket is enforcing.
- **`Client.Reserve`.** Holds a token, reports how long until it may be used,
  and lets you hand it back — the ground between `Allow`, which refuses rather
  than waits, and `Wait`, which waits and cannot refund.
- **`Config.SharedQuota`**, optional cross-replica rate limiting. Delegates the
  decision to a backend every replica consults, keeping the local bucket as a
  shadow that can only refuse. pace ships no backend; it ships the contract, as
  `pacetest.QuotaSuite`. Read [ADR
  0004](docs/adr/0004-shared-quota-is-approximate.md) first — it argues that
  most callers should not use this.
- `pacetest`, a package of conformance suites for the interfaces pace asks
  callers to implement.

### Fixed

- **A relative path could retarget the request at another host.** The path is
  concatenated onto `BaseURL`, but only one side of the seam was normalised, so
  against a base with no path of its own a path not starting with `/` ran into
  the authority: `https://api.example.com` plus `.evil.example.com/x` is a
  request to a host the caller never named. With any part of the path coming
  from user input, that is a request-forgery primitive. Found by fuzzing.
- `pace.Limit` is a `float64`, so `Limit(math.Inf(1))` — which reads as "no
  limit" — passed `validate`, whose only check was `Rate <= 0`, and produced a
  bucket holding `NaN` tokens that refused every request for the life of the
  process. A non-finite rate now maps onto `Inf`, and a `NaN` `Rate` is rejected
  at `New`. Found by fuzzing.
- `validateBaseURL` checked `url.URL.Host`, which is non-empty for `http://:`
  and `http://:8080` — bases with no hostname at all.
- `Response.RetryAfter` overflowed into a negative duration for a seconds value
  near `MaxInt64`, which a caller comparing it against a threshold reads as
  "retry immediately". It also used `time.Until`, making it the one method in
  the package that ignored `Config.Clock`.
- The idle sweep allocated an eviction-ID list even with no `UserEvicted`
  observer configured — 57KB per sweep of 2,000 users, on the path whose whole
  point is that it does almost nothing.
- `LimitError` and `ThrottleInfo` report the quota that user's bucket is
  enforcing, rather than `Config.Rate`. Their documentation always said "the
  configuration in force for that user"; until `QuotaFor` existed there was only
  one configuration, so reading the global happened to be right.
- `Client.Allow`'s documentation no longer claims it never blocks without
  qualification. It does not wait for quota, but it can do bounded I/O: a store
  load on a user's first request, and a `SharedQuota` call when one is
  configured.

## [0.2.0]

The single consolidated breaking release before v1.0.0. Everything that was ever
going to break breaks here, so that v1 can freeze the API — after v1, a breaking
change costs a `/v2` import path permanently. There are no deprecation shims;
see [MIGRATION.md](docs/MIGRATION.md) for an old/new table.

The common path is unchanged: `client.Get(ctx, "/path")` and its siblings keep
their signatures. What moves is how you obtain the client, how you configure it,
and what a few methods return.

### Added

- `Request.SetQuery`, `AddQuery`, and `SetQueryValues` add query parameters
  with proper escaping, merged with anything already written into the path.
- `Limiter.Stats` returns a snapshot of live users, requests, throttling, wait
  time, errors, and evictions. The counters are atomic loads and the user count
  sums a per-shard tally, so it is cheap enough to call on a scrape interval.
- `Config.Observer` reports throttling (with the expected delay), finished
  requests (status, latency, error), user evictions (with a reason), and durable
  job transitions. It is a struct of optional functions rather than an
  interface, so events can be added later without breaking implementations.
- `Config.MaxResponseBytes` caps the buffered response body (zero = unlimited,
  as `http.Client` does). Reading an unbounded body into memory is how a hostile
  or merely misbehaving upstream takes the process down.
- `Request.Stream` returns the raw `*http.Response` with its body unread, for
  responses too large to hold in memory. The caller closes the body; doing so
  releases the request, so `Shutdown` waits for it.
- `Config.RequestTimeout` bounds one HTTP round-trip. It excludes time spent
  waiting for a token: a request held back by throttling has not started, and
  charging that wait against its timeout would make the timeout a function of
  how busy the user is. It does not apply to `Request.Stream`, since a context
  deadline stays armed until the body is closed and would cut off the long
  download streaming exists for; use `TransportConfig.ResponseHeaderTimeout`
  there, which bounds the wait for headers without bounding the body.
- `Response.OK`, `Response.JSON`, and `Response.RetryAfter` (both the
  delta-seconds and HTTP-date forms), plus `Request.SetJSON`.
- `Config.ResultTTL` (default 24h) expires cached durable results. The cache is
  what makes a repeated `Durable` call cheap, but nothing bounded it, so on a
  busy service it was the dominant term in the database file's growth. Note that
  SQLite does not return freed pages to the filesystem: the file stops growing,
  it does not shrink.
- `Config.Retry` (a `RetryPolicy`) gives durable jobs exponential backoff with
  full jitter and an attempt ceiling; exhausting it dead-letters the job.
  Jitter is on by default because the failure that matters is correlated — an
  upstream outage stalls every job at once, and a fixed schedule sends them all
  back at the same instant.
- `Config.RetryOn` lets a caller decide which *responses* are worth repeating.
  It is nil by default: a response of any status means the request was
  delivered, and pace does not interpret status codes on your behalf.
- `Config.QueueWorkers` and `Config.QueuePollInterval` bound and pace the
  background retry loop.
- `Config.IdempotencyHeader` (default `Idempotency-Key`) is sent on every
  durable request carrying the job ID, so a cooperating server can collapse a
  retry into the original delivery. Set it to `"-"` to send nothing.
- `Config.AmbiguousPolicy` decides the fate of a durable job whose outcome is
  unknown after a crash: `AmbiguousAuto` (default) retries only when repeating
  is safe, `AmbiguousRetry` always retries, `AmbiguousPark` never does.
- `Config.OnDeadLetter` reports abandoned jobs, and `Limiter.DeadJobs` reads
  them back so they are visible to an operator after a restart.
- `Config.JobLease` bounds how long a claimed durable job stays owned, so a
  worker that crashes mid-send does not strand it.
- `ErrJobClaimed` reports that another worker owns a durable job.
- The SQLite schema is now versioned and migrated (`PRAGMA user_version`).
  Databases written by v0.1.0 upgrade in place; a database stamped newer than
  the running binary is refused rather than written through, so a rolled-back
  deploy cannot corrupt state a newer binary still expects to read.
- `TransportConfig` gains `Proxy`, `MaxConnsPerHost`, `ExpectContinueTimeout`,
  `DisableHTTP2`, and `DisableCompression`.
- `Request.AddHeader` appends a value without replacing existing ones, and
  `Request.Header()` exposes the underlying `http.Header`.
- `BatchStateStore` is an optional extension to `StateStore`. A store that
  implements it receives whole batches from the idle-user sweep and the final
  flush instead of one call per user, which matters when a sweep evicts
  thousands of users at once. The built-in SQLite backend implements it.
- `Config.StoreTimeout` bounds every `StateStore` operation (default 5s), so a
  wedged backend degrades to a fresh bucket rather than wedging the request.
- `Config.Shards` sets the lock-striping width (default 256, rounded up to a
  power of two, capped at 2^20). Lower it when running one Limiter per upstream.
- `Client.Wait(ctx)` blocks until the user has a token, and `Client.Allow()`
  takes one without blocking — the non-blocking and blocking halves of the
  `x/time/rate` trio, for pacing work pace does not perform itself.
- `Request.Do(ctx, method, path)` for methods without a named helper.
- `Client.UserID()` reports the identity a handle is bound to.
- White-box benchmarks isolating pace's own machinery from HTTP round-trip cost
  (`bench_internal_test.go`), plus `BenchmarkRequest_NoHTTP` for the full request
  path with the network stubbed out. A recorded baseline lives in
  `docs/bench/baseline-v0.1.0.txt`.
- `Makefile` with `test`, `lint`, `fmt`, `cover`, `bench`, `benchstat`, `vuln`,
  and `ci` targets.
- CI now runs a three-OS (Linux, macOS, Windows) by two-Go-version matrix with
  `-shuffle=on`, and enforces formatting via `golangci-lint fmt --diff`.
- `govulncheck`, CodeQL, and Dependabot workflows.
- Linters: `errorlint`, `nilerr`, `bodyclose`, `contextcheck`, `copyloopvar`,
  `makezero`, `wastedassign`, `predeclared`, `nolintlint`, `perfsprint`, `godot`,
  `thelper`, `tparallel`, `usetesting`, `depguard`.
- `.gitattributes` pinning the working tree to LF so `gofmt` checks behave the
  same on Windows as in CI.

### Changed

- **Breaking:** `Config.OnThrottle` is replaced by `Config.Observer.Throttled`,
  which carries the expected delay, the token count, and the limit in force.
  The old callback reported only that throttling had happened, which is the one
  thing the caller could already infer.
- **Breaking:** `Client.Tokens` returns `(float64, bool)` instead of using -1
  as a sentinel, which could not be told apart from a legitimately negative
  count. `Client.Evict` takes a context and returns `(bool, error)`: it performs
  store I/O, and swallowing that error into a log line is the wrong choice for
  an operation the caller invoked deliberately.
- **Breaking:** request headers are an `http.Header` rather than a
  `map[string]string`, which could not express a header that legitimately
  repeats (`Accept`, `Set-Cookie`). Durable jobs persisted by v0.1.0 have their
  stored headers converted by the schema migration.
- **Breaking:** `StateStore` methods now take a `context.Context`, and
  `SavedState` is replaced by `State` with a `time.Time` rather than unix
  nanoseconds. The README advertised Redis and Postgres backends that the old
  signature could not support — its own example closed over a `ctx` variable
  that did not exist in scope. Migration:

  ```go
  // was: Save(userID string, state pace.SavedState) error
  func (s *MyStore) Save(ctx context.Context, userID string, st pace.State) error
  ```
- **Breaking:** the rate-limit token is now taken when a request is sent, not
  when the builder is handed out. `Client.Request()` takes no context, returns
  no error, and costs nothing; the context moves to the terminal methods:

  ```go
  resp, err := lim.Client("alice").Request().
      SetHeader("X-Request-ID", "req-001").
      Post(ctx, "/resources")
  ```

  Code that called `Request(ctx)` only to acquire a token should call
  `Client.Wait(ctx)` instead. `Client.Get`/`Post`/`Put`/`Delete`/`Patch` are
  unchanged.
- **Breaking:** `Client.Durable(ctx, id)` is now `Client.Durable(id)
  (*Request, error)`, with the context passed to the terminal method. The
  deferred-error field it used to stash setup failures in is gone.
- **Breaking:** `New` returns a `*Limiter` rather than a `*Client`, and per-user
  handles come from `Limiter.Client(userID)`. `Config.Name` and `Client.For` are
  gone, and `Close`/`Shutdown` moved from `Client` to `Limiter`, where the
  resources they release actually live. `Close` now returns an error, so
  `*Limiter` satisfies `io.Closer`. Migration:

  ```go
  lim, err := pace.New(cfg)   // was: client, err := pace.New(cfg)
  defer lim.Close()
  alice := lim.Client("alice") // was: client.For("alice")
  resp, err := alice.Get(ctx, "/items/42") // unchanged
  ```
- **Breaking:** `Config.RatePerMinute int` is now `Config.Rate Limit`. Build it
  with `pace.PerSecond`, `pace.PerMinute`, `pace.PerHour`, or `pace.Every`, or
  use `pace.Inf` to disable throttling. Migration is mechanical:
  `RatePerMinute: 60` becomes `Rate: pace.PerMinute(60)`.
- **Breaking:** `ErrNoPersistence` is renamed `ErrNoQueue`. It reports a missing
  durable *queue*, which is a distinct thing from the `StateStore`.
- **Breaking:** `New` now returns `*ConfigError` rather than opaque errors, so
  callers can tell which field was rejected via `errors.As`.
- `go.mod` now requires `go 1.25` rather than the patch-level `go 1.25.7`, which
  had forced every consumer onto a toolchain at least that new.
- `BenchmarkCaller_ConcurrentUsers_256` is now `BenchmarkConcurrentUsers_256`
  and drives a stub transport. Pointing 256 goroutines at an `httptest` server
  measured the host's TCP accept backlog rather than pace, and overflowed it on
  Windows. The remaining loopback benchmarks are suffixed `_E2E`.

### Fixed

- **The durable queue never provided exactly-once delivery, and the README said
  it did.** A job dispatched but never recorded — a crash between the response
  and the commit, or a `Complete` that failed and was only logged — was replayed
  on restart, sending the request a second time. For a payment that is a
  duplicate charge. Delivery is now documented as at-least-once, the intent to
  send is committed *before* dispatch so the ambiguous window is detectable
  rather than silent, and `Config.AmbiguousPolicy` decides what happens to a job
  caught in it instead of blindly re-sending.
- `sqlite` stamped `created_at` and `completed_at` from `time.Now()`
  while everything else read `Config.Clock`, so the two disagreed and durable
  timestamps were untestable. The store is now told the time rather than
  deciding it, the same correction made earlier for `RestoreBucket`.
- Retrying happened only at startup, and only by spawning one goroutine per
  pending job. A fifty-thousand-job backlog became fifty thousand goroutines,
  each holding a request and a body buffer, and nothing was retried until the
  next restart. Recovery and retries now share one bounded worker pool.
- Two workers could send the same durable job. `INSERT OR IGNORE` deduplicates
  the row, not the send, so a replay goroutine and a live caller — or two
  processes sharing the database — could each decide they were the leader.
  Claiming a job is now a single conditional `UPDATE`.
- Two workers sharing a database could still double-send, by a second route the
  claim could not close. `Complete` deletes the pending row, so a finished job
  leaves nothing for `INSERT OR IGNORE` to conflict with: the losing worker
  reads the result cache just before the winner writes it, re-inserts the job as
  a fresh `queued` row, and legitimately wins the claim on it. `Enqueue` is now
  conditional on no recorded result. Found by writing the test for the
  multi-process guarantee the README states — it double-sent 14 of 40 jobs.
- `store.Release` matched on the job ID alone, so a worker whose lease had
  expired could return to the queue a job another worker was already sending,
  producing a third copy. It now matches on owner and state, and reports whether
  the release happened so the caller can stop rather than schedule a retry for a
  job it no longer owns.
- `Client.Allow`, `Client.Evict`, and `Limiter.DeadJobs` bypassed the shutdown
  barrier and could touch a store `Close` had already shut. The check-then-
  register sequence is now a single `enter`/`leave` pair rather than restated at
  each call site.
- `Client.Evict` called the `UserEvicted` observer with the shard write lock
  held, so an observer that asked the Limiter anything — `Tokens`, `Stats` —
  deadlocked against the eviction that notified it. It now fires outside the
  lock, and after the state has been persisted, so a failed save is no longer
  reported as a clean eviction.
- `Stats().Users` never returned to zero after `Close`: shutdown reported every
  remaining user as evicted but left them in the shards, so one snapshot claimed
  N users and +N evictions at once.
- `Request.Stream` was counted in `Stats.Requests` but skipped both
  `Stats.Errors` and `Observer.RequestFinished`, so the two halves of the metric
  described different populations.
- A failed `Complete` was logged at Warn and forgotten, silently converting a
  completed job into one that would be re-sent. It is now retried, and logged at
  Error when it still fails, because that is lost data.
- The built-in SQLite backend and user-supplied stores met at a private
  interface with a wrapper bridging them, so the batteries-included path was a
  special case that custom backends could not exercise. SQLite is now adapted to
  the same public `StateStore` a caller would implement, leaving one code path.
- The GC sweep held a shard's write lock across every `store.Save`, so evicting
  idle users blocked live requests hashing to that shard for the duration of a
  SQLite transaction each. Sweeping 2,000 users took ~4.6s of lock-held time;
  it now takes ~12ms with no lock held during persistence at all, by
  snapshotting under the lock, persisting outside it, and only then deleting.
  `saveAll` had the same shape and got the same treatment, and `SaveBatch`
  collapses a per-user transaction storm into chunks of 512.
- A user who made a request *during* a sweep was evicted anyway: `lastUsed` is
  updated atomically without taking the shard lock, so the sweep could not see
  it. The delete phase now skips any user touched since the snapshot.
- `userFor` called `store.Load` while holding the shard write lock, so a
  network-backed `StateStore` — the Redis and Postgres backends the README
  advertises — would close a shard for the length of a round-trip on every new
  user. The load now happens before the lock is taken.
- `Shutdown(ctx)` did not wait for in-flight requests, despite documenting that
  it does. The active-request counter was scoped to the call that returned the
  builder, which finishes before the HTTP round-trip starts, so the counter was
  already zero by the time a request was on the wire — and Shutdown returned and
  closed the store underneath it. The registration now spans the whole
  operation.
- Shutdown's deadline could not cancel a round-trip already in progress, so a
  server that never answered would outlive the Limiter. Each request context is
  now merged with the Limiter's lifetime.
- `Close` never waited for the GC goroutine: `gcWg` was created and added to but
  only ever waited on from a test helper, leaving a sweep free to `Save` into a
  store that `Close` had already shut. Both `Close` and `Shutdown` now run one
  teardown sequence that drains the GC loop, replay, and in-flight requests
  before touching the store.
- `Durable("")` silently degraded to a plain request *and* skipped rate limiting
  entirely, because dispatch keyed on a non-empty ID string while the plain
  branch assumed a token had already been paid for. An empty ID is now
  `ErrInvalidID`.
- A builder that was created and then abandoned burned a token nothing could
  refund. Building is now free.
- Replayed durable jobs ran on `context.Background()` and so could not be
  cancelled at shutdown; they now run on the Limiter's context.
- Lifecycle methods hung off a per-user handle, so `bob := alice.For("bob");
  bob.Close()` tore down the limiter `alice` and every other user shared. Only a
  `Limiter` can be closed now, so the mistake no longer compiles.
- With `Config.Name` unset, `userID` was the empty string and every convenience
  call silently rate-limited all traffic as one anonymous `""` user. Identity is
  now always supplied explicitly to `Limiter.Client`.
- The `_test.go` exclusion in `.golangci.yml` used the v1 `issues.exclude-rules`
  location, which v2 parses without complaint and ignores.
- A request that could not get a token in time reported `ErrClosed` — "client
  closed" — on a Client that was open and healthy. The limiter answers "would
  exceed context deadline" without waiting, so the caller's `ctx.Err()` is still
  nil at that point, and that was being read as proof the engine had shut down.
  Throttling now returns a `*LimitError` carrying the user, limit, and burst,
  and `ErrClosed` is returned only when the engine's own context is done. Both
  bundled examples printed the wrong error before this change.
- Restoring a persisted bucket rounded the token count to a whole number, so
  fractional credit was silently lost or invented on every restart and every GC
  eviction. Saving 0.5 tokens restored as 0; saving 2.7 restored as 3. With
  `Burst: 1`, any partial token restored as 0 — the user lost their credit
  entirely. Restore is now exact.
- `RestoreBucket` read the wall clock directly instead of the injected
  `Config.Clock`, which made the whole persistence-restore path impossible to
  test deterministically. It now takes `now` from the caller, and `Tokens`,
  `saveAll`, `sweep`, `evict`, and the `OnThrottle` check all read through the
  configured clock.
- A per-minute rate that does not divide 60s evenly was truncated by routing it
  through a `time.Duration` interval. The conversion is now exact.
- `sqlite` compared errors against `sql.ErrNoRows` with `==` instead of
  `errors.Is`, which would miss a wrapped error.
- `CONTRIBUTING.md` claimed CI enforced formatting via `go vet`. It does not —
  `go vet` has never checked formatting, and one file was in fact unformatted.

## [0.1.0]

Initial release.
