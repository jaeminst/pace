# ADR 0009 — config, limiter, client

**Status:** accepted (v0.12.0). One claim below was wrong and is corrected in the amendment at the end.

## Context

Two packages were doing five jobs.

The root held `Config` — sixteen fields, its validation, its defaulting — and
`New`. `pace/limiter` held everything else: token buckets, the sharded user
population and its GC, the cross-replica quota gate, the persistence policy, the
lifecycle, **and** URL building, `http.Client.Do`, response buffering, a
chainable request builder, **and** the rate vocabulary a caller writes
(`Limit`, `Quota`, `PerMinute`, `Inf`). Fifteen files, 1,988 non-test lines,
three subjects.

The seams were visible in the file list and in the import graph. `limiter`
imported `net/http` and `urlx` for one of its three jobs. `Spec` carried four
fields — `BaseURL`, `HTTPClient`, `RequestTimeout`, `MaxResponseBytes` — that
nothing in the engine read, because the engine makes no requests. And the
vocabulary a caller types most often lived in the package a caller had least
reason to import.

[ADR 0008](0008-the-root-re-exports-nothing.md) had already tried the obvious
repair once: hoist the request path *up* to the root, so the engine becomes a
pure rate limiter. That was built (3a84ff5) and reverted a day later (aa25073)
because `lim.Client("alice")` has to keep working, and a method on
`limiter.Limiter` cannot return a type declared in the root without `limiter`
importing the root. ADR 0008 recorded the conclusion as "the only two coherent
layouts are the one above and a single package holding everything."

That conclusion was too strong, and the reason is worth naming precisely: the
constraint is on **where the return type of `Pool.Client` lives**, not on where
the request path lives.

## Decision

**Three packages, one job each. The repository root declares nothing.**

```
pace/            doc.go — package pace, zero declarations
├─ config/       Config, Clock, Error, Limit, Quota, Inf, Finite, Per*
├─ limiter/      Limiter, LimitError, ErrClosed, Reservation
└─ client/       Pool, Client, Request, Response, ErrBodyTooLarge
```

```go
pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Rate:    config.PerMinute(60),
})
defer pool.Close()

resp, err := pool.Client("alice").Get(ctx, "/items/42")
```

`Pool.Client` returns a `*client.Client` — same package as `Pool`, so there is
no cycle and nothing to dissolve into the root. That is the whole of what ADR
0008 missed.

Import graph, verified with `go list`:

```
config  → observe registry shared store urlx
limiter → bucket config gate observe registry store
client  → config limiter observe urlx
```

`limiter` no longer imports `net/http` or `urlx`. There is no `Spec` at all —
`limiter.New` takes the `config.Config` itself. The diagram and graph above are
the shipped state; the amendment at the end is why they differ from what this
Decision first said.

### What cannot move, and why an interface does not help

Checked file by file, and recorded because these get proposed again:

- ~~**`limiter.Spec` cannot live in `config`.**~~ **This was wrong — see the
  amendment.** What is true is the narrower claim it was reasoning about:
  `func (Config) Spec() limiter.Spec` names a type in the package that imports
  `config`, so *that method* is forbidden. Moving the type is a different move.
- **`registry.Spec` cannot either.** Four of its callbacks take or return
  `registry.Snapshot` and `registry.Eviction`. Its
  `QuotaFor func(string) (float64, int)` is written in bare numbers precisely to
  avoid this class of edge — see
  [ADR 0007](0007-contracts-carry-numbers-not-types.md).
- **An interface breaks neither.** ADR 0007 states the argument and this ADR
  does not restate it: an interface breaks a cycle when the lower package needs
  the upper one to *do* something. These are data cycles. You cannot divide by
  an interface.

### `config` is not `pace/rate` again

The objection this layout invites: v0.9.0 deleted `pace/rate`, a leaf package
holding exactly this vocabulary, and here it is back under a different name.

It is not the same package, and ADR 0007's own numbers are why. `rate` existed
"because **five** other packages shared it", two of them contract packages a
third party implements a backend against — so a Redis author compiled
`pace/rate` to read two numbers out of a request. ADR 0007 fixed that by
de-typing `observe.ThrottleInfo` and `shared.TakeRequest` to `float64` and
`int`. Those fields are still bare numbers. `config` is imported by `limiter`
and `client`, both of them pace's own engine, and by nothing a third party
writes.

The test to apply next time: **the vocabulary may live wherever it reads best,
provided no package implemented against from outside has to compile it.**

*(v0.13.0 applied that test again and moved it once more, to `pace/bucket`, where
`Quota` sits beside the bucket it describes. See
[ADR 0011](0011-the-vocabulary-belongs-to-the-bucket.md). The `config` package
keeps `Config`, `Clock` and `Error`.)*

Note that `config` is not a leaf — it imports five packages. That would have
been fatal under ADR 0007's framing and is harmless under this test.

### Two new exported names

`client.New` needs three things that were unexported methods on the root's
`Config`. They collapse to two:

```go
func (cfg Config) Resolve() (Config, error)      // validate, then default
func (cfg Config) Quota(userID string) Quota     // becomes Spec.Quota
```

`Resolve` rather than exported `Validate` and `WithDefaults` because the two are
never useful apart, and because exposing them as peers publishes an ordering
contract as API: `Quota` called on an unresolved `Config` returns `{0, 0}` — a
bucket that refuses every request, silently, forever. Behind `Resolve` that
ordering stays private.

The method is `Quota` and not `QuotaOf` despite `Config.QuotaFor` being a field
on the same struct. `QuotaOf` beside `QuotaFor` is a one-syllable difference no
reviewer catches; `limiter.Spec{Quota: cfg.Quota}` reads correctly at the one
call site that matters. (`QuotaFor` itself is unavailable: Go forbids a field
and a method of the same name.)

### The root keeps `doc.go`

`package pace` with zero declarations. `import "github.com/jaeminst/pace"`
compiles, pkg.go.dev keeps a landing page, and the Go Reference badge keeps
resolving. The empty Index is stated as deliberate in the doc comment so it
does not read as a build failure.

Deleting the file entirely would drop the directory from `go list ./...`, and
with it from `-coverpkg`, from `go vet ./...` and from the derived fuzz sweep.
One documentation file is cheaper than four exclusions to remember.

## Consequences

**Every caller changes, and imports three packages where useful.** `config` to
write the quota, `client` to make the request, `limiter` to match the error.
That is the cost, it is the largest in any release so far, and every break is a
compile error.

**Unlike v0.11.0, this is not spelling-only.** That release moved type
*aliases*, so a value crossed the boundary unchanged. `Client`, `Request`,
`Response`, `ErrBodyTooLarge` and the vocabulary are declarations moving between
packages: a caller holding a `*limiter.Client` in their own struct field has a
type change. `ErrBodyTooLarge` in particular is a sentinel *value* that moved,
which is safe only because the old declaration is deleted rather than left
behind — a compatibility shim there would compile and be permanently false.

**Six test files stay in `limiter/` and import `client`.** They need both a
white-box seam and the request path, and `limiter/export_test.go` is
`package limiter`, so its seams exist only inside `limiter`'s own test binary.
The same arrangement ADR 0006 documented, one package further out.

**Test fixtures are duplicated between `limiter_test` and `client_test`.**
Deliberately: Go's test packages cannot export to one another, and a shared
`pacetest` package would be filtered out of `-coverpkg` by the `grep -v test$`
in the coverage command — silently unmeasured. Each copy is the minimum that
package calls, with a header saying why.

**The fuzz matrix is derived, not written down.** `FuzzLimitString` followed
`Limit` to `config` and `FuzzRetryAfter` followed `Response` to `client` — the
third release running in which a fuzz target moved. The previous breaks failed
loudly because the old package had been deleted; a target that merely moves does
not, since `go test ./pkg/ -fuzz='^Gone$'` prints "no fuzz tests to fuzz" and
exits 0. Both `make fuzz` and CI now ask `go test -list='^Fuzz'` where the
targets are and fail if the count is zero.

**Examples: 1 in `config`, 9 in `client`, 1 in `limiter`.** `ExampleLimitError`
is the one that resists — `LimitError` is declared in `limiter` but provoking a
throttle means making a request — so it lives in `limiter/example_test.go`
importing `client`. `ExampleConfig_quotaFor` became `ExampleConfig_Quota`: a
lowercase suffix is never validated by `go vet` at all, so the old name had been
naming an unexported method with nothing complaining.

## Alternatives considered

**Put `Config` in `client` and have two packages.** Then `config.Config` is
`client.Config`, and a caller writing a configuration imports the package that
makes requests. It also puts validation and assembly in one file again, which is
what ADR 0006 separated. The vocabulary would have to go somewhere else anyway,
since `client` imports `limiter`.

**One package holding everything.** ADR 0008's second "coherent layout": zero
aliases, one import, ~2,400 non-test lines and nineteen files in the repository
root. Rejected on the same ground ADR 0006 used, and the `pace/limiter` import
path would disappear.

**Move every `Spec` into `config` so it holds literally all configuration.**
`gate.Spec`, `shared.Config` and `transport.Config` could move; `registry.Spec`
cannot, because four of its callbacks name its own types. (`limiter.Spec` was on
the "cannot" side of this list and should not have been — it moved in the
amendment below.) Moving the other three would drag `net/http`, `bucket` and
`crypto/tls` into `config`, and "all settings except one" is still a worse rule
than "the settings a caller writes".

**Keep the root as a thin forwarder** — `func New(config.Config) (*client.Pool,
error)`. One import for the simple case, and it reintroduces exactly the facade
ADR 0008 removed: a name declared in one package and published from another.

## Amendment (v0.12.0), superseded below: `Spec` lives in `config` after all

The decision above states that `limiter.Spec` cannot live in `config`. That is
false, and the reasoning given for it does not support it.

`Spec`'s ten fields are `config.Quota`, `func() time.Time`, `*slog.Logger`,
`*observe.Observer`, `int`, three `time.Duration`s, `store.Store` and
`shared.Config`. **None of them is a `limiter` type.** `config` already imported
`observe`, `shared` and `store`, so moving the type there needs no new import
and creates no cycle in either direction.

What the original reasoning actually established is a narrower claim: a method
`func (Config) Spec() limiter.Spec` names a type in the package that imports
`config`, so it would close a loop. True — and it is about *that method*, not
about where the type is declared. The two got conflated, and the false one got
the bold text.

Correcting it turns out to buy the thing the false claim said was impossible:

```go
func (cfg Config) Spec() Spec   // same package, ordinary method
```

`client.New` shrinks from a ten-field literal to `limiter.New(cfg.Spec())`, and
`config` genuinely holds all of the configuration rather than most of it.
`Spec.validate` becomes exported `Spec.Validate`, because `limiter.New` calls it
across a package boundary now — which is honest: the vtable's contract is
exactly what a caller assembling the pieces by hand needs to be able to check.

Two consequences worth stating:

- **The panics say `config:` now, not `limiter:`.** They are raised from
  `Spec.Validate`, and a reader chasing the message should land where the fields
  are documented. They also name the field as `Spec.Quota` rather than `Quota`,
  since `config` has a `Config.QuotaFor` too and a bare `Quota` was ambiguous.
- **The vtable is no longer declared by the package that consumes it**, which is
  a real change to what [ADR 0006](0006-the-root-is-the-composition-root.md)
  described. The engine used to say what it needed; now the configuration
  package says it and the engine accepts it. That is the cost, and it is paid
  for by the two types a reader must distinguish — `Config` and `Spec` — sitting
  side by side under one doc, with `Config.Spec` between them as the only
  translation. `registry.Spec` and `gate.Spec` keep the old arrangement, so both
  patterns are now in the tree; the rule that decides which is whether the
  vtable names any of its consumer's types.

The test split follows the type: `config/spec_test.go` has the eight-case panic
table and needs no engine to run it, and `limiter/zero_test.go` keeps the one
property that is still the engine's — that `New` consults the vtable at all.

## Amendment (v0.12.0), final: there is no `Spec`

The amendment above moved `limiter.Spec` into `config` and gave `Config` a
`Spec()` method. Both are now deleted, because moving the type made the real
question visible: **what was the vtable for?**

Ten fields. Ten of them the same field under the same name:

```go
Spec{Quota: cfg.Quota, Logger: cfg.Logger, Observer: cfg.Observer,
     Shards: cfg.Shards, IdleExpiry: cfg.IdleExpiry, /* … */}
```

Only one entry was a translation at all — `Now: cfg.Clock.Now` — and inside the
engine that is `l.cfg.Clock.Now()` instead of `l.cfg.Now()`, nine call sites of
one extra selector. So the type restated the Config, and the method restated the
type, and `client.New` said `limiter.New(cfg.Spec())` where it could say
`limiter.New(cfg)`.

`limiter.New(cfg config.Config)`. `config/spec.go`, `Config.Spec` and
`config/spec_test.go` are gone; `limiter/validate.go` is the engine's own check,
which puts the requirements back with the package that has them and closes the
ADR 0006 regression the previous amendment opened.

Three things fell out of it:

- **One panic arm was guarding the impossible.** `Spec.Quota` was a `func` field
  and could be nil, so `Validate` checked it. `Config.Quota` is a *method* — a
  method value is never nil. The check is deleted, not moved.
- **`Spec.Validate` is unexported again**, as `validate(cfg)`. It was only
  exported because `limiter.New` had to call it across a package boundary; there
  is no boundary now.
- **`limiter` reads six of Config's sixteen fields**, and the four that describe
  HTTP are not among them. The vtable used to make that structural — it simply
  had no `BaseURL`. Now it is `limiter/httpfree_test.go`, a source scan asserting
  the four identifiers appear nowhere in the package's non-test files. Verified
  by mutation: adding `return l.cfg.BaseURL` to `stats.go` fails it, naming the
  file and the field.

That last point is the whole cost, and it is worth being plain about the trade:
a compiler guarantee became a test. The guarantee was real, and it was not worth
a second struct restating the first — the call site reads better, and a reader
following `client.New` into `limiter.New` now meets the same `Config` they wrote
instead of a second type that mirrors it.

**Two amendments in one release is a signal, not a triumph.** The first was
prompted by "can Spec live in config?", which was the wrong question; the right
one was "does Spec need to exist?". The general form, worth keeping: *when a
type's construction is N fields of `X: src.X`, the type is the source with extra
steps.*
