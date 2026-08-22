# ADR 0008 — The root re-exports nothing

**Status:** accepted (v0.2.0). One claim superseded by [ADR 0009](0009-config-limiter-client.md) — see the amendment at the end.

## Context

[ADR 0006](0006-the-root-is-the-composition-root.md) made the root the
composition root: `Config` and its validation live there, and `New` resolves one
into the vtable the engine takes. It left the other half of the root's job
alone — the facade. Ten names were re-exported from `pace/limiter`:

- four type aliases: `Limit`, `Quota`, `Reservation`, `LimitError`
- `const Inf`
- four wrapper funcs: `PerSecond`, `PerMinute`, `PerHour`, `Every`
- `var ErrClosed`

[ADR 0007](0007-contracts-carry-numbers-not-types.md) had stated the reason
plainly: *"The root re-exports the vocabulary so a caller still writes one
import."* That is a real benefit and it was paid for with a real cost.

**Go renders a type alias as a single line with no methods and no fields.**
`go doc pace Limiter` printed `type Limiter = limiter.Limiter` and stopped. This
is not a new discovery: v0.6.0 put sixty-three aliases in the root over one
internal package, and v0.7.0 spent a release unwinding them for exactly this
reason. The four biggest ones survived that unwinding.

So the facade documented nothing and sent the reader one package over anyway.
The caller who never leaves `pace` is the one the re-export was for, and they
are also the one who cannot find out what a `Limiter` does.

v0.11.0 first tried the other repair: hoist the request path *up* to the root so
those four names could be declared there. That works, and it costs a wrapper
struct — a root `Limiter` holding the engine plus five HTTP fields, with four
one-line forwarding methods — because `lim.Client()` has to keep working and a
method on `limiter.Limiter` cannot return a root type. Trading four undocumented
aliases for one wrapper and four forwards is not obviously a trade at all.

## Decision

**Every name in this library is declared exactly once. The root holds `Config`,
`Clock`, `ConfigError` and `New`, and re-exports nothing.**

```go
lim, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    limiter.PerMinute(60),
})              // *limiter.Limiter — the engine's own type, not a wrapper
alice := lim.Client("alice")        // *limiter.Client
resp, err := alice.Get(ctx, "/x")   // *limiter.Response
```

**An alias is for a type whose owner is elsewhere. It is not a way to publish a
name in two places.** That is the rule this settles, and it leaves the module
with zero aliases.

The request path stays in `pace/limiter` with the engine, because Go forces it
to. `pace.New` returns `*limiter.Limiter`, so `lim.Client(userID)` is a method
on the engine's type, and it cannot return a type declared in the root without
`limiter` importing `pace`.

### What cannot move, and why an interface does not rescue it

Checked file by file before this was decided, and recorded because the question
will be asked again:

- **`rate.go` cannot move to the root.** `Spec.Quota` returns a `Quota` and
  `LimitError.Limit` is a `Limit`. That is a *data* cycle, and
  [ADR 0007](0007-contracts-carry-numbers-not-types.md) already sets out why an
  interface does not break one: an interface breaks a *behaviour* cycle, and
  `float64(time.Second) / float64(q.Rate)` does not become possible by declaring
  a method set. You cannot divide by an interface.
- **`lifecycle.go` cannot move.** Go forbids declaring a method on a type from
  another package, and `Close` must stay a method on the engine's `Limiter`.

Together these mean the engine can never be dissolved into the root by halves.
The only two coherent layouts are the one above and a single package holding
everything — which [ADR 0006](0006-the-root-is-the-composition-root.md) rejected
under "Move the engine into the root", and which is now ~2,400 non-test lines
and nineteen files in the repository root.

*(That last paragraph is the one thing here v0.12.0 falsified. There is a third
layout: give the HTTP half its own package. See the amendment.)*

## Consequences

**A caller imports two packages.** This is the whole cost and it is not small:
`pace.Config{Rate: limiter.PerMinute(60)}` names two packages in one literal,
and `Config.Rate` is typed `limiter.Limit`. Every existing caller edits an
import line and a spelling.

Because the deleted names were *aliases*, the change is spelling-only and never
type-identity. Code already written as `limiter.PerMinute(60)` needs no edit at
all, and every break is a compile error rather than a silent one.

**`go doc` says something for every name.** The four types that documented
themselves as one line each now document their methods where a reader lands.

**`pace/response` is gone.** It was a package for one reason — the root aliased
`Response` and the engine returned it, so neither could import the other. With
no alias the reason evaporates, and it folds into `limiter/response.go`. Its
`response.New` went with it: a public constructor for a type a caller only ever
*receives*. A `Response` is a report, not something to assemble.

**`facade_test.go` is `new_test.go`.** Most of it was compile-time declarations
pinning aliases in both directions, and they now assert nothing. What survives
is one line — `var _ func(pace.Config) (*limiter.Limiter, error) = pace.New` —
which is the entire boundary, plus the four end-to-end tests of what `New`
wired. `TestASentinelMatchesWhatTheLimiterReturns` was deleted outright: there
is no root sentinel left that could disagree with the engine's.

**Every example naming a `Client`, `Limiter`, `Response` or `LimitError` moved
to `limiter/`.** Ten of eleven; `ExampleConfig_quotaFor` stays. This is the trap
worth writing down, because `go vet` does not catch it here:
`checkExampleName` resolves the identifier against the *test package's imports*,
not the documented package, so a stranded `ExampleClient_Get` in `pace_test`
resolves happily to `limiter.Client` — and `go/doc` then attaches it to nothing
and renders it nowhere. A lowercase suffix (`_quotaFor`) is never validated at
all. The check that works is: for every `ExampleX`, `X` must be declared in a
non-test file of the same directory.

**Coverage rose to 97.0%** from 96.8%, because deleting the facade deleted
declarations that were never behaviour.

## Alternatives considered

**Keep the wrapper, drop only the ten re-exports.** Preserves the engine/HTTP
split v0.11.0 first shipped, and leaves a root `Limiter` whose four exported
methods are one-line forwards. The split is worth something — a rate limiter
that does not import `net/http` is a cleaner object — but not a wrapper type
whose entire job is to have the same name as the thing it holds.

**Collapse `limiter` into the root.** Zero aliases, zero wrapper, one import —
the maximal form of this decision. Rejected on the same ground ADR 0006 used:
nineteen non-test files and ~2,400 lines in the repository root, plus twenty-six
test files beside them. The `pace/limiter` import path would also disappear.

**Re-export with `var PerMinute = limiter.PerMinute`.** Not considered
seriously; ADR 0007 already rejected it because a caller could reassign it. It
would not have fixed the aliases, which are the actual problem.

## Amendment (v0.12.0): there was a third layout

"The only two coherent layouts" was wrong, and the way it was wrong is
instructive. The constraint recorded above is real — `lim.Client(userID)` cannot
return a type declared in the root — but it is a constraint on **where the
return type lives**, not on where the request path lives. Put the request path
in `pace/client` and `Pool.Client` returns a `*client.Client`: same package, no
cycle, nothing dissolved into the root.

[ADR 0009](0009-config-limiter-client.md) takes it. Everything else here stands,
and two parts of it get stronger:

- **The root re-exports nothing** — it now declares nothing at all, which is the
  same rule with nothing left to break it.
- **The alias rule** — an alias is for a type whose owner is elsewhere — is what
  stopped a `client.Limit = config.Limit` convenience from being added on the
  way. A caller writing `config.PerMinute(60)` inside a `config.Config` literal
  is not paying for a second import.

The rejected alternative "**Keep the wrapper, drop only the ten re-exports**" is
adopted in substance, and the reason it was rejected is answered rather than
ignored: the objection was "a wrapper type whose entire job is to have the same
name as the thing it holds". `Pool` does not share a name with `Limiter`. It is
a different thing — an HTTP client that consults a limiter — and it has state of
its own to prove it.
