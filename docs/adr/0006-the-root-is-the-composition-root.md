# ADR 0006 — The root is the composition root

**Status:** accepted (v0.8.0)

## Context

pace is fifteen-odd packages that assemble into one Limiter. Something has to do
the assembling, and where that happens had drifted twice.

v0.6.0 put sixty-three aliases in the repository root over one internal package.
That satisfied "three files in the root" and changed nothing about how the
library was organised, at the cost of the published API documentation: Go
renders a type alias as a single line with no methods and no fields, so
`go doc` showed nothing for any of them.

v0.7.0 inverted it. Every package became public and documented, and the root
became a facade of ten names — nine aliases, six sentinel variables, and

```go
func New(cfg Config) (*Limiter, error) { return limiter.New(cfg) }
```

That is a forwarding function, not a composition. `limiter.New` did the
assembly, so `limiter.Config` was simultaneously the configuration a caller
writes — optional fields, validation, defaults — and the input to the engine.
Two jobs in one struct, and the package that held it was also the package with
all the behaviour.

Meanwhile four sub-packages had converged on a different shape. `registry`,
`gate`, `persist` and (while it existed) `runner` each take a `Config` that is a
*vtable*: every field required, nothing defaulted, `New` panicking on a value it
cannot work with, and the fields holding *answers* rather than the owner's
types. `registry.Config.QuotaFor` returns `(float64, int)` rather than a
`rate.Quota` precisely so that the package need not import `rate`. (`rate` was
absorbed into `limiter` in v0.9.0; the rule it illustrates is unchanged.)

`limiter` was the only assembled-from-parts package not shaped that way.

## Decision

**The root package is the composition root. `Config`, its validation and its
defaulting live there, and `New` assembles the engine. The engine takes a
vtable like every other package built from parts.**

*(v0.9.0: that vtable was originally called `limiter.Config`. It is
`limiter.Spec` now — see the amendment at the end.)*

```go
func New(cfg Config) (*Limiter, error) {
    if err := cfg.validate(); err != nil { return nil, err }
    cfg = cfg.withDefaults()
    return limiter.New(limiter.Spec{ /* … */ }), nil
}
```

The translation is the whole of the difference between the two structs, and it
follows the house rule of passing the answer rather than the type:

| `pace.Config` | `limiter.Spec` |
|---|---|
| `Transport http.RoundTripper` | `HTTPClient *http.Client` |
| `Clock Clock` | `Now func() time.Time` |
| `Rate`, `Burst`, `QuotaFor` | `Quota func(userID string) limiter.Quota` |

`limiter.New` no longer returns an error. Nothing in it can fail once the front
door has validated, and a vtable's contract is that a bad value is a wiring bug
— so it panics naming the field, as `registry`, `gate` and `persist` do.

### What is assembled where

The rule is not taste, and it is worth stating because it decides every case:

> A piece is built at the root if it can be built before the Limiter exists.

- **Root:** the `*http.Client` (from `Transport`), and the resolved `Config`.
- **`limiter.New`:** the `registry.Registry` and the `gate.Gate`. Both take
  `Config` fields that are *methods on the Limiter* — `registry.Config` wants
  ten of them, `gate.Config` three — and a method value cannot be taken before
  the receiver exists.
- **`limiter.New`, though it need not be:** the `persist.Adapter`, which has no
  callbacks and could be built at the root. It is not, because `limiter` still
  holds the `store.Store` itself for `Close` and for the test seam that swaps
  one in, and splitting the two would put the store in two places.

## Consequences

**The root is four files** — `doc.go`, `pace.go`, `config.go` and the tests.
`pace.go` reads top to bottom as the assembly, which is the question a reader
arriving at the repository actually has.

**Eleven fields are declared twice.** This is the real cost and it is not
hidden: `pace.Config` documents each field for a caller, `limiter.Spec`
documents what the engine does with it. The three translations above are what
keep it from being fourteen. The alternative — one struct doing both jobs — is
what this ADR is moving away from.

**`pace.Config`, `pace.Clock` and `pace.ConfigError` stop being aliases.** They
are declared at the root now, because validating and defaulting a configuration
is the front door's job. `facade_test.go` lost three alias pairs and gained a
comment saying why asserting them would now assert the opposite of the truth.
Every other re-exported name is still an alias, and still checked.

**The engine's tests build through the front door.** `limiter/*_test.go` is an
external test package (`package limiter_test`), so it may import the root even
though the root imports `limiter`; the test binary rebuilds the root against the
test-augmented engine, so the white-box seams still see the same Limiter the
root handed out. That is the right dependency anyway: a Limiter assembled any
other way is not one a caller can get.

## Alternatives considered

**Leave `Config` in `limiter` and compose at the root anyway.** The root would
alias the engine's `Config` while also taking it apart to build the pieces, which
reads as though the front door does not trust its own type. It also leaves
`limiter` as the one assembled package with a non-vtable `Config`.

**Move the engine into the root.** No duplicated fields at all, and roughly
1,800 lines and fourteen files in the repository root — the shape v0.6.0 was
explicitly moving away from.

**Generate one struct from the other.** Rejected without much thought: code
generation to save eleven field declarations is a build step, a generator to
maintain, and a diff nobody reads.

## Amendment (v0.9.0): the vtable is `limiter.Spec`

Naming it `Config` kept the convention `registry`, `gate` and `persist` follow,
and it was the wrong call for this one package. Those three are vtables a
caller never meets; `limiter` is the one a caller might import alongside the
root, and there they would find two types named `Config` with eleven field
names in common and no hint which was which.

The convention holds where it costs nothing and is dropped where it collides.
`registry.Config`, `gate.Config` and `persist.Config` are unchanged.
