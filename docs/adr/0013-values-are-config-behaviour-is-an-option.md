# ADR 0013 — Values are configuration, behaviour is an option

**Status:** accepted (v0.2.0). Amends
[ADR 0012](0012-one-hook-holds-the-quota.md).

## Context

ADR 0012 made `config.Config.QuotaFor` — a `func(key string) bucket.Quota` —
the only place a rate was configured. It was right about the problem it solved:
there had been three ways to say a rate and a rule about which one won.

It was wrong about where the answer belonged. Two things came out of it badly:

**`Config.Resolve` stopped being able to check a rate.** Its own ADR admitted
this: *"a typo that used to fail at `client.New` now throttles one key to a
standstill and writes a log line."* A configuration struct whose central field
cannot be validated at construction is not doing the job a configuration struct
is for. The flat case — every key gets 60/min — is the overwhelming majority of
uses, and it was paying the run-time failure mode of a feature it did not use.

**The flat case read as ceremony.** `QuotaFor: config.Fixed(bucket.Quota{...})`
wraps a value in a closure so the library can call it back and get the value
again. `config.Fixed` existed to hide that, which is a sign the shape was wrong:
a convenience whose whole job is to make a hook look like a field means the
field was what was wanted.

## Decision

**`Config` holds values. Behaviour is passed as an option.**

```go
type Config struct {
	BaseURL string
	Quota   bucket.Quota   // required, checked by Resolve
	…
}

func client.New(cfg config.Config, opts ...config.Option) (*Pool, error)
func limiter.New(cfg config.Config, opts ...config.Option) *Limiter
```

`config.Config.Quota` is a value: a caller writes it down, and `Resolve` checks
it before anything runs. `config.WithQuotaFor` is the option that grades keys
into tiers.

`config.Fixed` is deleted — the field is the flat case.

### The override is handed what it overrides

This is the part that keeps ADR 0012's result while undoing its shape:

```go
func WithQuotaFor(fn func(key string, def bucket.Quota) bucket.Quota) Option
```

`def` is `Config.Quota`. There is no precedence rule to document, no zero field
that means "inherit", and no second source to disagree with the first — the
value being overridden is an argument to the thing overriding it. A caller who
wants the default returns `def`.

ADR 0012 removed a fallback rule that was written out in eight places. That rule
does not come back here. What comes back is the validated field it was written
against.

### Where validation lives now

| | checked by | failure |
|---|---|---|
| `Config.Quota` | `Config.Resolve`, and `limiter.validate` | `*config.Error` from `New`, or a panic on a hand-built Config |
| what `WithQuotaFor` returns | `limiter.quotaFor` | clamped and logged at warn |

The split is the point. The value a caller writes down fails loudly at
construction. The value a hook produces cannot — it arrives one key at a time,
on the goroutine building that key's bucket — so it fails closed and says so.
ADR 0012 applied the second rule to everyone; this applies it only to callers
who asked for a hook.

### Why `Option` lives in `config`

Both `New` functions take options, so a type in either package would need a copy
or an import edge in the wrong direction. `config` is documented as "everything
a caller of pace configures", and an option is something a caller configures.
The package is now: `Config`, `Clock`, `Error`, `Option`, `Options`,
`WithQuotaFor`, `Apply`.

`Options` is exported because `limiter` reads it. `Apply` is exported because
`limiter.New` and `client.New` both fold a list into one, and folding it twice
is how the two would come to disagree.

## Consequences

**Every caller changes.** `QuotaFor: config.Fixed(q)` becomes `Quota: q`, and a
graded `QuotaFor` becomes a `WithQuotaFor` option with `def` in place of the
hand-written fallback.

**`Config` is fourteen fields, all values.** Nothing in it is a func except
`Clock` and `Observer`, which are injected collaborators rather than per-key
decisions, and neither answers a question `Resolve` should have been able to
check.

**The signature of both `New`s changed**, which is a wider break than the quota
work: it is the boundary `client/new_test.go` pins in a single line.

**Runtime rate changes now require a hook.** `Config.Quota` is fixed once `New`
has run, so an operator-adjustable rate means passing `WithQuotaFor` and
swapping what it reads. That is a deliberate narrowing: a process that never
needed a live rate no longer carries the machinery for one, and one that does
says so at construction.

## Alternatives considered

**Keep `QuotaFor` in `Config` and add options for other things.** Leaves the
central field unvalidatable, which is the defect this fixes.

**`WithQuotaFor(fn func(key string) bucket.Quota)`, no `def`.** Then a hook that
wants the default has to close over a copy of it, and `Config.Quota` becomes
dead whenever the option is present — two sources and a silent one, which is
exactly what ADR 0012 was written against.

**`Config.Quota` plus a zero-means-inherit hook.** The eight-places rule, back
again. Passing `def` costs one parameter and removes the rule entirely.

**Options for `Clock`, `Logger`, `Store`, `Observer` too.** Defensible, and a
larger change than the problem called for. They are values a caller writes once
and `Resolve` defaults; none of them is a per-key decision. Revisit if a second
genuinely behavioural setting appears.

## Amendment (v0.3.0)

The second behavioural setting appeared: `WithCookieJarFor`, which scopes
cookies to a key where `Config.CookieJar` is one jar for the Pool. It follows
this record exactly — a per-key decision, supplied as code, handed the value it
overrides (`Config.CookieJar`) as its `def` argument, unable to be checked by
`Resolve` and therefore not a field. It is also the first option read by
`client` rather than `limiter`, which exercised the sentence above about both
`New`s folding the same list: `client.New` now calls `config.Apply` too, and
each side reads its own fields of the one `Options`. The package surface grows
by one name: `WithCookieJarFor`.

The general options for `Clock`, `Logger`, `Store` and `Observer` remain
unwarranted, for the reason already given: none of them is a per-key decision.
