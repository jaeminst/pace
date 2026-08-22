# ADR 0007 — A contract carries numbers, not pace's types

**Status:** accepted (v0.9.0)

## Context

`pace/rate` was 99 lines declaring four things a caller writes — `Limit`,
`Quota`, and the constructors `PerSecond`/`PerMinute`/`PerHour`/`Every` — plus
`Inf` and `Finite`. It existed as its own package because five other packages
shared it, and a package shared by five cannot live inside any one of them.

That is the textbook argument for a leaf type package, and it was correct as far
as it went. What it did not weigh is who pays. Two of the five are **contract
packages**: `observe`, which a caller reads events from, and `shared`, which a
third party implements a backend against. Each named exactly one `rate` type, in
exactly one struct field:

```go
// observe
type ThrottleInfo struct { …; Limit rate.Limit; Burst int }

// shared
type TakeRequest struct { …; Quota rate.Quota }
```

The third is `gate`, which does arithmetic on the value: `q.Rate != rate.Inf`,
`q.Rate <= 0`, `float64(time.Second) / float64(q.Rate)`.

So a caller writing a Redis backend for `shared.Quota` — a package whose whole
pitch is "implement two methods against what you already run" — compiled
`pace/rate` to read two numbers out of a request.

## Decision

**De-type the two contract fields, drop `gate.Enabled`, and absorb `rate` into
`limiter`. The root re-exports the vocabulary so a caller still writes one
import.**

*(Amended in v0.11.0: the second sentence is reversed. The root re-exports
nothing. See [ADR 0008](0008-the-root-re-exports-nothing.md).)*

*(Amended in v0.13.0: the vocabulary is in `pace/bucket` now — see
[ADR 0011](0011-the-vocabulary-belongs-to-the-bucket.md). The test below is the
one that let it move twice, and it still holds: `bucket` imports nothing of
pace's, so no package a third party implements against compiles it.)*

*(Amended in v0.12.0: the vocabulary was in `pace/config` then, not `limiter` —
see [ADR 0009](0009-config-limiter-client.md). The absorption this ADR argued
for still stands: what it removed was a leaf package that **five** packages
compiled, two of them contract packages a third party implements against. The
de-typing below is why `config` is not that package again — it is imported by
`limiter` and `client`, both of them pace's own, and by nothing a third party
writes. The test to apply: the vocabulary may live wherever it reads best,
provided no package implemented against from outside has to compile it.)*

| | before | after |
|---|---|---|
| `observe.ThrottleInfo.Limit` | `rate.Limit` | `float64`, documented as per second |
| `shared.TakeRequest.Quota` | `rate.Quota` | `Rate float64` + `Burst int` |
| `gate.Take`/`Allow`/`Acquire` | take a `rate.Quota` | take the two numbers |
| `gate.FallbackDelay` | takes a `rate.Quota` | takes the rate |
| `gate.Enabled` | `q.Rate != rate.Inf` | deleted; the caller compares |

`gate.Enabled` is deleted rather than de-typed because `Inf` is
`math.MaxFloat64` and belongs to whoever owns `Limit`. Keeping the function
would have meant `gate` comparing against a bare `math.MaxFloat64` — the same
decision, spelled so that nothing explains it. `limiter.sharedEnabled` reads
`q.Rate != config.Inf` instead, which is one line where the comparison is
legible. (It read `q.Rate != Inf` until v0.12.0, when the constant moved to
`config`; the argument for deleting `gate.Enabled` is unaffected — `gate` would
still be comparing against a bare `math.MaxFloat64`.)

An interface cannot break these cycles, and it is worth writing down why, because
"introduce an interface" is the reflex. An interface breaks a cycle when the
lower package needs the upper one to *do* something. It cannot break one when the
lower package needs a *value* the upper one's type defines. `float64(time.Second)
/ float64(q.Rate)` does not become possible by declaring a method set.

## Consequences

**A contract package now imports nothing of pace's** except where a type is
genuinely shared — `queue` was the last such case and went in v0.8.0. `observe`
and `shared` are declarations over standard-library types.

**`observe.ThrottleInfo.Limit` loses `Limit.String`,** which rendered `"60/min"`.
A metrics pipeline emits the number, so the loss lands on log lines, where the
caller can format it. This is the one real cost.

**`shared.TakeRequest` loses its nesting.** A backend author reads `req.Rate` and
`req.Burst` rather than `req.Quota.Rate` and `req.Quota.Burst`. `shared.Quota` is
already the name of the backend interface, which is why the field was typed from
another package in the first place; two flat fields need no name at all.

**Both are frozen at v1 and this is not reversible** once tagged. That is the
reason it happened now rather than later.

**~~A caller imports one package.~~** *(Reversed in v0.11.0; three as of
v0.12.0 — `config` to write the quota, `client` to make the request, `limiter`
to match the error.)* `PerMinute(60)`,
`Limit`, `Quota` and `Inf` were re-exported from the root — the constructors as
wrappers rather than `var PerMinute = limiter.PerMinute`, so a caller could not
reassign them. They are `limiter.`-qualified now, and a caller imports both
packages.

`Finite` was never re-exported: it maps a true infinity onto the value the
bucket can hold, which is plumbing rather than vocabulary, so the root called
`limiter.Finite` by name. As of v0.12.0 it is declared in `config` beside the
`Config.Resolve` that calls it, which is where plumbing for a written value
belongs — the paragraph's conclusion inverted along with its subject.

**One fewer package**, 18 rather than 19.

## Alternatives considered

**Merge `rate` into `bucket`.** No de-typing, no cycle: `bucket` is a leaf, and
`observe`, `shared` and `gate` could all import it. Rejected because it makes
every contract package depend on the token-bucket implementation and, through
it, on `golang.org/x/time` — a worse trade than the one taken, since the point
was to stop contract packages compiling pace's code.

**Duplicate the type in each package that needs it.** Three `Limit float64`
declarations and a conversion at every boundary. Strictly worse than one shared
package, which is what we started with.

**Leave `rate` alone and re-export from the root.** This was the original plan
and it solved the ergonomic half — a caller wrote `pace.PerMinute(60)` either
way. It leaves the dependency: `shared` still compiles `rate`, and a directory
still exists for 99 lines. Reasonable; the trade above was taken deliberately
over it.

*(v0.11.0 note: half of this alternative has since been vindicated and half
buried. The re-export was dropped — so the ergonomic argument that made this
option attractive turned out not to be one the library wanted to pay for. The
absorption it argued against was the right call and stands.)*
