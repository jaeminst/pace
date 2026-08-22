# ADR 0012 — One hook holds the quota

**Status:** amended by
[ADR 0013](0013-values-are-config-behaviour-is-an-option.md). Supersedes
[ADR 0010](0010-the-default-quota-is-live-state.md).

> **Amended.** The counting below stands and so does its conclusion — one
> answer to "what is this key's quota", no fallback rule. What did not stand is
> putting that answer in a `Config` field: a hook cannot be checked by
> `Config.Resolve`, and the cost of that is named in this ADR's own
> Consequences. ADR 0013 keeps the result and moves the hook to an option,
> handing it the configured value as an argument so no precedence rule returns.

## Context

A rate could be written in three places and changed in two:

| | |
|---|---|
| `config.Config.Rate` / `Burst` | the initial default, required |
| `config.Config.QuotaFor` | a per-user override, optional |
| `limiter.Limiter.SetDefaultQuota` | the live default, added one release earlier |

Three ways to say one thing needs rules about which wins, and this had them. A
zero field in a `QuotaFor` result selected the default; the default started as
`Config.Rate` and moved from there; `Config.Rate` was still required even when
`QuotaFor` answered for every user. The rule was written out in eight places
across code, examples and tests, and `bucket.Quota` — the vocabulary type, which
imports nothing of pace's — carried a doc comment explaining a `config` fallback
in order to state it.

The counting that started this:

- **`config.Config.Quota(userID)`** had no production caller. It resolved a
  user's quota against the *written* default, so after any `SetDefaultQuota` it
  answered a question the Limiter answered differently. An exported method whose
  only job was to be wrong.
- **`Limiter.cfg.Rate` and `Limiter.cfg.Burst`** were read at exactly one line —
  the seed in `New` — and never again. The struct they sit in is documented
  "immutable after New", which invites a reader to treat them as authoritative.
  They stop being so the moment the default moves.
- **`registry.Spec.QuotaFor`** returned `(float64, int)`, so a `bucket.Quota`
  was taken apart and put back together on the create path and again on the
  reload path.
- **`bucket.Quota` meant two things**: an absolute pair coming out of
  `Bucket.Quota`, and a partial override going into `QuotaFor`.

None of these were bugs. Together they were four places to look for one number.

## Decision

**`config.Config.QuotaFor` is the only place a rate is configured.**
`Config.Rate`, `Config.Burst`, `Config.Quota`, `Config.QuotaWith`,
`Limiter.DefaultQuota` and `Limiter.SetDefaultQuota` are all deleted.

A function of a user ID already expresses everything the deleted fields could:
a flat rate is a function that ignores its argument. The reverse was never true,
which is why the flat fields needed the hook beside them in the first place.

`config.Fixed(q)` returns the closure for the flat case, so the common
configuration stays one line. It is a convenience over the one hook, not a
second place to configure a rate — a Config using it has exactly one answer to
"what is this user's quota", the same as one that does not.

### What the fallback rule cost

`QuotaFor` now returns an absolute quota, zero fields included. A caller with a
map writes their own fallback:

```go
free := bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}
cfg.QuotaFor = func(userID string) bucket.Quota {
	if q, ok := (*tiers.Load())[userID]; ok {
		return q
	}
	return free
}
```

That is four lines where it was one, and it is the real cost of this decision.
What it buys: `bucket.Quota` means one thing, the vocabulary package stops
documenting a `config` rule, and the eight restatements of "zero selects the
default" go with it.

### One type from the hook to the bucket

`registry.Spec.QuotaFor` returns `bucket.Quota`, and `bucket.NewBucket`,
`RestoreBucket` and `SetQuotaAt` take one. The de-typing was
[ADR 0006](0006-the-root-is-the-composition-root.md)'s, so that `registry` need
not import the vocabulary — and v0.13.0 moved the vocabulary into `bucket`,
which `registry` already imports. The premise was gone; the round trip was not.

[ADR 0007](0007-contracts-carry-numbers-not-types.md) still holds. Its argument
was about **contract packages** — `observe` and `shared`, which a third party
reads events from or implements a backend against — and those still carry plain
`float64` and `int`. `registry` was never one of them.

### Where validation went

`Config.Resolve` rejected a `Rate` at or below zero, and a NaN, at construction.
It cannot any more: at Resolve time there is no rate, only a function. The quota
arrives one user at a time, from caller code, on the goroutine building that
user's bucket.

So the check moved to `limiter.quotaFor`, and its failure mode changed. There is
no call to return an error from, so an unusable rate **fails closed**: it is
clamped to zero, which is a bucket that never refills, and logged at warn level
naming the user. Resolve still checks what it can — that the hook is present at
all — and `limiter.validate` panics on a nil one.

**This is a real loss and it is worth naming.** A typo that used to fail at
`client.New` now throttles one user to a standstill and writes a log line.
Failing closed is the safe direction and a NaN never reaches the arithmetic, but
the error moved from construction to run time and from loud to quiet.

The clamp is applied in `quotaFor` rather than left to `bucket`, because a user
with no bucket yet is answered from `quotaFor` directly — reporting the rate a
caller wrote while enforcing the one the bucket floors it to would be two
answers to the same question, which is the thing this ADR is about.

## Consequences

**Every caller changes.** `Rate:`/`Burst:` become
`QuotaFor: config.Fixed(bucket.Quota{…})`. A compile error, and mechanical.

**Runtime adjustment keeps its semantics.** ADR 0010 argued that the default has
to be changeable in a running process; that requirement stands, and swapping
what `QuotaFor` reads satisfies it identically — new users pick the change up
without a reload because they are about to call the hook, and users already in
memory wait for `ReloadQuotas` or `ReloadQuota`. What is gone is the *second*
way to do it. ADR 0010's normalisation-in-two-places problem goes with it: there
is one normalisation now, in `quotaFor`.

**Two of the three copies survive, and should.**

- `bucket.Bucket.quota` still shadows `rate.Limiter`'s own limit and burst.
  `x/time/rate` exposes them only through two separately locked accessors, so
  reading both gives pairs nobody configured — the defect v0.13.0 fixed. One
  writer, `SetQuotaAt`, keeps them equal.
- `LimitError.Limit`, `ThrottleInfo.Limit` and `shared.TakeRequest.Rate` are
  *reports*. Nothing reads them back, so they cannot drift.

**`bucket.SetQuotaAt`'s ordering claim was wrong and is corrected.** It said
publishing the pair last means "a report never promises more than the bucket is
enforcing". That holds for a raise and is backwards for a lowering, and
publishing first only swaps which direction is wrong. No ordering removes the
step. What the ordering actually buys — the whole of it — is that a reader sees
a coherent pair rather than a mix.

## Alternatives considered

**Keep the default field, make `QuotaFor` explicit** — `Config.Quota` as one
field plus `QuotaFor func(string) (bucket.Quota, bool)`. This kills the zero-field
rule and un-overloads `bucket.Quota` while keeping the one-line flat case. It
leaves two sources and therefore a precedence rule, just a simpler one, and
leaves `SetDefaultQuota` as a second runtime knob.

**Keep everything, document it better.** The rule was already documented in
eight places. Documenting a thing eight times is what having it in one place is
supposed to prevent.

**Delete `SetDefaultQuota` only.** Removes the second runtime knob and none of
the three static sources. Half a fix, and the half that leaves `Config.Rate`
required-but-ignorable.
