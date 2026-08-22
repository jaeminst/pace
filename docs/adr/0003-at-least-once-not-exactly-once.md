# ADR 0003 — The durable queue is at-least-once, not exactly-once

**Status:** superseded by [ADR 0005](0005-pace-ships-contracts-not-backends.md) (v0.2.0)

The durable queue was removed in v0.2.0. This is kept for the correction it
records — v0.1.0 shipped a false "exactly-once" claim — and because the
reasoning applies to any queue built on top of pace rather than inside it.

## Context

v0.1.0's README headlined the durable queue with **"exactly-once semantics"** and
a guarantees table whose first row read:

> **Exactly-once (success)** — A job that received an HTTP response is never
> retried; the cached response is returned on every subsequent call.

That was false. The sequence was: enqueue, send, record the result. A crash
between the response arriving and the result being committed left the row in
`pending`, and the next start replayed it. So did a `Complete` that failed —
that path logged a warning and moved on. For a payment endpoint, the outcome is
a duplicate charge, arriving from the one library whose headline says it cannot
happen.

There is a test for it now. Against the old implementation it reports:

```
the stranded POST was re-sent 1 times, want 0
```

## Decision

**Delivery is at-least-once. The documentation says so, in those words.**

Exactly-once delivery over HTTP is not achievable by a client alone. Once bytes
leave the process, a crash before the response is recorded leaves no way to
learn whether the server acted. No amount of local bookkeeping closes that; the
information is on the other side of the wire.

What is achievable, and what pace does:

1. **Commit the intent to send before dispatching.** A job found in `sending`
   after a restart is one whose outcome is genuinely unknown. The window cannot
   be removed, but it can be made *detectable* instead of silent.
2. **Let the caller decide what happens in that window.**
   `Config.AmbiguousPolicy` chooses between retrying and parking, defaulting to
   retry only when repeating is safe.
3. **Send an idempotency key.** Every durable request carries
   `Idempotency-Key: <job id>`. Against a server that honours it, delivery is
   effectively exactly-once — and the claim is stated with that condition
   attached, because it depends on the server, not on pace.
4. **Make the send exclusive.** Claiming a job is one conditional `UPDATE`.
   `INSERT OR IGNORE` deduplicates the *row*, not the *send*, which is what
   allowed a replay worker and a live caller to both dispatch the same request.

## Consequences

- The strongest claim pace may make is: *at-least-once delivery; effectively
  exactly-once against an endpoint that honours the idempotency key.* Anything
  stronger is a promise the network does not permit.
- A non-idempotent request whose outcome is unknown is dropped by default rather
  than repeated. Losing a charge is recoverable by asking the user; charging
  twice is not, and pace does not get to choose which risk a caller takes.
- Retrying is bounded. A job that never succeeds ends in the dead-letter table
  where a human can see it, not in a loop.

## Why this document exists

The false claim was not careless — it is the claim everyone wants to be able to
make, and the implementation looked like it supported it. This ADR exists so the
next person to write a guarantees table has to argue with it first.
