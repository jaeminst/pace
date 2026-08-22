# ADR 0004 — The shared quota is approximate, and upstream is the authority

**Status:** accepted (v0.2.0)

## Context

Without `Config.Shared`, `Rate` is enforced once per process. Run the
service on ten replicas and the upstream sees ten times the configured rate.
The usual answer — set `Rate` to your share and move on — is exactly right when
load is even, and wrong when it is not: a replica handling a quarter of the
traffic sits idle on three quarters of its share while its busiest neighbour
throttles requests the fleet had budget for.

`Config.Shared` closes that gap by delegating the decision to a backend every
replica consults. This ADR records what that does and does not buy, because the
phrase "distributed rate limiting" promises considerably more than any
implementation of it delivers.

## Decision

Delegate, and keep the local bucket as a shadow that can only refuse.

The soundness argument is one inequality. The shadow and the shared bucket are
configured with the same rate and burst, and this replica's consumption is a
subset of the fleet's, so

    shadowTokens >= sharedTokens

always holds. A shadow with no tokens therefore proves the backend has none
either, and the request can be refused without a round-trip. The converse does
not follow, so a shadow that grants proves nothing and the backend still has to
be asked. That asymmetry is the whole design: it makes the common refusal free
and leaves every grant authoritative.

The corollary is that a refusal from the backend must **not** consume the
shadow. Consuming it would break the inequality in the dangerous direction — a
replica losing the race would ratchet its own shadow toward zero and stop
asking, while the shared quota still had room for it.

## What pace guarantees

- **Exactly one `Take` per admitted request.** No retries behind your back, no
  speculative pre-fetching. `Client.Reserve` counts as an admitted request: it
  takes its shared token at reserve time, not when the caller acts.
- **The shadow only refuses.** It never admits a request the backend has not
  admitted.
- **A refusal costs nothing.** Requests pace refuses locally never reach the
  backend; requests the backend refuses have their local reservation returned.
  The one thing a refusal *cannot* return is a shared token already taken —
  which is why `Reservation.Cancel` gives back only the local one. That error
  runs in the safe direction, leaving the fleet charged for a request that did
  not happen, and it is the price of there being no "return a token" method to
  get wrong.
- **Failure is a policy, not an accident.** `Config.OnQuotaError` decides, and
  the default is documented rather than emergent.
- **The shadow is never persisted.** With a shared quota configured, pace neither
  saves the local bucket to a `StateStore` nor restores it from one. The bucket
  describes what *this replica* spent, not what the user spent; restoring one
  replica's snapshot into another would have it throttling itself for traffic it
  never sent, and would break the inequality above.

## What nobody guarantees

- **The accuracy is your backend's property, not pace's.** pace asks a question
  and believes the answer. A backend that races, drifts, or rounds produces a
  limiter that races, drifts, or rounds. `shared/quotatest` is how you find
  out which you have.
- **A partition degrades to N × Rate by default.** `QuotaFallbackLocal` keeps
  serving against the local bucket, which is the same trade pace already makes
  for `StateStore`: refusing traffic because bookkeeping is unreachable is
  usually worse than briefly over-serving. Choose `QuotaDeny` when it is not.
- **There is no fairness.** Whichever replica asks first is served first. A
  replica that consistently loses the race is not compensated, and pace does not
  try to detect the situation.
- **`Grant.Tokens` is an upper bound, not a fact.** It is a snapshot of a value
  other replicas are changing, stale before it is read.
- **Upstream is the authority.** A 429 and its `Retry-After` are the truth.
  Client-side limiting is courtesy — it keeps you from being the reason for the
  429 — and no amount of shared accounting makes it enforcement. Handle the 429
  regardless of what this feature reports.

## Consequences

`Client.Allow` no longer strictly "never blocks". With a shared quota
configured, a request the shadow admits makes one backend call bounded by
`Config.QuotaTimeout` (500ms by default). That matters most where `Allow` is
used as an inbound load shedder, and it matters at the worst moment: when a
hostile user is hammering the service. The shadow pre-filter is the mitigation
— such a user exhausts their shadow almost immediately and is refused locally
from then on — but the first request through each refill still pays.

The latency cost on the throttled path is real and small. A granted request adds
one backend round-trip: roughly 0.2–0.5ms to a same-AZ Redis. What pace paces is
outbound HTTP, so the call that follows takes 10–500ms. That is 0.1% to 3%.

## What we are not doing

- **Shipping a backend.** A Redis implementation would be a second Go module to
  version, tag, and support, and its correctness would depend on a Lua script
  most users would never read. `shared/quotatest` ships the contract instead,
  executable against whatever you build.
- **Making the circuit breaker configurable.** Five consecutive failures open it
  for five seconds, after which a single probe decides whether it closes or the
  cooldown starts again. Its job is to stop a dead backend charging every request a
  full `QuotaTimeout`; nobody is going to tune that, and two more `Config` fields
  would have to keep working forever.
- **Putting a timestamp in `TakeRequest`.** Replica clocks disagree by
  milliseconds to seconds. Shared accounting keyed on client-supplied time is
  wrong by construction, so the backend must stamp its own.

## The honest recommendation

Most callers who want "distributed rate limiting" should set `Rate` to their
share of the limit and handle 429s properly. That costs nothing, fails in no new
ways, and is within a constant factor of correct whenever load is roughly even.

Reach for `Config.Shared` when load is genuinely uneven across replicas, when the
upstream limit is a contractual cap rather than a throttle, or when replica count
changes often enough that dividing by hand is its own source of bugs. It is a
real answer to a real problem — but the problem is narrower than the phrase
suggests, and the cost is an operational dependency on every outbound call path.

The library that published ADR 0003 does not get to be vague about this one.
