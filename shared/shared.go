package shared

import (
	"context"
	"errors"
	"time"

	"github.com/jaeminst/pace/rate"
)

// Quota is a token supply shared by every process that consults it.
//
// Supply one via [Config.Quota] to make rate limiting apply across
// replicas rather than once per process. pace never creates, configures, or
// closes a Quota; it only asks.
//
// Read [ErrUnavailable] and [Config.OnError] before relying on this.
// A shared limiter is only as available as the backend behind it, and pace's
// default when that backend is unreachable is to keep serving traffic against
// each replica's local bucket — which is the same choice it makes for
// store.Store, and which means a partition degrades to roughly N times the
// intended rate rather than to an outage.
//
// # Implementing one
//
// Take must be atomic: two concurrent calls for the same user must not both
// succeed against the same token. Whatever backend you use has to do the
// arithmetic itself — a read-then-write from the client loses races.
//
// Timestamps must come from the backend, not the caller. That is why
// [TakeRequest] carries none: replica clocks disagree by milliseconds to
// seconds, and shared accounting keyed on client-supplied time is wrong by
// construction.
//
// A Take that returns OK false must consume nothing.
//
// All of this is asserted against a real implementation by the conformance
// suite in github.com/jaeminst/pace/shared/quotatest. Run it before you trust one.
type Quota interface {
	Take(ctx context.Context, req TakeRequest) (Grant, error)
}

// Waiter is an optional extension to [Quota], discovered by
// type assertion in the same way store.BatchStore extends store.Store.
//
// Implement it when the backend can park a waiter and wake it — a blocking pop,
// a subscription, anything better than polling. Without it, pace polls on the
// schedule [Grant.RetryAfter] describes, which works but wakes up more often
// than the backend needs it to.
type Waiter interface {
	Quota

	// Wait blocks until a token has been taken for req, or ctx is done. A nil
	// return means the token is taken, with the same finality as a Take that
	// returned OK.
	Wait(ctx context.Context, req TakeRequest) error
}

// TakeRequest is one request for shared tokens.
//
// It deliberately carries no timestamp: see [Quota] on why the backend
// must supply its own.
type TakeRequest struct {
	// UserID identifies whose quota is being drawn on.
	UserID string

	// Namespace is [Config.Namespace] verbatim, so several Limiters can
	// share one backend without colliding.
	Namespace string

	// Tokens is how many to take. Always 1 today; it exists so that a weighted
	// request does not need a new method later.
	Tokens int

	// Quota is the rate and burst in force for this user, so a backend that
	// stores no configuration of its own can still enforce the right limit.
	Quota rate.Quota
}

// Grant is a backend's answer to a [TakeRequest].
type Grant struct {
	// OK reports whether the tokens were taken. False must mean nothing was
	// consumed.
	OK bool

	// RetryAfter is how long until a retry could succeed. Zero means the
	// backend is not saying, and pace falls back to its local estimate.
	RetryAfter time.Duration

	// Tokens is how many remain. pace reports it as observe.ThrottleInfo.Tokens on a
	// refusal, in preference to the local shadow bucket's count: the shadow
	// holds this replica's fraction of the quota, so on this path it is the
	// backend that knows the number an operator is asking for.
	//
	// Nil means the backend does not track it — a pointer rather than a negative
	// sentinel, because pace's own buckets go negative while a reservation is
	// outstanding, so a backend modelled the same way reports a real negative
	// that a sentinel would swallow. v0.2.0 removed exactly this pattern from
	// Client.Tokens.
	//
	// It is a snapshot of a shared value that other replicas are changing, so
	// treat it as an upper bound rather than a fact.
	Tokens *float64
}

// Config configures cross-replica rate limiting. Every field is ignored
// unless Quota is set, since that is what turns it on.
//
// It is nested rather than flattened into the Limiter's own Config: four
// top-level fields configuring one optional subsystem crowd the two everybody
// actually sets, and grouping them is impossible once v1 freezes the API. It
// also stops
// limiter.Config.QuotaFor — per-user tiering, which works with no backend at all —
// reading as if Timeout and OnError governed it.
type Config struct {
	// Quota is the backend every replica consults. Nil limits per process.
	//
	// The local bucket stays, as a shadow that can only refuse. It never grants
	// a request the backend has not also granted, so it costs nothing in
	// correctness and saves a round-trip for every request this replica can
	// already tell is over its own share.
	//
	// Read [Quota] and OnError before adopting this. Most callers who
	// want "distributed rate limiting" are better served by setting
	// limiter.Config.Rate to their share of the limit and handling 429s honestly;
	// this trades an operational dependency on every outbound call path for
	// accuracy that only matters when replicas are unevenly loaded.
	Quota Quota

	// Namespace is passed through in [TakeRequest.Namespace], so several
	// Limiters can share one backend without colliding.
	Namespace string

	// Timeout bounds each [Quota] call. Zero defaults to 500ms.
	//
	// It is much shorter than [Config.StoreTimeout] because it sits in front of
	// every request rather than in front of a user's first one.
	Timeout time.Duration

	// OnError decides what happens when the backend cannot be reached. Zero is
	// [FallbackLocal].
	OnError ErrorPolicy
}

// ErrorPolicy decides what happens to a request when the shared backend
// cannot be reached. See [Config.OnError].
type ErrorPolicy int

const (
	// FallbackLocal falls back to this replica's local bucket, which
	// enforces the configured rate per replica rather than in total. This is
	// the default, and it is the same trade pace already makes when a
	// StateStore is unavailable: refusing traffic because bookkeeping is down
	// is usually worse than briefly over-serving.
	FallbackLocal ErrorPolicy = iota

	// Deny refuses the request with [ErrUnavailable]. Choose it when
	// exceeding the shared limit is worse than dropping traffic — a hard
	// contractual cap, or an upstream that bans rather than throttles.
	Deny

	// Allow lets the request through without consulting anything. Choose
	// it only when the limit is advisory and availability is the point.
	Allow
)

func (p ErrorPolicy) String() string {
	switch p {
	case FallbackLocal:
		return "fallback-local"
	case Deny:
		return "deny"
	case Allow:
		return "allow"
	default:
		return "unknown"
	}
}

// ErrUnavailable reports that the shared backend could not be reached and
// [Config.OnError] is [Deny]. The cause is wrapped.
var ErrUnavailable = errors.New("pace: shared quota unavailable")
