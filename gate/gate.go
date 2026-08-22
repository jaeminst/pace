// Package gate decides whether a request may proceed against a shared quota.
//
// It is the runtime for github.com/jaeminst/pace/shared, kept away from the
// contract: a backend author implements [shared.Quota] and should not have to
// compile a token bucket, a circuit breaker and a logger to do it.
//
// What it owns is the whole of the shared-quota decision: the local shadow
// bucket that may only refuse, the round-trip to the backend, the circuit
// breaker in front of that round-trip, the failure policy, and the poll
// schedule when a backend refuses without saying when to retry. It also owns
// the three counters that describe those calls, because nothing else writes
// them.
package gate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/breaker"
	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/shared"
)

// Config is everything the gate needs from its owner. Every field is required
// and [New] panics on one it cannot work with: this is a vtable for one caller
// rather than a set of options, so a zero field is a nil call on the first
// request rather than a default.
type Config struct {
	// Quota is the backend every replica consults. Required — a Gate is only
	// built when one is configured.
	Quota shared.Quota

	// Namespace, Timeout and OnError are shared.Config's other three fields,
	// passed through rather than held as a struct so that this package does not
	// have to care that they arrived together.
	Namespace string
	Timeout   time.Duration
	OnError   shared.ErrorPolicy

	Logger *slog.Logger

	// Now is the owner's clock, so every instant the breaker and the poll
	// schedule reason about comes from the source the rest of pace reports
	// against.
	Now func() time.Time

	// Closed is what to return when the owner's context is done — the owner's
	// own "I am shutting down" error, so a caller matching on it does not have
	// to know this package exists.
	Closed error

	// Throttled reports a wait about to happen. It is the one callback here
	// that does real work, and it cannot become a return value: [Gate.Acquire]
	// reports *before* a wait that may last seconds, so deferring the report to
	// the return would tell the caller after the fact.
	//
	// It takes the bucket rather than any richer notion of a user because that
	// is all the report needs: the tokens it holds, and the rate and burst it
	// is enforcing.
	Throttled func(ctx context.Context, userID string, b *bucket.Bucket, delay time.Duration, at time.Time, tokens *float64)

	// BeforeWait and BeforeQuotaTake are test seams. Pass method values that
	// read the hook at call time, not the hooks themselves: a test may install
	// one after the owner has started.
	BeforeWait      func()
	BeforeQuotaTake func()
}

// Gate is the shared-quota decision for one Limiter.
type Gate struct {
	cfg Config
	ctx context.Context
	// breaker short-circuits calls to a backend that is failing, so a dead one
	// costs one timeout every cooldown rather than one per request.
	breaker breaker.Breaker
	// The three counters describing backend calls. They live here rather than
	// with the owner's other tallies because this package is the only thing
	// that writes them; the owner reads them back through the accessors, the
	// way registry.Registry reports its own evictions.
	takes    atomic.Int64
	refused  atomic.Int64
	failures atomic.Int64
}

// New builds a gate bound to ctx, which must be the owner's lifetime context.
func New(ctx context.Context, cfg Config) *Gate {
	switch {
	case cfg.Quota == nil:
		panic("gate: Quota is required")
	case cfg.Logger == nil || cfg.Now == nil || cfg.Closed == nil:
		panic("gate: Logger, Now and Closed are required")
	case cfg.Throttled == nil || cfg.BeforeWait == nil || cfg.BeforeQuotaTake == nil:
		panic("gate: Throttled, BeforeWait and BeforeQuotaTake are required; pass a no-op if you have no hook")
	case cfg.Timeout <= 0:
		panic("gate: Timeout must be positive")
	}
	return &Gate{cfg: cfg, ctx: ctx}
}

// Takes reports how many times this gate has asked the backend for a token,
// whether granted, refused, or failed.
func (g *Gate) Takes() int64 { return g.takes.Load() }

// Refused reports how many of those the backend answered with a refusal. A
// healthy shared limiter refuses constantly; this is the shape of the load
// rather than an alarm.
func (g *Gate) Refused() int64 { return g.refused.Load() }

// Errors reports how many produced no answer at all — the backend failed, timed
// out, or the breaker was short-circuiting calls to one already known to be
// down. This is the number worth alerting on.
func (g *Gate) Errors() int64 { return g.failures.Load() }

// WaitError marks an error the owner should report as its own throttle error
// rather than pass through.
//
// The distinction matters: a failure to obtain a token within the caller's
// deadline is the owner's LimitError, carrying the user and the limit in force,
// while a refusal under [shared.Deny] is already the error the caller should
// see. Returning a marked wrapper rather than calling back into the owner to
// build the error is what keeps this package from needing to know that type.
type WaitError struct{ Cause error }

func (e *WaitError) Error() string { return e.Cause.Error() }
func (e *WaitError) Unwrap() error { return e.Cause }

// errUnsatisfiable stands in when the bucket refuses a reservation outright,
// which needs a burst below one and so is unreachable through a valid Config.
//
// It is unexported: an exported sentinel is a promise that a caller can match
// it, and nothing can reach this one to try. It arrives wrapped in a
// [WaitError] like every other cause on that path.
var errUnsatisfiable = errors.New("burst too small to ever satisfy the request")

// errBreakerOpen stands in for the cause when the breaker is short-circuiting.
var errBreakerOpen = errors.New("circuit breaker open after repeated failures")

// Take asks the backend for one token, applying the breaker and the failure
// policy. It reports whether the request may proceed, and an error only when
// the policy is [shared.Deny].
//
// The caller must already have cleared the local shadow bucket; see [Gate.Allow].
func (g *Gate) Take(ctx context.Context, userID string, rateLimit float64, burst int) (shared.Grant, bool, error) {
	now := g.cfg.Now()
	if !g.breaker.Allow(now) {
		// Counted as an error rather than passed over silently: from the
		// caller's side a short-circuited call and a failed one are the same
		// event, and the breaker being open is itself the evidence the backend
		// is down.
		g.failures.Add(1)
		ok, err := g.onUnavailable(errBreakerOpen)
		return shared.Grant{}, ok, err
	}

	g.cfg.BeforeQuotaTake()
	g.takes.Add(1)
	callCtx, cancel := context.WithTimeout(ctx, g.cfg.Timeout)
	defer cancel()

	grant, err := g.cfg.Quota.Take(callCtx, g.request(userID, rateLimit, burst))
	if err != nil {
		// Tell "the backend failed" apart from "our caller left" before doing
		// anything with either. A conformant backend honours the context — the
		// conformance suite requires it — so a caller whose deadline expires
		// mid-Take produces an error here that says nothing about the backend.
		// Charging it to the breaker lets a service with short per-request
		// deadlines open the breaker on its own cancellations, and running it
		// through the failure policy turns it into "proceed", which is how Wait
		// came to return nil on an expired context.
		switch {
		case g.ctx.Err() != nil:
			g.breaker.Abandoned()
			return shared.Grant{}, false, g.cfg.Closed
		case ctx.Err() != nil:
			g.breaker.Abandoned()
			return shared.Grant{}, false, ctx.Err()
		}
		g.failures.Add(1)
		g.breaker.Failed(g.cfg.Now())
		g.cfg.Logger.Warn("pace: shared quota", "user", userID, "err", err)
		ok, perr := g.onUnavailable(err)
		return shared.Grant{}, ok, perr
	}
	g.breaker.Succeeded()
	if !grant.OK {
		g.refused.Add(1)
	}
	return grant, grant.OK, nil
}

// request is the one question this package asks a backend. Take and Wait both
// ask it, and a difference between them would be a difference the backend sees
// but nothing here states.
func (g *Gate) request(userID string, rateLimit float64, burst int) shared.TakeRequest {
	return shared.TakeRequest{
		UserID:    userID,
		Namespace: g.cfg.Namespace,
		Tokens:    1,
		Rate:      rateLimit,
		Burst:     burst,
	}
}

// RetryDelay is how long to wait after a refusal: the backend's own schedule
// when it supplied one, and [FallbackDelay] when it did not.
//
// It is exported because the Limiter's Reserve path makes the same decision on
// a grant this package handed it, and a second copy of the rule would be a
// second place for it to drift.
func RetryDelay(g shared.Grant, rateLimit float64) time.Duration {
	if g.RetryAfter > 0 {
		return g.RetryAfter
	}
	return FallbackDelay(rateLimit)
}

// onUnavailable applies the failure policy.
func (g *Gate) onUnavailable(cause error) (bool, error) {
	switch g.cfg.OnError {
	case shared.Deny:
		return false, fmt.Errorf("%w: %w", shared.ErrUnavailable, cause)
	case shared.Allow:
		return true, nil
	default: // shared.FallbackLocal
		// The shadow bucket already granted this request, and it enforces the
		// configured rate for this replica. Proceeding is the fallback.
		return true, nil
	}
}

// pollDelay spreads a shared-quota poll over [d, 1.5d] so that waiters across
// every replica do not wake at the same instant.
//
// The argument for jittering is the one RetryPolicy already makes: the failures
// are correlated — every waiter is queued behind the same refill — so an
// unjittered schedule sends the whole fleet back together and recreates the
// contention it was meant to resolve.
//
// It jitters upward only, unlike the retry backoff. d is the backend's own
// statement of when a retry could succeed, so waking earlier than that spends a
// round-trip to be told the same thing.
func pollDelay(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	//nolint:gosec // jitter needs spread, not unpredictability
	return d + time.Duration(rand.Int64N(int64(d/2)+1))
}

// Bounds on the poll interval used when the backend refuses without saying when
// to retry. They apply only to that fallback: a RetryAfter the backend did
// supply is honoured as given, however long.
const (
	// minPollDelay stops the loop becoming a spin. Without a floor, a
	// high-rate user's token interval rounds to microseconds and the poller
	// hammers the backend.
	minPollDelay = 10 * time.Millisecond
	// maxPollDelay bounds how stale a guess may be. At one token per hour the
	// interval alone would park a caller for an hour on a bucket another
	// replica may have freed in a second.
	maxPollDelay = 30 * time.Second
)

// FallbackDelay returns how long to wait after a refusal the backend did not
// schedule.
//
// The obvious candidate — the local bucket's own DelayAt — is structurally
// useless here, and that is what made this a spin. Reaching this path at all
// means the shadow granted and the backend refused, and the refusal has just
// cancelled the shadow reservation to put the token back; so the shadow holds a
// token by construction and DelayAt returns zero every time.
//
// One token-period at the user's own rate is the honest guess instead: it is
// how long the shared bucket needs to earn the token this caller was refused.
func FallbackDelay(rateLimit float64) time.Duration {
	if rateLimit <= 0 {
		return minPollDelay
	}
	d := time.Duration(float64(time.Second) / rateLimit)
	return min(max(d, minPollDelay), maxPollDelay)
}
