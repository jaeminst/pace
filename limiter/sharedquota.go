package limiter

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jaeminst/pace/shared"

	"github.com/jaeminst/pace/rate"

	"github.com/jaeminst/pace/internal/registry"
)

// sharedEnabled reports whether requests must consult a shared backend.
//
// An infinite rate skips it: there is nothing to ration, and a round-trip per
// request to be told so would be pure cost.
func (l *Limiter) sharedEnabled(q rate.Quota) bool {
	return l.cfg.Shared.Quota != nil && q.Rate != rate.Inf
}

// takeShared asks the backend for one token, applying the breaker and the
// failure policy. It reports whether the request may proceed, and an error only
// when the policy is QuotaDeny.
//
// The caller must already have cleared the local shadow bucket; see
// [Limiter.allow].
func (l *Limiter) takeShared(ctx context.Context, userID string, q rate.Quota) (shared.Grant, bool, error) {
	now := l.cfg.Clock.Now()
	if !l.quotaBreaker.Allow(now) {
		// Counted as an error rather than passed over silently: from the
		// caller's side a short-circuited call and a failed one are the same
		// event, and the breaker being open is itself the evidence the backend
		// is down.
		l.stats.quotaErrors.Add(1)
		ok, err := l.onQuotaUnavailable(errBreakerOpen)
		return shared.Grant{}, ok, err
	}

	l.fireBeforeQuotaTake()
	l.stats.quotaTakes.Add(1)
	callCtx, cancel := context.WithTimeout(ctx, l.cfg.Shared.Timeout)
	defer cancel()

	grant, err := l.cfg.Shared.Quota.Take(callCtx, shared.TakeRequest{
		UserID:    userID,
		Namespace: l.cfg.Shared.Namespace,
		Tokens:    1,
		Quota:     q,
	})
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
		case l.ctx.Err() != nil:
			l.quotaBreaker.Abandoned()
			return shared.Grant{}, false, ErrClosed
		case ctx.Err() != nil:
			l.quotaBreaker.Abandoned()
			return shared.Grant{}, false, ctx.Err()
		}
		l.stats.quotaErrors.Add(1)
		l.quotaBreaker.Failed(l.cfg.Clock.Now())
		l.cfg.Logger.Warn("pace: shared quota", "user", userID, "err", err)
		ok, perr := l.onQuotaUnavailable(err)
		return shared.Grant{}, ok, perr
	}
	l.quotaBreaker.Succeeded()
	if !grant.OK {
		l.stats.quotaRefused.Add(1)
	}
	return grant, grant.OK, nil
}

// errBreakerOpen stands in for the cause when the breaker is short-circuiting.
var errBreakerOpen = errors.New("circuit breaker open after repeated failures")

// onQuotaUnavailable applies SharedConfig.OnError.
func (l *Limiter) onQuotaUnavailable(cause error) (bool, error) {
	switch l.cfg.Shared.OnError {
	case shared.Deny:
		return false, fmt.Errorf("%w: %w", shared.ErrUnavailable, cause)
	case shared.Allow:
		return true, nil
	default: // QuotaFallbackLocal
		// The shadow bucket already granted this request, and it enforces the
		// configured rate for this replica. Proceeding is the fallback.
		return true, nil
	}
}

// quotaPollDelay spreads a shared-quota poll over [d, 1.5d] so that waiters
// across every replica do not wake at the same instant.
//
// The argument for jittering is the one RetryPolicy already makes: the failures
// are correlated — every waiter is queued behind the same refill — so an
// unjittered schedule sends the whole fleet back together and recreates the
// contention it was meant to resolve.
//
// It jitters upward only, unlike the retry backoff. d is the backend's own
// statement of when a retry could succeed, so waking earlier than that spends a
// round-trip to be told the same thing.
func quotaPollDelay(d time.Duration) time.Duration {
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
	// quotaMinPollDelay stops the loop becoming a spin. Without a floor, a
	// high-rate user's token interval rounds to microseconds and the poller
	// hammers the backend.
	quotaMinPollDelay = 10 * time.Millisecond
	// quotaMaxPollDelay bounds how stale a guess may be. At one token per
	// hour the interval alone would park a caller for an hour on a bucket
	// another replica may have freed in a second.
	quotaMaxPollDelay = 30 * time.Second
)

// fallbackPollDelay returns how long to wait after a refusal the backend did
// not schedule.
//
// The obvious candidate — the local bucket's own DelayAt — is structurally
// useless here, and that is what made this a spin. Reaching this path at all
// means the shadow granted and the backend refused, and the refusal has just
// cancelled the shadow reservation to put the token back; so the shadow holds a
// token by construction and DelayAt returns zero every time.
//
// One token-period at the user's own rate is the honest guess instead: it is
// how long the shared bucket needs to earn the token this caller was refused.
func fallbackPollDelay(q rate.Quota) time.Duration {
	if q.Rate <= 0 {
		return quotaMinPollDelay
	}
	d := time.Duration(float64(time.Second) / float64(q.Rate))
	return min(max(d, quotaMinPollDelay), quotaMaxPollDelay)
}

// allowShared is Limiter.allow when a shared backend is configured.
//
// The local bucket is consulted first, as a shadow, and the reservation it
// produces is cancelled unless the backend also grants. Two properties make
// that sound:
//
//   - The shadow and the shared bucket are configured with the same rate and
//     burst, and this replica's consumption is a subset of the fleet's, so the
//     shadow always holds at least as many tokens as the shared bucket does. A
//     shadow that refuses therefore proves the backend would have refused too,
//     which is what makes skipping the round-trip safe.
//   - The converse does not hold, so a shadow that grants proves nothing and
//     the backend still has to be asked.
//
// The cancel on refusal is not tidiness. Consuming the shadow for a request the
// backend rejected breaks the inequality in the dangerous direction: a replica
// that keeps losing the race would ratchet its own shadow down to zero and stop
// asking, while the shared quota still had room for it.
func (l *Limiter) allowShared(ctx context.Context, userID string, u *registry.User, q rate.Quota, now time.Time) bool {
	res := u.Bucket().ReserveAt(now)
	if !res.OK() || res.DelayFrom(now) > 0 {
		res.CancelAt(now)
		l.reportThrottle(ctx, userID, u, u.Bucket().DelayAt(now), now)
		return false
	}

	grant, ok, err := l.takeShared(ctx, userID, q)
	if ok && err == nil {
		return true
	}
	// Cancelled at the instant the token was taken, not at "now". CancelAt
	// refuses to refund once the reservation's time to act has passed, and with
	// a real clock even a nanosecond of drift during the backend call would be
	// enough — which would make the refund depend on clock granularity rather
	// than on the decision.
	res.CancelAt(now)

	delay := grant.RetryAfter
	if delay <= 0 {
		// Same reasoning as acquireShared: the cancel above put the shadow
		// token back, so DelayAt would report zero and tell the caller a
		// refused request needs no wait.
		delay = fallbackPollDelay(q)
	}
	l.reportThrottleTokens(ctx, userID, u, delay, now, grant.Tokens)
	return false
}

// acquireShared is [Limiter.acquire] when a shared backend is configured.
//
// Each round reserves against the local shadow, waits out whatever delay that
// reservation implies, and only then spends a backend round-trip. The shadow
// argument from allowShared applies unchanged: waiting locally costs the
// backend nothing and cannot admit a request the backend would refuse.
//
// A refusal from the backend cancels the shadow reservation, for the reason
// allowShared gives: a replica that consumed locally for requests it was
// refused would ratchet itself below its own share while the shared quota still
// had room.
//
// [Observer.Throttled] fires at most once per request rather than once per
// round, so a long wait is not reported as a burst of throttles.
func (l *Limiter) acquireShared(ctx context.Context, userID string, u *registry.User, q rate.Quota) error {
	// Before the closure below, which that path allocates and never calls.
	if waiter, canWait := l.cfg.Shared.Quota.(shared.Waiter); canWait {
		return l.waitShared(ctx, waiter, userID, u, q)
	}

	reported := false
	report := func(delay time.Duration, tokens *float64) {
		if reported {
			return
		}
		reported = true
		now := l.cfg.Clock.Now()
		l.reportThrottleTokens(ctx, userID, u, delay, now, tokens)
	}

	for {
		now := l.cfg.Clock.Now()
		res := u.Bucket().ReserveAt(now)
		if !res.OK() {
			return l.throttled(userID, u, errUnsatisfiable)
		}
		if delay := res.DelayFrom(now); delay > 0 {
			report(delay, nil)
			l.fireBeforeWait()
			if err := l.sleep(ctx, delay); err != nil {
				res.CancelAt(now)
				return l.throttled(userID, u, err)
			}
		}

		grant, ok, err := l.takeShared(ctx, userID, q)
		if err != nil {
			res.CancelAt(now)
			return err
		}
		if ok {
			return nil
		}
		res.CancelAt(now)

		delay := grant.RetryAfter
		if delay <= 0 {
			delay = fallbackPollDelay(q)
		}
		report(delay, grant.Tokens)
		if err := l.sleep(ctx, quotaPollDelay(delay)); err != nil {
			return l.throttled(userID, u, err)
		}
	}
}

// waitShared uses a backend that can park a waiter, so pace does not poll.
//
// While the backend is answering, the local shadow is not consulted: the
// backend has taken responsibility for the wait, and gating it locally as well
// would delay a caller the backend was ready to serve.
//
// When the backend cannot answer, that stops being true, and the failure
// policy has to mean the same thing here as everywhere else. It used to return
// nil on every failure path, which made QuotaFallbackLocal — the default, and
// the conservative one — admit without limit: a backend that goes down opened
// the breaker after five failures and then served every user instantly for the
// whole cooldown. The fallback now does what its name says and waits on this
// replica's own bucket.
func (l *Limiter) waitShared(
	ctx context.Context, w shared.Waiter, userID string, u *registry.User, q rate.Quota,
) error {
	if !l.quotaBreaker.Allow(l.cfg.Clock.Now()) {
		l.stats.quotaErrors.Add(1)
		return l.sharedWaitFallback(ctx, userID, u, errBreakerOpen)
	}

	// No throttle is reported on this path, and that is a known gap rather than
	// an oversight. The backend owns the wait, so pace cannot know in advance
	// whether this caller will be parked or served instantly — and
	// Observer.Throttled is documented as firing *before* the wait, with the
	// expected delay. Firing it unconditionally, which is what this used to do,
	// made Stats.Throttled equal Stats.Requests on every WaitingSharedQuota
	// deployment; inventing a delay after the fact would be no better.
	// Under-counting a case pace genuinely cannot observe beats reporting a
	// number that is wrong.
	l.fireBeforeWait()
	l.fireBeforeQuotaTake()
	l.stats.quotaTakes.Add(1)

	err := w.Wait(ctx, shared.TakeRequest{
		UserID:    userID,
		Namespace: l.cfg.Shared.Namespace,
		Tokens:    1,
		Quota:     q,
	})
	switch {
	case err == nil:
		l.quotaBreaker.Succeeded()
		return nil
	case l.ctx.Err() != nil:
		return ErrClosed
	case ctx.Err() != nil:
		// The caller gave up; that is not the backend's failure.
		l.quotaBreaker.Abandoned()
		return ctx.Err()
	}

	l.stats.quotaErrors.Add(1)
	l.quotaBreaker.Failed(l.cfg.Clock.Now())
	l.cfg.Logger.Warn("pace: shared quota wait", "user", userID, "err", err)
	return l.sharedWaitFallback(ctx, userID, u, err)
}

// sharedWaitFallback applies [SharedConfig.OnError] on the waiting path, where
// there is no shadow reservation already in hand.
func (l *Limiter) sharedWaitFallback(ctx context.Context, userID string, u *registry.User, cause error) error {
	switch l.cfg.Shared.OnError {
	case shared.Deny:
		return fmt.Errorf("%w: %w", shared.ErrUnavailable, cause)
	case shared.Allow:
		return nil
	default: // QuotaFallbackLocal
		// Enforce the configured rate for this replica, which is what the
		// polling path gets for free by reserving against the shadow first.
		l.fireBeforeWait()
		if err := u.Bucket().Wait(ctx); err != nil {
			return l.throttled(userID, u, err)
		}
		return nil
	}
}

// errUnsatisfiable stands in when the bucket refuses a reservation outright,
// which needs a burst below one and so is unreachable through Config.
var errUnsatisfiable = errors.New("burst too small to ever satisfy the request")

// sleep waits for d, or until ctx is done.
//
// A non-positive d still consults ctx. It used to return nil outright, which
// made a polling loop that computed a zero delay uncancellable: nothing in the
// loop except the backend itself could then notice the caller had given up.
func (l *Limiter) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
