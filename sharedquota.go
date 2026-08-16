package pace

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// SharedQuota is a token supply shared by every process that consults it.
//
// Supply one via [Config.SharedQuota] to make rate limiting apply across
// replicas rather than once per process. pace never creates, configures, or
// closes a SharedQuota; it only asks.
//
// Read [ErrQuotaUnavailable] and [Config.OnQuotaError] before relying on this.
// A shared limiter is only as available as the backend behind it, and pace's
// default when that backend is unreachable is to keep serving traffic against
// each replica's local bucket — which is the same choice it makes for
// [StateStore], and which means a partition degrades to roughly N times the
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
// suite in [github.com/jaeminst/pace/pacetest]. Run it before you trust one.
type SharedQuota interface {
	Take(ctx context.Context, req TakeRequest) (Grant, error)
}

// WaitingSharedQuota is an optional extension to [SharedQuota], discovered by
// type assertion in the same way [BatchStateStore] extends [StateStore].
//
// Implement it when the backend can park a waiter and wake it — a blocking pop,
// a subscription, anything better than polling. Without it, pace polls on the
// schedule [Grant.RetryAfter] describes, which works but wakes up more often
// than the backend needs it to.
type WaitingSharedQuota interface {
	SharedQuota

	// Wait blocks until a token has been taken for req, or ctx is done. A nil
	// return means the token is taken, with the same finality as a Take that
	// returned OK.
	Wait(ctx context.Context, req TakeRequest) error
}

// TakeRequest is one request for shared tokens.
//
// It deliberately carries no timestamp: see [SharedQuota] on why the backend
// must supply its own.
type TakeRequest struct {
	// UserID identifies whose quota is being drawn on.
	UserID string

	// Namespace is [Config.QuotaNamespace] verbatim, so several Limiters can
	// share one backend without colliding.
	Namespace string

	// Tokens is how many to take. Always 1 today; it exists so that a weighted
	// request does not need a new method later.
	Tokens int

	// Quota is the rate and burst in force for this user, so a backend that
	// stores no configuration of its own can still enforce the right limit.
	Quota Quota
}

// Grant is a backend's answer to a [TakeRequest].
type Grant struct {
	// OK reports whether the tokens were taken. False must mean nothing was
	// consumed.
	OK bool

	// RetryAfter is how long until a retry could succeed. Zero means the
	// backend is not saying, and pace falls back to its local estimate.
	RetryAfter time.Duration

	// Tokens is how many remain, for reporting only. Negative means the
	// backend does not track it. It is a snapshot of a shared value that other
	// replicas are changing, so treat it as an upper bound rather than a fact.
	Tokens float64
}

// QuotaErrorPolicy decides what happens to a request when the shared backend
// cannot be reached. See [Config.OnQuotaError].
type QuotaErrorPolicy int

const (
	// QuotaFallbackLocal falls back to this replica's local bucket, which
	// enforces the configured rate per replica rather than in total. This is
	// the default, and it is the same trade pace already makes when a
	// StateStore is unavailable: refusing traffic because bookkeeping is down
	// is usually worse than briefly over-serving.
	QuotaFallbackLocal QuotaErrorPolicy = iota

	// QuotaDeny refuses the request with [ErrQuotaUnavailable]. Choose it when
	// exceeding the shared limit is worse than dropping traffic — a hard
	// contractual cap, or an upstream that bans rather than throttles.
	QuotaDeny

	// QuotaAllow lets the request through without consulting anything. Choose
	// it only when the limit is advisory and availability is the point.
	QuotaAllow
)

func (p QuotaErrorPolicy) String() string {
	switch p {
	case QuotaFallbackLocal:
		return "fallback-local"
	case QuotaDeny:
		return "deny"
	case QuotaAllow:
		return "allow"
	default:
		return "unknown"
	}
}

// ErrQuotaUnavailable reports that the shared backend could not be reached and
// [Config.OnQuotaError] is [QuotaDeny]. The cause is wrapped.
var ErrQuotaUnavailable = errors.New("pace: shared quota unavailable")

// Circuit-breaker constants. These are not configurable on purpose: their job
// is to stop a dead backend charging every request QuotaTimeout, and nobody is
// going to tune that. A configurable version would be two more Config fields
// that must keep working forever.
const (
	// quotaBreakerThreshold is how many consecutive failures open the breaker.
	quotaBreakerThreshold = 5
	// quotaBreakerCooldown is how long it stays open before one request is let
	// through to test the backend.
	quotaBreakerCooldown = 5 * time.Second
)

// quotaBreaker short-circuits calls to a backend that is failing, so a dead
// SharedQuota costs one timeout every cooldown rather than one per request.
type quotaBreaker struct {
	mu       sync.Mutex
	failures int
	openTill time.Time
}

// allow reports whether a call should be attempted at now.
func (b *quotaBreaker) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !now.Before(b.openTill)
}

func (b *quotaBreaker) succeeded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.openTill = time.Time{}
}

func (b *quotaBreaker) failed(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures++
	if b.failures >= quotaBreakerThreshold {
		b.openTill = now.Add(quotaBreakerCooldown)
		b.failures = 0
	}
}

// sharedEnabled reports whether requests must consult a shared backend.
//
// An infinite rate skips it: there is nothing to ration, and a round-trip per
// request to be told so would be pure cost.
func (l *Limiter) sharedEnabled(q Quota) bool {
	return l.cfg.SharedQuota != nil && q.Rate != Inf
}

// takeShared asks the backend for one token, applying the breaker and the
// failure policy. It reports whether the request may proceed, and an error only
// when the policy is QuotaDeny.
//
// The caller must already have cleared the local shadow bucket; see
// [Limiter.allow].
func (l *Limiter) takeShared(ctx context.Context, userID string, q Quota) (Grant, bool, error) {
	now := l.cfg.Clock.Now()
	if !l.quotaBreaker.allow(now) {
		ok, err := l.onQuotaUnavailable(errBreakerOpen)
		return Grant{}, ok, err
	}

	l.fireBeforeQuotaTake()
	callCtx, cancel := context.WithTimeout(ctx, l.cfg.QuotaTimeout)
	defer cancel()

	grant, err := l.cfg.SharedQuota.Take(callCtx, TakeRequest{
		UserID:    userID,
		Namespace: l.cfg.QuotaNamespace,
		Tokens:    1,
		Quota:     q,
	})
	if err != nil {
		l.quotaBreaker.failed(l.cfg.Clock.Now())
		l.cfg.Logger.Warn("pace: shared quota", "user", userID, "err", err)
		ok, perr := l.onQuotaUnavailable(err)
		return Grant{}, ok, perr
	}
	l.quotaBreaker.succeeded()
	return grant, grant.OK, nil
}

// errBreakerOpen stands in for the cause when the breaker is short-circuiting.
var errBreakerOpen = errors.New("circuit breaker open after repeated failures")

// onQuotaUnavailable applies Config.OnQuotaError.
func (l *Limiter) onQuotaUnavailable(cause error) (bool, error) {
	switch l.cfg.OnQuotaError {
	case QuotaDeny:
		return false, fmt.Errorf("%w: %w", ErrQuotaUnavailable, cause)
	case QuotaAllow:
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
func (l *Limiter) allowShared(ctx context.Context, userID string, u *user, q Quota, now time.Time) bool {
	res := u.bucket.ReserveAt(now)
	if !res.OK() || res.DelayFrom(now) > 0 {
		res.CancelAt(now)
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: userID,
			Delay:  u.bucket.DelayAt(now),
			Tokens: u.bucket.TokensAt(now),
			Limit:  q.Rate,
			Burst:  q.Burst,
		})
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
		delay = u.bucket.DelayAt(now)
	}
	l.observeThrottled(ctx, ThrottleInfo{
		UserID: userID,
		Delay:  delay,
		Tokens: u.bucket.TokensAt(now),
		Limit:  q.Rate,
		Burst:  q.Burst,
	})
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
func (l *Limiter) acquireShared(ctx context.Context, userID string, u *user, q Quota) error {
	reported := false
	report := func(delay time.Duration) {
		if reported {
			return
		}
		reported = true
		now := l.cfg.Clock.Now()
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: userID,
			Delay:  delay,
			Tokens: u.bucket.TokensAt(now),
			Limit:  q.Rate,
			Burst:  q.Burst,
		})
	}

	if waiter, canWait := l.cfg.SharedQuota.(WaitingSharedQuota); canWait {
		return l.waitShared(ctx, waiter, userID, q, report)
	}

	for {
		now := l.cfg.Clock.Now()
		res := u.bucket.ReserveAt(now)
		if !res.OK() {
			return l.limitError(userID, u, errUnsatisfiable)
		}
		if delay := res.DelayFrom(now); delay > 0 {
			report(delay)
			l.fireBeforeWait()
			if err := l.sleep(ctx, delay); err != nil {
				res.CancelAt(now)
				return l.waitFailure(userID, u, err)
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
			delay = u.bucket.DelayAt(l.cfg.Clock.Now())
		}
		report(delay)
		if err := l.sleep(ctx, quotaPollDelay(delay)); err != nil {
			return l.waitFailure(userID, u, err)
		}
	}
}

// waitShared uses a backend that can park a waiter, so pace does not poll.
//
// The local shadow is not consulted here: the backend has taken responsibility
// for the wait, and gating it locally as well would delay a caller the backend
// was ready to serve.
func (l *Limiter) waitShared(
	ctx context.Context, w WaitingSharedQuota, userID string, q Quota,
	report func(time.Duration),
) error {
	if !l.quotaBreaker.allow(l.cfg.Clock.Now()) {
		if ok, err := l.onQuotaUnavailable(errBreakerOpen); err != nil {
			return err
		} else if !ok {
			return fmt.Errorf("%w: %w", ErrQuotaUnavailable, errBreakerOpen)
		}
		return nil
	}

	// The backend decides how long this takes, so the delay is not knowable in
	// advance; report zero rather than invent a number.
	report(0)
	l.fireBeforeWait()
	l.fireBeforeQuotaTake()

	err := w.Wait(ctx, TakeRequest{
		UserID:    userID,
		Namespace: l.cfg.QuotaNamespace,
		Tokens:    1,
		Quota:     q,
	})
	switch {
	case err == nil:
		l.quotaBreaker.succeeded()
		return nil
	case l.ctx.Err() != nil:
		return ErrClosed
	case ctx.Err() != nil:
		// The caller gave up; that is not the backend's failure.
		return ctx.Err()
	}

	l.quotaBreaker.failed(l.cfg.Clock.Now())
	l.cfg.Logger.Warn("pace: shared quota wait", "user", userID, "err", err)
	ok, perr := l.onQuotaUnavailable(err)
	if perr != nil {
		return perr
	}
	if !ok {
		return fmt.Errorf("%w: %w", ErrQuotaUnavailable, err)
	}
	return nil
}

// errUnsatisfiable stands in when the bucket refuses a reservation outright,
// which needs a burst below one and so is unreachable through Config.
var errUnsatisfiable = errors.New("burst too small to ever satisfy the request")

// waitFailure turns a failed wait into the error acquire reports, matching the
// non-shared path.
func (l *Limiter) waitFailure(userID string, u *user, err error) error {
	if l.ctx.Err() != nil {
		return ErrClosed
	}
	return l.limitError(userID, u, err)
}

func (l *Limiter) limitError(userID string, u *user, err error) error {
	return &LimitError{
		UserID: userID,
		Limit:  Limit(u.bucket.Limit()),
		Burst:  u.bucket.Burst(),
		Delay:  u.bucket.DelayAt(l.cfg.Clock.Now()),
		Err:    err,
	}
}

// sleep waits for d, or until ctx is done.
func (l *Limiter) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
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

// persistsState reports whether per-user token state should be written to and
// read from [Config.Store].
//
// A shared quota turns the local bucket into a shadow, and a shadow must never
// be persisted. The bucket no longer describes what this user has spent — it
// describes what this replica has spent, which is a fraction of it. Restoring
// replica A's snapshot into replica B would have B throttling itself for
// traffic it never sent, and the inequality that makes the shadow safe
// (shadowTokens >= sharedTokens) is exactly what that breaks.
//
// The authoritative count lives in the backend, which is the point of
// configuring one.
func (l *Limiter) persistsState() bool {
	return l.store != nil && l.cfg.SharedQuota == nil
}
