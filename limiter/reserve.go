package limiter

import (
	"context"
	"time"

	"github.com/jaeminst/pace/internal/bucket"
)

// Reservation is a rate-limit token held for a request the caller intends to
// make. Obtain one from [Client.Reserve].
//
// It is the ground between [Client.Allow], which refuses rather than waits, and
// [Client.Wait], which waits and cannot give the token back. A Reservation
// tells you how long the wait would be and lets you change your mind.
//
// A Reservation is not safe for concurrent use.
type Reservation struct {
	lim    *Limiter
	res    *bucket.Reservation
	userID string
	delay  time.Duration
	ok     bool
	done   bool
}

// Reserve holds one token for this user and reports when it may be used. It
// does not block on the bucket.
//
// The token is taken immediately, so a Reservation you decide not to use must
// be released with [Reservation.Cancel] — otherwise the user is charged for a
// request that never happened.
//
//	r := c.Reserve()
//	if !r.OK() || r.Delay() > tolerable {
//	    r.Cancel()
//	    return errTooBusy
//	}
//	time.Sleep(r.Delay())
//	// … now make the call
//
// A reservation counts toward [Limiter.Stats] and fires [Observer.Throttled]
// when the delay is non-zero, so it is accounted for exactly as a wait is.
//
// Like [Client.Allow], Reserve may do store I/O the first time a user is seen,
// bounded by [Config.StoreTimeout], and consults [SharedConfig.Quota] when one
// is configured. ctx bounds both.
func (c *Client) Reserve(ctx context.Context) *Reservation {
	l := c.lim
	r := &Reservation{lim: l, userID: c.userID}
	if !l.enter() {
		return r // not OK; Cancel is a no-op
	}
	defer l.leave()

	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	ctx, release := l.withLifetime(ctx)
	defer release()
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()

	u := l.reg.GetOrCreate(ctx, c.userID)
	u.Touch(now)

	q := quotaOf(u)

	r.res = u.Bucket().ReserveAt(now)
	r.ok = r.res.OK()
	if !r.ok {
		return r
	}
	// Snapshotted rather than recomputed on each Delay call, so the value is
	// deterministic and means what its documentation says: the wait measured
	// from the moment Reserve returned.
	r.delay = r.res.DelayFrom(now)

	// With a shared backend the local bucket is a shadow, and a shadow may only
	// refuse. A non-zero delay is a refusal — it proves the shared bucket has
	// nothing either — and is authoritative without a round-trip. Anything the
	// shadow admits still has to be paid for at the backend, or Reserve would be
	// the one entry point handing out tokens the fleet never agreed to.
	if l.sharedEnabled(q) && r.delay == 0 {
		grant, ok, err := l.takeShared(ctx, c.userID, q)
		if err != nil || !ok {
			// Cancel at the instant the token was taken: CancelAt refuses to
			// refund once the reservation's time to act has passed, so
			// cancelling at "now" would make the refund depend on clock
			// granularity. Not consuming the shadow for a request the backend
			// refused is the same rule allowShared documents.
			r.res.CancelAt(now)
			r.ok = false
			r.delay = grant.RetryAfter
			if r.delay <= 0 {
				r.delay = fallbackPollDelay(q)
			}
			l.reportThrottle(ctx, c.userID, u, r.delay, now)
			return r
		}
	}

	if r.delay > 0 {
		l.reportThrottle(ctx, c.userID, u, r.delay, now)
	}
	return r
}

// OK reports whether a token was reserved. It is false when the Limiter is
// shutting down, and when [SharedConfig.Quota] is configured and the backend
// refused. Without a shared quota it is false only during shutdown: a
// reservation is always for one token and [Config.Burst] is never below one, so
// the bucket's "can never be satisfied" refusal is unreachable from here. A
// Reservation that is not OK holds nothing, and Cancel on it is a no-op.
func (r *Reservation) OK() bool { return r.ok }

// Delay is how long to wait before acting, measured from when [Client.Reserve]
// returned. Zero means a token was already available.
//
// When OK is false because a shared backend refused, Delay is that backend's
// own estimate of when a retry could succeed, or pace's guess of one
// token-period if it did not say. It is zero for a Reservation refused because
// the Limiter is shutting down, where no wait would help.
func (r *Reservation) Delay() time.Duration { return r.delay }

// Cancel returns the token to the bucket. It is a no-op once the delay has
// elapsed — by then the token is spent — and on any call after the first.
//
// With [SharedConfig.Quota] configured it returns only the local token. The
// shared one is already spent: [SharedQuota] has no way to hand a token back,
// deliberately, since "exactly one Take per admitted request" is what makes the
// accounting comprehensible. The error is in the safe direction — the fleet
// stays charged for a request that did not happen, so the limit is under-served
// rather than over — but it does mean a workload that reserves and cancels
// often will drift below its share. If that matters, prefer [Client.Allow].
func (r *Reservation) Cancel() {
	if !r.ok || r.done {
		return
	}
	r.done = true
	r.res.CancelAt(r.lim.cfg.Clock.Now())
}
