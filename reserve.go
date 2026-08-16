package pace

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
// Like [Client.Allow], Reserve may briefly do store I/O the first time a user
// is seen, bounded by [Config.StoreTimeout]. Neither method takes a context;
// that is a wart both share, kept because changing Allow's signature would cost
// more than the honesty is worth.
func (c *Client) Reserve() *Reservation {
	l := c.lim
	r := &Reservation{lim: l, userID: c.userID}
	if !l.enter() {
		return r // not OK; Cancel is a no-op
	}
	defer l.leave()

	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.StoreTimeout)
	defer cancel()

	u := l.userFor(ctx, c.userID)
	u.lastUsed.Store(now.UnixNano())

	r.res = u.bucket.ReserveAt(now)
	r.ok = r.res.OK()
	if !r.ok {
		return r
	}
	// Snapshotted rather than recomputed on each Delay call, so the value is
	// deterministic and means what its documentation says: the wait measured
	// from the moment Reserve returned.
	r.delay = r.res.DelayFrom(now)
	if r.delay > 0 {
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: c.userID,
			Delay:  r.delay,
			Tokens: u.bucket.TokensAt(now),
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
		})
	}
	return r
}

// OK reports whether a token was reserved. In practice it is false only when
// the Limiter is shutting down: a reservation is always for one token and
// [Config.Burst] is never below one, so the bucket's "can never be satisfied"
// refusal is unreachable from here. A Reservation that is not OK holds nothing,
// and Cancel on it is a no-op.
func (r *Reservation) OK() bool { return r.ok }

// Delay is how long to wait before acting, measured from when [Client.Reserve]
// returned. Zero means a token was already available.
func (r *Reservation) Delay() time.Duration { return r.delay }

// Cancel returns the token to the bucket. It is a no-op once the delay has
// elapsed — by then the token is spent — and on any call after the first.
func (r *Reservation) Cancel() {
	if !r.ok || r.done {
		return
	}
	r.done = true
	r.res.CancelAt(r.lim.cfg.Clock.Now())
}
