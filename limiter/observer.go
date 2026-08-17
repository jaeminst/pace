package limiter

import (
	"context"
	"time"

	"github.com/jaeminst/pace/observe"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/registry"
)

// observeThrottled fires the throttle hooks, if any are configured.
func (l *Limiter) observeThrottled(ctx context.Context, info observe.ThrottleInfo) {
	l.stats.throttled.Add(1)
	l.stats.waitNanos.Add(info.Delay.Nanoseconds())
	if l.cfg.Observer != nil && l.cfg.Observer.Throttled != nil {
		l.cfg.Observer.Throttled(ctx, info)
	}
}

// observesRequests reports whether anything is listening for finished requests.
//
// Call sites check it before assembling a RequestInfo. The struct carries
// strings, a duration and an error, and building one per request costs real
// allocation on a path that most callers run with no observer at all.
func (l *Limiter) observesRequests() bool {
	return l.cfg.Observer != nil && l.cfg.Observer.RequestFinished != nil
}

// countRequest records the outcome of a dispatched round-trip.
func (l *Limiter) countRequest(err error) {
	if err != nil {
		l.stats.errors.Add(1)
	}
}

// reportThrottle tells the observer a request had to wait, filling in
// everything derivable from the user's own bucket as of t.
//
// delay is the caller's, because only the caller knows it: sometimes it is what
// the local bucket says, sometimes it is a shared backend's RetryAfter, and
// sometimes it is a reservation's snapshotted wait. Everything else comes from
// one place so the five fields cannot drift apart across the seven sites that
// report a throttle.
func (l *Limiter) reportThrottle(ctx context.Context, userID string, u *registry.User, delay time.Duration, t time.Time) {
	l.reportBucketTokens(ctx, userID, u.Bucket(), delay, t, nil)
}

// reportBucketTokens is reportThrottle for the shared-quota path, where the
// backend may have reported the count itself.
//
// On that path the local bucket is a shadow, and [ADR 0004] states it is never
// authoritative: it may refuse, but what it holds is this replica's fraction of
// the quota rather than the quota. Reporting it answers a question the operator
// did not ask. So when the backend supplies a number — [Grant.Tokens] — that is
// the one describing the limit actually in force, and it wins.
//
// A backend that does not track tokens passes nil, and the shadow is reported
// as before. That is not authoritative either, but it is the best available and
// an upper bound on the truth, which is the same guarantee the shadow gives
// everywhere else.
//
// [ADR 0004]: https://github.com/jaeminst/pace/blob/main/docs/adr/0004-shared-quota-is-approximate.md
func (l *Limiter) reportBucketTokens(
	ctx context.Context, userID string, b *bucket.Bucket, delay time.Duration, t time.Time, shared *float64,
) {
	q := rate.Quota{Rate: rate.Limit(b.Limit()), Burst: b.Burst()}
	tokens := b.TokensAt(t)
	if shared != nil {
		tokens = *shared
	}
	l.observeThrottled(ctx, observe.ThrottleInfo{
		UserID: userID,
		Delay:  delay,
		Tokens: tokens,
		Limit:  q.Rate,
		Burst:  q.Burst,
	})
}

// observesEvictions reports whether building an EvictInfo is worth it. The
// sweep and the shutdown drop both walk every user, so the check is what keeps
// them from reading a token count nobody will look at.
func (l *Limiter) observesEvictions() bool {
	return l.cfg.Observer != nil && l.cfg.Observer.UserEvicted != nil
}

func (l *Limiter) observeJob(info observe.JobInfo) {
	if l.cfg.Observer != nil && l.cfg.Observer.JobTransition != nil {
		l.cfg.Observer.JobTransition(l.ctx, info)
	}
}
