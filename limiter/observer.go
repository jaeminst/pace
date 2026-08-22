package limiter

import (
	"context"
	"net/http"
	"time"

	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/response"

	"github.com/jaeminst/pace/bucket"
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
	q := Quota{Rate: Limit(b.Limit()), Burst: b.Burst()}
	tokens := b.TokensAt(t)
	if shared != nil {
		tokens = *shared
	}
	l.observeThrottled(ctx, observe.ThrottleInfo{
		UserID: userID,
		Delay:  delay,
		Tokens: tokens,
		Limit:  float64(q.Rate),
		Burst:  q.Burst,
	})
}

// observesEvictions reports whether building an EvictInfo is worth it. The
// sweep and the shutdown drop both walk every user, so the check is what keeps
// them from reading a token count nobody will look at.
func (l *Limiter) observesEvictions() bool {
	return l.cfg.Observer != nil && l.cfg.Observer.UserEvicted != nil
}

// onEvict translates one eviction into the public report. The registry counts
// them; this only tells anybody who asked to hear about it.
func (l *Limiter) onEvict(e registry.Eviction) {
	if l.cfg.Observer == nil || l.cfg.Observer.UserEvicted == nil {
		return
	}
	// The Limiter's own context: cancelled at Close, so a hook doing bounded
	// work can bail instead of holding up shutdown.
	l.cfg.Observer.UserEvicted(l.ctx, observe.EvictInfo{
		UserID:   e.UserID,
		Reason:   evictReasonOf(e.Reason),
		Tokens:   e.Tokens,
		LastUsed: e.LastUsed,
	})
}

func evictReasonOf(r registry.Reason) observe.EvictReason {
	switch r {
	case registry.Explicit:
		return observe.EvictExplicit
	case registry.Shutdown:
		return observe.EvictShutdown
	default: // registry.Idle
		return observe.EvictIdle
	}
}

// statusOf reports a response's status, or zero when there was none.
func statusOf(resp *response.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode()
}

// httpStatusOf is statusOf for the raw response Stream hands back.
func httpStatusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}
