// Package bucket provides a token-bucket rate limiter backed by golang.org/x/time/rate.
package bucket

import (
	"context"
	"math"
	"time"

	"golang.org/x/time/rate"
)

// maxDurationSeconds is the largest span time.Duration can represent.
const maxDurationSeconds = float64(math.MaxInt64) / float64(time.Second)

// Bucket wraps a [rate.Limiter] with a Wait method that honours both the
// caller's context and the manager's lifetime context.
type Bucket struct {
	limiter *rate.Limiter
}

// NewBucket creates a Bucket that refills at perSec tokens per second, up to
// the given burst ceiling.
func NewBucket(perSec float64, burst int) *Bucket {
	return &Bucket{limiter: rate.NewLimiter(rate.Limit(finite(perSec)), burst)}
}

// finite maps a rate rate.Limiter cannot work with onto one it can.
//
// Its own Inf is math.MaxFloat64, not a true infinity, and handing it a real
// one poisons the arithmetic: every token count downstream becomes NaN, and a
// bucket holding NaN tokens refuses every request forever. A NaN rate means
// nothing at all, so it becomes zero — no refill, which is the conservative
// reading.
//
// pace normalises this before it gets here. The check stays because this
// package is the one that owns the arithmetic, and because a silent NaN is a
// remarkably hard failure to diagnose from the outside.
func finite(perSec float64) float64 {
	switch {
	case math.IsNaN(perSec):
		return 0
	case math.IsInf(perSec, 1):
		return math.MaxFloat64
	case math.IsInf(perSec, -1):
		return 0
	default:
		return perSec
	}
}

// RestoreBucket creates a Bucket holding exactly savedTokens as of savedAt,
// plus whatever accrued between savedAt and now, capped at burst.
//
// Callers pass now explicitly rather than letting the bucket read the wall
// clock, so the restore path is deterministic under an injected Clock.
func RestoreBucket(perSec float64, burst int, savedTokens float64, savedAt, now time.Time) *Bucket {
	perSec = finite(perSec)
	l := rate.NewLimiter(rate.Limit(perSec), burst)

	// A store can hand back nonsense: a hand-edited row, a truncated write, a
	// NaN that round-tripped through a REAL column. Grant no credit rather
	// than propagate it — under-crediting a throttle is the safe direction.
	if math.IsNaN(savedTokens) || math.IsInf(savedTokens, 0) {
		savedTokens = 0
	}
	// A savedAt in the future means clock skew, not a debt to settle.
	elapsed := math.Max(0, now.Sub(savedAt).Seconds())
	tokens := math.Max(0, math.Min(float64(burst), savedTokens+elapsed*perSec))

	// A fresh limiter reads as full at any realistic instant, because its last
	// update is the zero Time. Draining the entire burst at the instant from
	// which refilling yields exactly `tokens` at `now` reproduces fractional
	// state, which ReserveN's integer argument cannot express directly.
	l.ReserveN(drainInstant(now, tokens, perSec), burst)
	return &Bucket{limiter: l}
}

// drainInstant returns the time t at which emptying a bucket leaves it holding
// exactly tokens at now.
func drainInstant(now time.Time, tokens, perSec float64) time.Time {
	if perSec <= 0 || tokens <= 0 {
		return now
	}
	secs := tokens / perSec
	if secs >= maxDurationSeconds {
		// Refilling this much would take centuries. Any sufficiently distant
		// past instant saturates the bucket, which is the only answer left.
		return now.Add(math.MinInt64)
	}
	return now.Add(-time.Duration(secs * float64(time.Second)))
}

// TokensAt returns the number of tokens available at t.
func (b *Bucket) TokensAt(t time.Time) float64 { return b.limiter.TokensAt(t) }

// Limit returns the refill rate, in tokens per second.
func (b *Bucket) Limit() float64 { return float64(b.limiter.Limit()) }

// Burst returns the token ceiling.
func (b *Bucket) Burst() int { return b.limiter.Burst() }

// SetQuotaAt changes the refill rate and ceiling as of t, keeping the tokens
// accrued up to that instant. Tokens above the new ceiling are dropped, since
// the ceiling is what the bucket may hold.
func (b *Bucket) SetQuotaAt(t time.Time, perSec float64, burst int) {
	b.limiter.SetLimitAt(t, rate.Limit(finite(perSec)))
	b.limiter.SetBurstAt(t, burst)
}

// AllowAt consumes one token if one is available at t, and reports whether it
// did. It never blocks.
func (b *Bucket) AllowAt(t time.Time) bool { return b.limiter.AllowN(t, 1) }

// Reservation is a token held for a caller who has not decided whether to use
// it. Obtain one from [Bucket.ReserveAt].
type Reservation struct{ res *rate.Reservation }

// ReserveAt takes one token as of t and reports when it may be used, without
// blocking. The token is spent immediately; a reservation the caller decides
// against must be handed back with [Reservation.CancelAt].
func (b *Bucket) ReserveAt(t time.Time) *Reservation {
	return &Reservation{res: b.limiter.ReserveN(t, 1)}
}

// OK reports whether a token was reserved. It is false when the request could
// never be satisfied — a burst too small to hold it.
func (r *Reservation) OK() bool { return r.res.OK() }

// DelayFrom is how long after t the reserved token may be used.
func (r *Reservation) DelayFrom(t time.Time) time.Duration { return r.res.DelayFrom(t) }

// CancelAt returns the token to the bucket. It has no effect once the delay has
// elapsed, since by then the token is spent.
func (r *Reservation) CancelAt(t time.Time) { r.res.CancelAt(t) }

// Wait blocks until one token is available or ctx is done.
//
// Merging the caller's context with the owning limiter's lifetime is the
// caller's job: doing it here as well would derive a second context on every
// request for no additional guarantee.
func (b *Bucket) Wait(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}

// DelayAt returns how long after t one token becomes available, or zero when
// one already is.
//
// The bucket refills deterministically, so this is exact rather than an
// estimate — which makes it the number worth reporting to a caller deciding
// whether to wait.
func (b *Bucket) DelayAt(t time.Time) time.Duration {
	tokens := b.limiter.TokensAt(t)
	if tokens >= 1 {
		return 0
	}
	perSec := float64(b.limiter.Limit())
	if perSec <= 0 {
		return 0
	}
	return time.Duration((1 - tokens) / perSec * float64(time.Second))
}
