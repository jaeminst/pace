// Package bucket is a token bucket, and the vocabulary for describing one.
//
// [Quota] is a rate and a ceiling — the two numbers that define a bucket — and
// [Limit] is the rate, written in whatever unit reads best at the call site:
//
//	bucket.NewQuota("60/m", 10)                            // from a string
//	bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}    // the same value
//
// A caller meets these in
// [github.com/jaeminst/pace/config.Config], which is where a rate is written,
// and gets one back from [Bucket.Quota], which is what a rate limiter is
// enforcing. They are the same type because they describe the same thing, and
// keeping them so is why this package holds the vocabulary rather than config:
// a Quota whose Rate were a config type could not live beside the bucket it
// configures without an import cycle, and splitting the two would mean two
// spellings of one pair.
//
// The bucket itself is backed by golang.org/x/time/rate and mostly delegates.
// What is original is [RestoreBucket], which rebuilds a bucket from a persisted
// token count and the instant it was saved, and the drain that makes the
// arithmetic exact — both fuzz-hardened, because a restore that is off by a
// fraction is a quota that is wrong forever.
//
// Nothing here imports anything else of pace's, which is what keeps it usable
// from every layer. The contract packages a third party implements against —
// store, shared, observe — still carry plain float64 and int and import nothing,
// per [ADR 0007]; this package is pace's own and the rule does not reach it.
//
// [ADR 0007]: https://github.com/jaeminst/pace/blob/main/docs/adr/0007-contracts-carry-numbers-not-types.md
package bucket

import (
	"context"
	"math"
	"sync/atomic"
	"time"

	"golang.org/x/time/rate"
)

// maxDurationSeconds is the largest span time.Duration can represent.
const maxDurationSeconds = float64(math.MaxInt64) / float64(time.Second)

// Bucket wraps a [rate.Limiter] with a Wait method that honours both the
// caller's context and the manager's lifetime context.
//
// It must not be copied: quota holds a lock-free pointer whose whole purpose is
// that every reader shares one. go vet's copylocks enforces that.
type Bucket struct {
	limiter *rate.Limiter
	// quota is what the limiter above is enforcing, as one immutable value.
	//
	// rate.Limiter reports the rate and the ceiling through two separately
	// locked methods, so reading both gives a pair that may never have been
	// configured — and that pair is what LimitError, ThrottleInfo, a Client's
	// Quota and shared.TakeRequest all report. One pointer load cannot tear,
	// and is cheaper than the two mutex acquisitions it replaces.
	quota atomic.Pointer[Quota]
}

// NewBucket creates a Bucket enforcing q.
func NewBucket(q Quota) *Bucket {
	q.Rate = Limit(usableRate(float64(q.Rate)))
	b := &Bucket{limiter: rate.NewLimiter(rate.Limit(q.Rate), q.Burst)}
	b.quota.Store(&q)
	return b
}

// usableRate maps a rate rate.Limiter cannot work with onto one it can.
//
// It is not [Finite], though they overlap. Finite is the caller-facing
// normalisation — a true infinity becomes [Inf], which is what the arithmetic
// downstream can hold. This one is the floor underneath it, and it also answers
// for NaN and negative infinity, because by the time a value reaches here there
// is nobody left to reject it.
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
func usableRate(perSec float64) float64 {
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
func RestoreBucket(q Quota, savedTokens float64, savedAt, now time.Time) *Bucket {
	q.Rate = Limit(usableRate(float64(q.Rate)))
	perSec, burst := float64(q.Rate), q.Burst
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
	b := &Bucket{limiter: l}
	b.quota.Store(&q)
	return b
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

// Quota returns the rate and the ceiling this bucket is enforcing, as they were
// set together.
//
// **This is the source of truth for a key's limit, not the configuration.** A
// a config.WithQuotaFor option may have given it its own, and a reload may have
// changed it since; every report pace makes — LimitError, ThrottleInfo, a
// Client's Quota, and the TakeRequest handed to a shared backend — comes from
// here.
//
// One load, so the pair is always one somebody configured. Asking rate.Limiter
// for the two separately can return a combination that never existed, because a
// change to each takes a different lock.
func (b *Bucket) Quota() Quota { return *b.quota.Load() }

// SetQuotaAt changes the refill rate and ceiling as of t, keeping the tokens
// accrued up to that instant. Tokens above the new ceiling are dropped, since
// the ceiling is what the bucket may hold.
//
// The pair is published last, after both changes have landed, so a reader
// between the two rate.Limiter calls sees the old quota rather than a mix. That
// is the whole of what the ordering buys. It does not make the report agree with
// what is being enforced: inside that window the limiter is already on the new
// pair while Quota still answers with the old one, so a lowered quota reads high
// for an instant. Publishing first would trade that for a raised quota reading
// high instead. Neither order removes the step, and a coherent pair is worth
// more than which side of the change it falls on.
func (b *Bucket) SetQuotaAt(t time.Time, q Quota) {
	q.Rate = Limit(usableRate(float64(q.Rate)))
	b.limiter.SetLimitAt(t, rate.Limit(q.Rate))
	b.limiter.SetBurstAt(t, q.Burst)
	b.quota.Store(&q)
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

// CancelAt returns the token to the bucket, if it is not too late.
//
// It has no effect once the reservation's delay has elapsed, since by then the
// token is spent. A reservation that needed no wait is already at its deadline,
// so whether cancelling it refunds anything depends on whether t has advanced
// past the instant it was taken — see [Reservation.DelayFrom] for whether there
// was a wait to be inside of. It also does nothing while the bucket's rate is
// Inf, which needs no accounting.
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
