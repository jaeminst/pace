// quota.go is the one place this package turns a user ID into a quota.
//
// There is no default kept beside config.Config.QuotaFor and no setter that
// overrides it. A function of a user ID already expresses both "everyone gets
// this" and "this user gets that", so a second handle on the same value would
// only be a second answer to give when the two disagree.

package limiter

import (
	"math"

	"github.com/jaeminst/pace/bucket"
)

// quotaFor resolves the quota in force for userID and normalises it.
//
// The normalisation is here rather than in config.Config.Resolve because this
// value does not exist at Resolve time: it arrives one user at a time, from
// caller code, on the goroutine that is creating their bucket. Resolve can
// check that the hook is present; only this can see what it returns.
//
// An unusable rate is not an error a caller can be handed — there is no call to
// return it from — so it is logged and clamped to zero, which is a bucket that
// never refills. That user is throttled to a standstill rather than a NaN
// reaching the arithmetic: it fails closed, and quietly, which is the price of
// moving the check off Resolve.
func (l *Limiter) quotaFor(userID string) bucket.Quota {
	q := l.cfg.QuotaFor(userID)
	if q.Burst <= 0 {
		q.Burst = 1
	}
	q.Rate = bucket.Finite(q.Rate)
	if q.Rate <= 0 || math.IsNaN(float64(q.Rate)) {
		l.cfg.Logger.Warn("pace: QuotaFor returned an unusable rate; throttling this user to a standstill",
			"user", userID, "rate", float64(q.Rate))
		// Clamped here rather than left for the bucket, because a user with no
		// bucket yet is answered from this function: reporting the rate a
		// caller wrote and enforcing the one the bucket floors it to would be
		// two answers to the same question.
		q.Rate = 0
	}
	return q
}

// ReloadQuota re-reads the quota for one user and applies it to their live
// bucket, keeping the tokens they have already accrued. It reports whether that
// user had a bucket in memory at all.
//
// This is [Limiter.ReloadQuotas] for a single user, and it is O(1) where that is
// a walk of every shard. Before it existed the choices were that walk or
// [Limiter.Evict], and Evict is not the same thing: it drops the accrued tokens
// and writes to the store on the way out.
func (l *Limiter) ReloadQuota(userID string) bool { return l.reg.ReloadUser(userID) }
