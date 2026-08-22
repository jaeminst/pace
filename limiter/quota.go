// quota.go is the one place this package turns a key into a quota.
//
// There are two inputs and no precedence rule between them: config.Config.Quota
// is the rate, and config.WithQuotaFor — if a caller passed one — is handed
// that rate and decides what this key gets instead. The override is told what
// it is overriding, so neither has to win.

package limiter

import (
	"math"

	"github.com/jaeminst/pace/bucket"
)

// quotaFor resolves the quota in force for key.
//
// Without config.WithQuotaFor this is Config.Quota for everyone, already
// checked and normalised by Config.Resolve — the whole reason the default is a
// value and not a hook. With it, the caller's function is handed that value and
// answers for this key, and what comes back has to be normalised here: it
// arrives one key at a time, from caller code, on the goroutine building that
// key's bucket, long after Resolve could have looked at it.
//
// An unusable rate is not an error a caller can be handed — there is no call to
// return it from — so it is logged and clamped to zero, which is a bucket that
// never refills. That key is throttled to a standstill rather than a NaN
// reaching the arithmetic: it fails closed, and quietly.
func (l *Limiter) quotaFor(key string) bucket.Quota {
	if l.opts.QuotaFor == nil {
		return l.cfg.Quota
	}
	q := l.opts.QuotaFor(key, l.cfg.Quota)
	if q.Burst <= 0 {
		q.Burst = 1
	}
	q.Rate = bucket.Finite(q.Rate)
	if q.Rate <= 0 || math.IsNaN(float64(q.Rate)) {
		l.cfg.Logger.Warn("pace: WithQuotaFor returned an unusable rate; throttling this key to a standstill",
			"key", key, "rate", float64(q.Rate))
		// Clamped here rather than left for the bucket, because a key with no
		// bucket yet is answered from this function: reporting the rate a
		// caller wrote and enforcing the one the bucket floors it to would be
		// two answers to the same question.
		q.Rate = 0
	}
	return q
}

// ReloadQuota re-reads the quota for one key and applies it to their live
// bucket, keeping the tokens they have already accrued. It reports whether that
// key had a bucket in memory at all.
//
// This is [Limiter.ReloadQuotas] for a single key, and it is O(1) where that is
// a walk of every shard. Before it existed the choices were that walk or
// [Limiter.Evict], and Evict is not the same thing: it drops the accrued tokens
// and writes to the store on the way out.
func (l *Limiter) ReloadQuota(key string) bool { return l.reg.ReloadKey(key) }
