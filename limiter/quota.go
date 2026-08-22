// quota.go is the default quota as live state: the one thing a caller writes in
// a config.Config that this Limiter keeps changing after New.
//
// Everything else in the Config is snapshotted at construction and read from an
// immutable copy. This one field is not, because "adjust the rate at run time"
// is an operation an operator performs on a running process, and the whole
// population's default is the natural handle for it.

package limiter

import (
	"errors"
	"math"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/config"
)

// quotaFor resolves the quota in force for userID: the current default, with
// whatever config.Config.QuotaFor says about this user folded over it.
//
// One load of the default per call, so the rate and the burst are always a pair
// somebody set.
func (l *Limiter) quotaFor(userID string) bucket.Quota {
	return l.cfg.QuotaWith(*l.quota.Load(), userID)
}

// DefaultQuota returns the default in force: what a user gets when
// config.Config.QuotaFor names nothing for them.
//
// It starts as the Config's Rate and Burst and changes only through
// [Limiter.SetDefaultQuota].
func (l *Limiter) DefaultQuota() bucket.Quota { return *l.quota.Load() }

// SetDefaultQuota changes the default every user falls back to, and reports a
// [*github.com/jaeminst/pace/config.Error] for a quota the engine cannot use.
//
// It takes effect immediately for users whose bucket does not exist yet — their
// first request, or their first after an eviction. Buckets already in memory
// keep what they have until [Limiter.ReloadQuotas] or [Limiter.ReloadQuota]
// re-reads it for them, which is the same explicit step a change behind
// config.Config.QuotaFor needs and for the same reason: applying it means
// walking the population, and that is a maintenance operation rather than
// something to do on a request.
//
// Call this and then ReloadQuotas from the same goroutine and the change lands
// uniformly. Calling them concurrently can leave a population permanently
// split, because nothing re-runs the walk — there is no eventual convergence
// here, only the order you impose.
//
// It takes a whole [github.com/jaeminst/pace/bucket.Quota] on purpose. A setter
// for one half would have to read the other, and two concurrent callers doing
// read-modify-write lose an update; taking both keeps this a single store.
func (l *Limiter) SetDefaultQuota(q bucket.Quota) error {
	// The same normalisation config.Config.Resolve performs, because this value
	// arrives after Resolve has run and nothing downstream re-checks it.
	if q.Rate <= 0 || math.IsNaN(float64(q.Rate)) {
		return &config.Error{
			Field: "Rate",
			Value: q.Rate,
			Err:   errors.New("must be greater than zero"),
		}
	}
	if q.Burst <= 0 {
		q.Burst = 1
	}
	q.Rate = bucket.Finite(q.Rate)

	l.quota.Store(&q)
	return nil
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
