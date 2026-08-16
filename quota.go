package pace

import "math"

// Quota is the rate and burst in force for one user.
//
// The zero Quota selects [Config.Rate] and [Config.Burst], and each field falls
// back independently. That is deliberate rather than incidental: a
// [Config.QuotaFor] backed by a map returns the zero Quota for every user the
// map does not mention, which is exactly the default those users should get.
//
// Persisted token state carries no quota. A user restored from a [StateStore]
// is given whatever QuotaFor returns at that moment, and their saved tokens are
// capped at the current burst — so lowering someone's tier takes effect on
// their next restore instead of granting them a ceiling they no longer have.
type Quota struct {
	// Rate is the maximum request rate. Zero or negative selects Config.Rate.
	Rate Limit

	// Burst is the token ceiling. Zero or negative selects Config.Burst.
	Burst int
}

// quotaFor resolves the quota in force for userID, filling in the Limiter-wide
// defaults for anything the caller left unset.
//
// It runs caller-supplied code, so every call site must be outside any shard
// lock. See [Limiter.userFor] and [Limiter.ReloadQuotas].
func (l *Limiter) quotaFor(userID string) Quota {
	q := Quota{Rate: l.cfg.Rate, Burst: l.cfg.Burst}
	if l.cfg.QuotaFor == nil {
		return q
	}
	got := l.cfg.QuotaFor(userID)
	if got.Rate > 0 {
		q.Rate = got.Rate
	}
	if got.Burst > 0 {
		q.Burst = got.Burst
	}
	q.Rate = finiteRate(q.Rate)
	return q
}

// finiteRate maps a true infinity onto [Inf], the value the token bucket can
// actually work with.
//
// [Limit] is a float64, so nothing stops a caller writing Limit(math.Inf(1)) —
// which reads as "no limit" and is a reasonable thing to try. But Inf is
// math.MaxFloat64 rather than a real infinity precisely so the arithmetic
// downstream stays defined: handing x/time/rate a genuine +Inf produces a
// bucket whose token count is NaN, and one that therefore refuses every request
// for the life of the process. Found by fuzzing RestoreBucket.
//
// A NaN needs no case here. It fails the `> 0` test above, so a NaN from
// QuotaFor falls back to the validated Config.Rate; a NaN in Config.Rate itself
// is rejected by validate.
func finiteRate(r Limit) Limit {
	if math.IsInf(float64(r), 0) {
		return Inf
	}
	return r
}

// ReloadQuotas re-reads [Config.QuotaFor] for every user currently holding
// in-memory state and applies the result to their live bucket, keeping the
// tokens they have already accrued. Call it when whatever QuotaFor reads has
// changed.
//
// Users not in memory need nothing: their bucket is built from QuotaFor the
// next time they appear. Before this existed, changing a quota meant building a
// new Limiter, which dropped every bucket in the process.
//
// It walks every shard, so it is a maintenance operation rather than something
// to call per request. Each shard is copied under its own read lock and
// released before QuotaFor is consulted, so a slow QuotaFor never blocks a
// request — at the cost that the reload is a series of per-shard snapshots
// rather than one instant across the whole Limiter.
func (l *Limiter) ReloadQuotas() {
	type entry struct {
		userID string
		u      *user
	}
	var batch []entry
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.RLock()
		batch = batch[:0]
		for id, u := range sh.users {
			batch = append(batch, entry{userID: id, u: u})
		}
		sh.mu.RUnlock()

		for _, e := range batch {
			q := l.quotaFor(e.userID)
			// Read the clock per user rather than once for the whole walk.
			// SetQuotaAt stamps the bucket's last-updated instant, so a `now`
			// captured before 256 shards' worth of QuotaFor calls rewinds every
			// bucket touched after it — and the rewound interval is refilled a
			// second time, handing free tokens to anyone who made a request
			// while the reload was in progress.
			e.u.bucket.SetQuotaAt(l.cfg.Clock.Now(), float64(q.Rate), q.Burst)
		}
	}
}

// Quota returns the rate and burst in force for this user.
//
// While the user holds in-memory state this is what their bucket is actually
// enforcing, which can differ from what [Config.QuotaFor] would return now —
// see [Limiter.ReloadQuotas]. Otherwise it is what they would be given on their
// next request. Unlike [Client.Tokens] it always has an answer, because a quota
// is configuration rather than state.
func (c *Client) Quota() Quota {
	l := c.lim
	sh := l.shardFor(c.userID)
	sh.mu.RLock()
	u, ok := sh.users[c.userID]
	sh.mu.RUnlock()
	if ok {
		return Quota{Rate: Limit(u.bucket.Limit()), Burst: u.bucket.Burst()}
	}
	return l.quotaFor(c.userID)
}
