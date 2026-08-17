package limiter

import "github.com/jaeminst/pace/limit"

// quotaFor resolves the quota in force for userID, filling in the Limiter-wide
// defaults for anything the caller left unset.
//
// It runs caller-supplied code, so every call site must be outside any shard
// lock. See [Limiter.userFor] and [Limiter.ReloadQuotas].
func (l *Limiter) quotaFor(userID string) limit.Quota {
	q := limit.Quota{Rate: l.cfg.Rate, Burst: l.cfg.Burst}
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
	q.Rate = limit.Finite(q.Rate)
	return q
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
func (l *Limiter) ReloadQuotas() { l.reg.Reload() }

// Quota returns the rate and burst in force for this user.
//
// While the user holds in-memory state this is what their bucket is actually
// enforcing, which can differ from what [Config.QuotaFor] would return now —
// see [Limiter.ReloadQuotas]. Otherwise it is what they would be given on their
// next request. Unlike [Client.Tokens] it always has an answer, because a quota
// is configuration rather than state.
func (c *Client) Quota() limit.Quota {
	l := c.lim
	if u, ok := l.reg.Lookup(c.userID); ok {
		return quotaOf(u)
	}
	return l.quotaFor(c.userID)
}
