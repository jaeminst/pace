package limiter

import "github.com/jaeminst/pace/registry"

// ReloadQuotas re-reads [Spec.Quota] for every user currently holding
// in-memory state and applies the result to their live bucket, keeping the
// tokens they have already accrued. Call it when whatever that function reads
// has changed.
//
// Users not in memory need nothing: their bucket is built from Spec.Quota the
// next time they appear. Before this existed, changing a quota meant building a
// new Limiter, which dropped every bucket in the process.
//
// It walks every shard, so it is a maintenance operation rather than something
// to call per request. Each shard is copied under its own read lock and
// released before Spec.Quota is consulted, so a slow one never blocks a
// request — at the cost that the reload is a series of per-shard snapshots
// rather than one instant across the whole Limiter.
func (l *Limiter) ReloadQuotas() { l.reg.Reload() }

// Quota returns the rate and burst in force for this user.
//
// While the user holds in-memory state this is what their bucket is actually
// enforcing, which can differ from what [Spec.Quota] would return now —
// see [Limiter.ReloadQuotas]. Otherwise it is what they would be given on their
// next request. Unlike [Client.Tokens] it always has an answer, because a quota
// is configuration rather than state.
func (c *Client) Quota() Quota {
	l := c.lim
	if u, ok := l.reg.Lookup(c.userID); ok {
		return quotaOf(u)
	}
	return l.cfg.Quota(c.userID)
}

// quotaOf reports what this user's bucket is currently enforcing.
//
// The bucket is the source of truth, not Config: Config.QuotaFor may have
// given this user their own, and [Limiter.ReloadQuotas] may have changed it
// since. Every report — LimitError, ThrottleInfo, Client.Quota, and the
// TakeRequest handed to a shared backend — reads it from here.
func quotaOf(u *registry.User) Quota {
	return Quota{Rate: Limit(u.Bucket().Limit()), Burst: u.Bucket().Burst()}
}
