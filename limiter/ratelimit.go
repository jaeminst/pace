package limiter

import (
	"context"

	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/registry"
)

// acquire blocks until userID has a token or ctx is done. ctx must already be
// merged with the Limiter's lifetime via withLifetime. Callers are responsible
// for the activeWg registration around it.
func (l *Limiter) acquire(ctx context.Context, userID string) error {
	l.stats.requests.Add(1)
	now := l.cfg.Now()
	u := l.reg.GetOrCreate(ctx, userID)
	u.Touch(now)

	if q := (quotaOf(u)); l.sharedEnabled(q) {
		return l.throttledFromGate(userID, u, l.gate.Acquire(ctx, userID, u.Bucket(), float64(q.Rate), q.Burst))
	}

	if u.Bucket().TokensAt(now) < 1 {
		l.reportThrottle(ctx, userID, u, u.Bucket().DelayAt(now), now)
	}
	l.fireBeforeWait()
	if err := u.Bucket().Wait(ctx); err != nil {
		// Ask the Limiter's own context whether it shut down, rather than
		// inferring it from the caller's context still being live; see
		// Limiter.throttled.
		return l.throttled(userID, u, err)
	}
	return nil
}

// allow consumes a token for userID if one is immediately available.
func (l *Limiter) allow(ctx context.Context, userID string) bool {
	if !l.enter() {
		return false
	}
	defer l.leave()

	l.stats.requests.Add(1)
	now := l.cfg.Now()
	// The caller's context merged with the Limiter's lifetime, so a Close
	// arriving mid-load cancels the store read. StoreTimeout is not applied
	// here: the persistence adapter bounds every call it makes to the store,
	// which is the I/O this is about. Wrapping again out here would put a
	// *store* timeout around lock acquisition too, and would leave acquire —
	// which never wrapped — enforcing a different rule from its two siblings.
	ctx, release := l.withLifetime(ctx)
	defer release()
	u := l.reg.GetOrCreate(ctx, userID)
	u.Touch(now)

	q := quotaOf(u)
	if l.sharedEnabled(q) {
		ok, delay, tokens := l.gate.Allow(ctx, userID, u.Bucket(), float64(q.Rate), q.Burst, now)
		if !ok {
			l.reportBucketTokens(ctx, userID, u.Bucket(), delay, now, tokens)
		}
		return ok
	}

	if !u.Bucket().AllowAt(now) {
		l.reportThrottle(ctx, userID, u, u.Bucket().DelayAt(now), now)
		return false
	}
	return true
}

// tokens reports the available tokens for userID, and whether the user has
// in-memory state at all.
func (l *Limiter) tokens(userID string) (float64, bool) {
	u, ok := l.reg.Lookup(userID)
	if !ok {
		return 0, false
	}
	return u.Bucket().TokensAt(l.cfg.Now()), true
}

// quotaOf reports what this user's bucket is currently enforcing.
//
// The bucket is the source of truth, not the configuration:
// config.Config.QuotaFor may have given this user their own, and
// [Limiter.ReloadQuotas] may have changed it since. Every report — LimitError,
// ThrottleInfo, client.Client.Quota, and the TakeRequest handed to a shared
// backend — reads it from here.
func quotaOf(u *registry.User) config.Quota {
	return config.Quota{Rate: config.Limit(u.Bucket().Limit()), Burst: u.Bucket().Burst()}
}
