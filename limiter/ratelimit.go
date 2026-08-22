package limiter

import (
	"context"
)

// acquire blocks until key has a token or ctx is done. ctx must already be
// merged with the Limiter's lifetime via withLifetime. Callers are responsible
// for the activeWg registration around it.
func (l *Limiter) acquire(ctx context.Context, key string) error {
	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	u := l.reg.GetOrCreate(ctx, key)
	u.Touch(now)

	if q := u.Bucket().Quota(); l.sharedEnabled(q) {
		return l.throttledFromGate(key, u, l.gate.Acquire(ctx, key, u.Bucket()))
	}

	if u.Bucket().TokensAt(now) < 1 {
		l.reportThrottle(ctx, key, u, u.Bucket().DelayAt(now), now)
	}
	l.fireBeforeWait()
	if err := u.Bucket().Wait(ctx); err != nil {
		// Ask the Limiter's own context whether it shut down, rather than
		// inferring it from the caller's context still being live; see
		// Limiter.throttled.
		return l.throttled(key, u, err)
	}
	return nil
}

// allow consumes a token for key if one is immediately available.
func (l *Limiter) allow(ctx context.Context, key string) bool {
	if !l.enter() {
		return false
	}
	defer l.leave()

	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	// The caller's context merged with the Limiter's lifetime, so a Close
	// arriving mid-load cancels the store read. StoreTimeout is not applied
	// here: the persistence adapter bounds every call it makes to the store,
	// which is the I/O this is about. Wrapping again out here would put a
	// *store* timeout around lock acquisition too, and would leave acquire —
	// which never wrapped — enforcing a different rule from its two siblings.
	ctx, release := l.withLifetime(ctx)
	defer release()
	u := l.reg.GetOrCreate(ctx, key)
	u.Touch(now)

	q := u.Bucket().Quota()
	if l.sharedEnabled(q) {
		ok, delay, tokens := l.gate.Allow(ctx, key, u.Bucket(), now)
		if !ok {
			l.reportBucketTokens(ctx, key, u.Bucket(), delay, now, tokens)
		}
		return ok
	}

	if !u.Bucket().AllowAt(now) {
		l.reportThrottle(ctx, key, u, u.Bucket().DelayAt(now), now)
		return false
	}
	return true
}

// tokens reports the available tokens for key, and whether the key has
// in-memory state at all.
func (l *Limiter) tokens(key string) (float64, bool) {
	u, ok := l.reg.Lookup(key)
	if !ok {
		return 0, false
	}
	return u.Bucket().TokensAt(l.cfg.Clock.Now()), true
}
