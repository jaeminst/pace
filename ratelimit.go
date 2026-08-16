package pace

import "context"

// acquire blocks until userID has a token or ctx is done. ctx must already be
// merged with the Limiter's lifetime via withLifetime. Callers are responsible
// for the activeWg registration around it.
func (l *Limiter) acquire(ctx context.Context, userID string) error {
	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	u := l.userFor(ctx, userID)
	u.lastUsed.Store(now.UnixNano())

	if q := (Quota{Rate: Limit(u.bucket.Limit()), Burst: u.bucket.Burst()}); l.sharedEnabled(q) {
		return l.acquireShared(ctx, userID, u, q)
	}

	if tokens := u.bucket.TokensAt(now); tokens < 1 {
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: userID,
			Delay:  u.bucket.DelayAt(now),
			Tokens: tokens,
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
		})
	}
	l.fireBeforeWait()
	if err := u.bucket.Wait(ctx); err != nil {
		// Ask the Limiter's own context whether it shut down, rather than
		// inferring it from the caller's context still being live. The
		// limiter reports "would exceed context deadline" without waiting,
		// so ctx.Err() is legitimately nil in that case too — treating that
		// as ErrClosed told callers the Limiter was closed when it was not.
		if l.ctx.Err() != nil {
			return ErrClosed
		}
		return &LimitError{
			UserID: userID,
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
			// Measured at the point of failure, not at entry: this is the
			// number a caller reads to decide when to try again.
			Delay: u.bucket.DelayAt(l.cfg.Clock.Now()),
			Err:   err,
		}
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
	now := l.cfg.Clock.Now()
	// The caller's context merged with the Limiter's lifetime, so a Close
	// arriving mid-load cancels the store read, then bounded by StoreTimeout so
	// a caller who passed a context without a deadline still gets one.
	ctx, release := l.withLifetime(ctx)
	defer release()
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()
	u := l.userFor(ctx, userID)
	u.lastUsed.Store(now.UnixNano())

	q := Quota{Rate: Limit(u.bucket.Limit()), Burst: u.bucket.Burst()}
	if l.sharedEnabled(q) {
		return l.allowShared(ctx, userID, u, q, now)
	}

	if !u.bucket.AllowAt(now) {
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: userID,
			Delay:  u.bucket.DelayAt(now),
			Tokens: u.bucket.TokensAt(now),
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
		})
		return false
	}
	return true
}

// tokens reports the available tokens for userID, and whether the user has
// in-memory state at all.
func (l *Limiter) tokens(userID string) (float64, bool) {
	sh := l.shardFor(userID)
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return u.bucket.TokensAt(l.cfg.Clock.Now()), true
}
