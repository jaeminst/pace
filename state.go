package pace

import (
	"context"
	"time"
)

// loadState reads a user's persisted state, if any. A store error is logged and
// treated as "no saved state": a fresh bucket is the safe fallback, and failing
// the request because persistence is unavailable would be worse than briefly
// granting a full burst.
func (l *Limiter) loadState(ctx context.Context, userID string) (State, bool) {
	if !l.persistsState() {
		return State{}, false
	}
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()
	st, found, err := l.store.Load(ctx, userID)
	if err != nil {
		l.cfg.Logger.Warn("pace: load user state", "user", userID, "err", err)
		return State{}, false
	}
	return st, found
}

// snap is a point-in-time copy of one user's state, taken under a shard lock so
// that persistence can happen without holding one.
type snap struct {
	u        *user
	userID   string
	shardIdx uint32
	tokens   float64
	lastUsed int64
}

func (s snap) state() State {
	return State{Tokens: s.tokens, LastUsed: time.Unix(0, s.lastUsed)}
}

func (l *Limiter) saveAll() {
	now := l.cfg.Clock.Now()
	var all []snap
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.RLock()
		for id, u := range sh.users {
			all = append(all, snap{
				userID:   id,
				tokens:   u.bucket.TokensAt(now),
				lastUsed: u.lastUsed.Load(),
			})
		}
		sh.mu.RUnlock()
	}
	l.flush(all)
}

// flush persists snapshots with no lock held. Stores that can write a batch in
// one transaction do; the rest fall back to one call per user, still outside
// every lock.
//
// It runs on context.Background rather than the Limiter's context: the final
// flush happens after the Limiter has been cancelled, and inheriting a
// cancelled context would discard exactly the state Close exists to save.
// StoreTimeout is what bounds it instead.
func (l *Limiter) flush(snaps []snap) {
	if !l.persistsState() || len(snaps) == 0 {
		return
	}
	if bs, ok := l.store.(BatchStateStore); ok {
		const chunk = 512 // bound each round-trip so one sweep cannot monopolise the store
		batch := make([]UserState, 0, min(chunk, len(snaps)))
		for start := 0; start < len(snaps); start += chunk {
			batch = batch[:0]
			for _, sn := range snaps[start:min(start+chunk, len(snaps))] {
				batch = append(batch, UserState{UserID: sn.userID, State: sn.state()})
			}
			ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
			err := bs.SaveBatch(ctx, batch)
			cancel()
			if err != nil {
				l.cfg.Logger.Warn("pace: flush state", "count", len(batch), "err", err)
			}
		}
		return
	}
	for _, sn := range snaps {
		ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
		err := l.store.Save(ctx, sn.userID, sn.state())
		cancel()
		if err != nil {
			l.cfg.Logger.Warn("pace: flush state", "user", sn.userID, "err", err)
		}
	}
}

// persistsState reports whether per-user token state should be written to and
// read from [Config.Store].
//
// A shared quota turns the local bucket into a shadow, and a shadow must never
// be persisted. The bucket no longer describes what this user has spent — it
// describes what this replica has spent, which is a fraction of it. Restoring
// replica A's snapshot into replica B would have B throttling itself for
// traffic it never sent, and the inequality that makes the shadow safe
// (shadowTokens >= sharedTokens) is exactly what that breaks.
//
// The authoritative count lives in the backend, which is the point of
// configuring one.
func (l *Limiter) persistsState() bool {
	return l.store != nil && l.cfg.Shared.Quota == nil
}
