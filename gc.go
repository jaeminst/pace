package pace

import (
	"context"
	"fmt"
	"time"
)

// evictUser removes userID from memory, persisting the current token state
// first when a store is configured.
//
// The shard lock covers only the map surgery. Both of the things that follow —
// the store write and the observer callback — run outside it, for the reason
// the sweep already documents: neither may be executed with a shard held shut.
// An observer that calls back into the Limiter would otherwise deadlock against
// the very lock this function took, and a slow store would stall every user who
// hashes to this shard.
//
// Unlike the sweep the store write is still synchronous, because Evict's
// contract is that state is persisted by the time it returns. The observer
// fires after it, so a failed save is not reported as a clean eviction.
func (l *Limiter) evictUser(ctx context.Context, userID string) (bool, error) {
	if !l.enter() {
		return false, ErrClosed
	}
	defer l.leave()

	now := l.cfg.Clock.Now()
	sh := l.shardFor(userID)

	sh.mu.Lock()
	u, ok := sh.users[userID]
	if ok {
		delete(sh.users, userID)
		sh.live.Add(-1)
	}
	sh.mu.Unlock()

	if !ok {
		return false, nil
	}

	if l.persistsState() {
		sn := snap{userID: userID, tokens: u.bucket.TokensAt(now), lastUsed: u.lastUsed.Load()}
		saveCtx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
		defer cancel()
		if err := l.store.Save(saveCtx, userID, sn.state()); err != nil {
			return true, fmt.Errorf("pace: evict %q: %w", userID, err)
		}
	}
	l.observeEvicted(EvictInfo{
		UserID:   userID,
		Reason:   EvictExplicit,
		Tokens:   u.bucket.TokensAt(now),
		LastUsed: time.Unix(0, u.lastUsed.Load()),
	})
	return true, nil
}

// dropUsers discards every user's in-memory state, counts the drop as
// evictions, and tells the observer about each one.
//
// It empties the shards rather than only reading them. A Stats call after Close
// must not report a population that no longer exists — the alternative is a
// snapshot that says "N users" and "+N evictions" at the same time, which
// cannot both be true.
//
// It runs after the final flush, so the state it discards has already been
// persisted. The observer fires outside the shard lock, as everywhere else.
func (l *Limiter) dropUsers() {
	notify := l.observesEvictions()
	now := l.cfg.Clock.Now()

	var dropped int64
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		var infos []EvictInfo
		if notify {
			infos = make([]EvictInfo, 0, len(sh.users))
			for id, u := range sh.users {
				infos = append(infos, EvictInfo{
					UserID:   id,
					Reason:   EvictShutdown,
					Tokens:   u.bucket.TokensAt(now),
					LastUsed: time.Unix(0, u.lastUsed.Load()),
				})
			}
		}
		dropped += int64(len(sh.users))
		clear(sh.users)
		sh.live.Store(0)
		sh.mu.Unlock()

		// observeEvicted counts as it notifies.
		for _, info := range infos {
			l.observeEvicted(info)
		}
	}
	if !notify {
		// Nobody is listening, so the per-user list was never built; count the
		// drop in one go instead.
		l.stats.evictions.Add(max(0, dropped))
	}
}

func (l *Limiter) gcLoop() {
	defer l.gcWg.Done()
	ticker := time.NewTicker(l.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.sweep()
			l.purgeResults()
		case <-l.ctx.Done():
			return
		}
	}
}

// sweepInPlace evicts idle users when there is no state to persist.
//
// The ID list exists only so the observer can be notified outside the lock.
// With no observer there is nobody to notify, and building it anyway allocated
// a slice the size of the whole eviction — 57KB per sweep of 2,000 users, on
// the one path whose entire point is that it does almost nothing.
func (l *Limiter) sweepInPlace(cutoff int64) {
	notify := l.observesEvictions()
	now := l.cfg.Clock.Now()
	var dropped []EvictInfo
	var n int64
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		for id, u := range sh.users {
			if u.lastUsed.Load() < cutoff {
				delete(sh.users, id)
				sh.live.Add(-1)
				n++
				if notify {
					dropped = append(dropped, EvictInfo{
						UserID:   id,
						Reason:   EvictIdle,
						Tokens:   u.bucket.TokensAt(now),
						LastUsed: time.Unix(0, u.lastUsed.Load()),
					})
				}
			}
		}
		sh.mu.Unlock()
	}
	if !notify {
		l.stats.evictions.Add(n)
		return
	}
	// observeEvicted counts as it notifies.
	for _, info := range dropped {
		l.observeEvicted(info)
	}
}

// sweep evicts idle users in three phases, so that no store I/O ever happens
// while a shard lock is held.
//
// Doing it in one pass — save and delete under the write lock — made every
// eviction a synchronous SQLite transaction that blocked live requests hashing
// to that shard. The order below also matters: deleting before persisting would
// let a fresh request for the same user load stale state, which the pending
// write would then overwrite.
func (l *Limiter) sweep() {
	now := l.cfg.Clock.Now()
	cutoff := now.Add(-l.cfg.IdleExpiry).UnixNano()

	// With nothing to persist there is no I/O to move out of the lock, so the
	// extra snapshot pass would be pure overhead. Evict in place.
	if !l.persistsState() {
		l.sweepInPlace(cutoff)
		l.fireAfterSweep()
		return
	}

	// Phase 1: collect. No I/O, no mutation.
	var expired []snap
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.RLock()
		for id, u := range sh.users {
			if lu := u.lastUsed.Load(); lu < cutoff {
				expired = append(expired, snap{
					u:        u,
					userID:   id,
					shardIdx: uint32(i),
					tokens:   u.bucket.TokensAt(now),
					lastUsed: lu,
				})
			}
		}
		sh.mu.RUnlock()
	}
	if len(expired) == 0 {
		l.fireAfterSweep()
		return
	}

	// Phase 2: persist, holding nothing.
	l.flush(expired)

	// Phase 3: delete, but only what has not been touched since the snapshot.
	var evicted []EvictInfo
	// A user who made a request in between keeps their live bucket; the value
	// written in phase 2 is simply an older snapshot of state that is still in
	// memory and will be saved again at its next eviction, so nothing is lost.
	for _, sn := range expired {
		sh := &l.shards[sn.shardIdx]
		sh.mu.Lock()
		if cur, ok := sh.users[sn.userID]; ok && cur == sn.u && cur.lastUsed.Load() == sn.lastUsed {
			delete(sh.users, sn.userID)
			sh.live.Add(-1)
			evicted = append(evicted, EvictInfo{
				UserID:   sn.userID,
				Reason:   EvictIdle,
				Tokens:   sn.tokens,
				LastUsed: time.Unix(0, sn.lastUsed),
			})
		}
		sh.mu.Unlock()
	}
	for _, info := range evicted {
		l.observeEvicted(info)
	}
	l.fireAfterSweep()
}
