package registry

import (
	"context"
	"time"
)

// report notifies the owner of one eviction and counts it.
//
// Counting happens here rather than in the owner so that the two paths which
// skip the notification — nobody is listening, so no per-user list is built —
// can still count in bulk against the same tally. It must be called with no
// shard lock held.
func (r *Registry) report(e Eviction) {
	r.evictions.Add(1)
	r.cfg.OnEvict(e)
}

// Evict removes userID from memory, persisting the current token state first
// when the owner has somewhere to put it.
//
// The shard lock covers only the map surgery. Both of the things that follow —
// the store write and the eviction report — run outside it, for the reason the
// sweep already documents: neither may be executed with a shard held shut. An
// owner's hook that calls back in would otherwise deadlock against the very
// lock this function took, and a slow store would stall every user who hashes
// to this shard.
//
// Unlike the sweep the store write is still synchronous, because the owner's
// contract is that state is persisted by the time this returns. The report
// fires after it, so a failed save is not announced as a clean eviction.
func (r *Registry) Evict(ctx context.Context, userID string) (bool, error) {
	now := r.cfg.Now()
	sh := r.shardFor(userID)

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

	snap := u.snapshot(userID, now)
	if r.cfg.Persists() {
		if err := r.cfg.Save(ctx, snap); err != nil {
			return true, err
		}
	}
	r.report(snap.eviction(Explicit))
	return true, nil
}

// DropAll discards every user's in-memory state and counts the drop as
// evictions.
//
// It empties the shards rather than only reading them. A population count taken
// after shutdown must not report users who no longer exist — the alternative is
// a snapshot that says "N users" and "+N evictions" at the same time, which
// cannot both be true.
//
// The owner calls it after the final flush, so the state it discards has
// already been persisted. Reports fire outside the shard lock, as everywhere.
func (r *Registry) DropAll() {
	notify := r.cfg.Observes()
	now := r.cfg.Now()

	var dropped int64
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		var infos []Eviction
		if notify {
			infos = make([]Eviction, 0, len(sh.users))
			for id, u := range sh.users {
				infos = append(infos, u.snapshot(id, now).eviction(Shutdown))
			}
		}
		dropped += int64(len(sh.users))
		clear(sh.users)
		sh.live.Store(0)
		sh.mu.Unlock()

		// report counts as it notifies.
		for _, info := range infos {
			r.report(info)
		}
	}
	if !notify {
		// Nobody is listening, so the per-user list was never built; count the
		// drop in one go instead.
		r.evictions.Add(max(0, dropped))
	}
}

// Sweep evicts idle users in three phases, so that no store I/O ever happens
// while a shard lock is held.
//
// Doing it in one pass — save and delete under the write lock — made every
// eviction a synchronous store transaction that blocked live requests hashing
// to that shard. The order below also matters: deleting before persisting would
// let a fresh request for the same user load stale state, which the pending
// write would then overwrite.
func (r *Registry) Sweep() {
	now := r.cfg.Now()
	cutoff := now.Add(-r.cfg.IdleExpiry).UnixNano()

	// With nothing to persist there is no I/O to move out of the lock, so the
	// extra snapshot pass would be pure overhead. Evict in place.
	if !r.cfg.Persists() {
		r.sweepInPlace(cutoff)
		r.cfg.AfterSweep()
		return
	}

	// Phase 1: collect. No I/O, no mutation.
	type expiredUser struct {
		u        *User
		shardIdx uint32
		snap     Snapshot
		lastUsed int64
	}
	var expired []expiredUser
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.RLock()
		for id, u := range sh.users {
			if lu := u.lastUsed.Load(); lu < cutoff {
				expired = append(expired, expiredUser{
					u:        u,
					shardIdx: uint32(i),
					// Not u.snapshot: this is the one site that must reuse the
					// lu it just compared, rather than re-read it. Phase 3
					// deletes only if lastUsed still equals this value, so a
					// second read could persist a timestamp the guard then
					// disagrees with.
					snap: Snapshot{
						UserID:   id,
						Tokens:   u.bucket.TokensAt(now),
						LastUsed: time.Unix(0, lu),
					},
					lastUsed: lu,
				})
			}
		}
		sh.mu.RUnlock()
	}
	if len(expired) == 0 {
		r.cfg.AfterSweep()
		return
	}

	// Phase 2: persist, holding nothing.
	snaps := make([]Snapshot, len(expired))
	for i, e := range expired {
		snaps[i] = e.snap
	}
	r.cfg.Flush(snaps)

	// Phase 3: delete, but only what has not been touched since the snapshot.
	var evicted []Eviction
	// A user who made a request in between keeps their live bucket; the value
	// written in phase 2 is simply an older snapshot of state that is still in
	// memory and will be saved again at its next eviction, so nothing is lost.
	for _, e := range expired {
		sh := &r.shards[e.shardIdx]
		sh.mu.Lock()
		if cur, ok := sh.users[e.snap.UserID]; ok && cur == e.u && cur.lastUsed.Load() == e.lastUsed {
			delete(sh.users, e.snap.UserID)
			sh.live.Add(-1)
			evicted = append(evicted, Eviction{
				UserID:   e.snap.UserID,
				Reason:   Idle,
				Tokens:   e.snap.Tokens,
				LastUsed: e.snap.LastUsed,
			})
		}
		sh.mu.Unlock()
	}
	for _, info := range evicted {
		r.report(info)
	}
	r.cfg.AfterSweep()
}

// sweepInPlace evicts idle users when there is no state to persist.
//
// The eviction list exists only so the owner can be notified outside the lock.
// With nobody listening there is nobody to notify, and building it anyway
// allocated a slice the size of the whole eviction — 57KB per sweep of 2,000
// users, on the one path whose entire point is that it does almost nothing.
func (r *Registry) sweepInPlace(cutoff int64) {
	notify := r.cfg.Observes()
	now := r.cfg.Now()
	var dropped []Eviction
	var n int64
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.Lock()
		for id, u := range sh.users {
			if u.lastUsed.Load() < cutoff {
				delete(sh.users, id)
				sh.live.Add(-1)
				n++
				if notify {
					dropped = append(dropped, u.snapshot(id, now).eviction(Idle))
				}
			}
		}
		sh.mu.Unlock()
	}
	if !notify {
		r.evictions.Add(n)
		return
	}
	// report counts as it notifies.
	for _, info := range dropped {
		r.report(info)
	}
}
