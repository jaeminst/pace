package pace

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/internal/bucket"
)

// numShards is the default shard count. It must be a power of two for the
// bitmask fast-path in shardIndex.
const numShards = 256

// shard is padded to a cache line so that two shards' mutexes never share one.
// Without it, traffic for unrelated users on adjacent shards would contend in
// the cache even though the locks themselves never collide.
type shard struct {
	mu    sync.RWMutex     // 24 B
	users map[string]*user //  8 B
	// live mirrors len(users) so that counting the population does not mean
	// acquiring every shard lock.
	live atomic.Int64 // 8 B
	_    [24]byte     // pad to 64 B
}

type user struct {
	bucket   *bucket.Bucket
	lastUsed atomic.Int64 // unix nanoseconds; updated atomically
}

// shardIndex is FNV-1a over the raw bytes of s, inlined.
//
// It is not faster than hash/fnv — both measure ~20ns for a 32-byte ID, since
// the interface dispatch there is once per Write, not once per byte. What it
// avoids is depending on escape analysis: hash/fnv needs []byte(s) to stay off
// the heap, which holds today only because the compiler can prove it.
//
// Index rather than range: ranging a string decodes UTF-8 runes, which would
// both cost more and produce a different hash for non-ASCII IDs.
func shardIndex(s string, mask uint32) uint32 {
	const (
		offset32 = 2166136261
		prime32  = 16777619
	)
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h & mask
}

// newShards allocates n shards as one block rather than n separately allocated
// pointers, so a lookup is an index rather than a pointer chase.
func newShards(n int) []shard {
	shards := make([]shard, n)
	for i := range shards {
		shards[i].users = make(map[string]*user)
	}
	return shards
}

func (l *Limiter) shardFor(userID string) *shard {
	return &l.shards[shardIndex(userID, l.shardMask)]
}

func (l *Limiter) userFor(ctx context.Context, userID string) *user {
	sh := l.shardFor(userID)
	// hot path: existing user needs only a read lock
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if ok {
		return u
	}
	// cold path: new user
	l.fireGetOrCreate()
	// Load before taking the write lock. A custom StateStore may be backed by
	// Redis or Postgres, and holding a shard closed across a network round-trip
	// blocks every user that hashes to it. Two concurrent first-requests for
	// the same user may both Load, but the read is idempotent and the loser's
	// result is discarded — strictly better than serialising I/O under a lock.
	st, found := l.loadState(ctx, userID)
	// Resolved here for the same reason loadState is: QuotaFor is the caller's
	// code, and no caller-supplied function may run with a shard held shut.
	q := l.quotaFor(userID)

	sh.mu.Lock()
	if u, ok = sh.users[userID]; ok {
		sh.mu.Unlock()
		return u
	}
	u = l.newUser(q, st, found)
	sh.users[userID] = u
	sh.live.Add(1)
	sh.mu.Unlock()
	return u
}

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

func (l *Limiter) newUser(q Quota, st State, found bool) *user {
	u := &user{}
	now := l.cfg.Clock.Now()
	if found {
		u.bucket = bucket.RestoreBucket(float64(q.Rate), q.Burst, st.Tokens, st.LastUsed, now)
		u.lastUsed.Store(st.LastUsed.UnixNano())
	} else {
		u.bucket = bucket.NewBucket(float64(q.Rate), q.Burst)
	}
	if u.lastUsed.Load() == 0 {
		u.lastUsed.Store(now.UnixNano())
	}
	return u
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
		var dropped []string
		for i := range l.shards {
			sh := &l.shards[i]
			sh.mu.Lock()
			for id, u := range sh.users {
				if u.lastUsed.Load() < cutoff {
					delete(sh.users, id)
					sh.live.Add(-1)
					dropped = append(dropped, id)
				}
			}
			sh.mu.Unlock()
		}
		for _, id := range dropped {
			l.observeEvicted(id, EvictIdle)
		}
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
	var evicted []string
	// A user who made a request in between keeps their live bucket; the value
	// written in phase 2 is simply an older snapshot of state that is still in
	// memory and will be saved again at its next eviction, so nothing is lost.
	for _, sn := range expired {
		sh := &l.shards[sn.shardIdx]
		sh.mu.Lock()
		if cur, ok := sh.users[sn.userID]; ok && cur == sn.u && cur.lastUsed.Load() == sn.lastUsed {
			delete(sh.users, sn.userID)
			sh.live.Add(-1)
			evicted = append(evicted, sn.userID)
		}
		sh.mu.Unlock()
	}
	for _, id := range evicted {
		l.observeEvicted(id, EvictIdle)
	}
	l.fireAfterSweep()
}
