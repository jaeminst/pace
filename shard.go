package pace

import (
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/internal/bucket"
	"github.com/jaeminst/pace/internal/store"
)

const (
	numShards = 256 // must be a power of two for the bitmask fast-path
	shardMask = numShards - 1
)

type shard struct {
	mu    sync.RWMutex
	users map[string]*user
}

type user struct {
	bucket   *bucket.Bucket
	lastUsed atomic.Int64 // unix nanoseconds; updated atomically
}

func (c *engine) shardFor(userID string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return c.shards[h.Sum32()&shardMask]
}

func (c *engine) userFor(userID string) *user {
	sh := c.shardFor(userID)
	// hot path: existing user needs only a read lock
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if ok {
		return u
	}
	// cold path: new user — double-check under write lock to avoid races
	if hook := c._testHookGetOrCreate; hook != nil {
		hook()
	}
	sh.mu.Lock()
	if u, ok = sh.users[userID]; ok {
		sh.mu.Unlock()
		return u
	}
	u = c.newUser(userID)
	sh.users[userID] = u
	sh.mu.Unlock()
	return u
}

func (c *engine) newUser(userID string) *user {
	u := &user{}
	now := c.clock.Now()
	if c.store != nil {
		if ss, found, err := c.store.Load(userID); err == nil && found {
			u.bucket = bucket.RestoreBucket(float64(c.cfg.Rate), c.cfg.Burst, ss.Tokens, time.Unix(0, ss.LastUsed), now)
			u.lastUsed.Store(ss.LastUsed)
		} else if err != nil {
			c.logger.Warn("pace: load user state", "user", userID, "err", err)
		}
	}
	if u.bucket == nil {
		u.bucket = bucket.NewBucket(float64(c.cfg.Rate), c.cfg.Burst)
	}
	if u.lastUsed.Load() == 0 {
		u.lastUsed.Store(now.UnixNano())
	}
	return u
}

func (c *engine) saveAll() {
	now := c.clock.Now()
	for _, sh := range c.shards {
		sh.mu.RLock()
		for id, u := range sh.users {
			if err := c.store.Save(id, u.bucket.TokensAt(now), u.lastUsed.Load()); err != nil {
				c.logger.Warn("pace: save on close", "user", id, "err", err)
			}
		}
		sh.mu.RUnlock()
	}
}

func (c *engine) gcLoop() {
	defer c.gcWg.Done()
	ticker := time.NewTicker(c.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.sweep()
		case <-c.ctx.Done():
			return
		}
	}
}

// evict saves state to the store (if configured) and removes userID from
// sh. Must be called with sh.mu held for writing.
func (c *engine) evict(sh *shard, userID string, u *user, now time.Time) {
	if c.store != nil {
		if err := c.store.Save(userID, u.bucket.TokensAt(now), u.lastUsed.Load()); err != nil {
			c.logger.Warn("pace: evict save", "user", userID, "err", err)
		}
	}
	delete(sh.users, userID)
}

func (c *engine) sweep() {
	now := c.clock.Now()
	cutoff := now.Add(-c.idleExpiry).UnixNano()
	for _, sh := range c.shards {
		sh.mu.Lock()
		for id, u := range sh.users {
			if u.lastUsed.Load() < cutoff {
				c.evict(sh, id, u, now)
			}
		}
		sh.mu.Unlock()
	}
}

// storer is the internal persistence interface. *store.Store (SQLite) and
// storeWrapper (wrapping a public StateStore) both satisfy it.
type storer interface {
	Save(userID string, tokens float64, lastUsed int64) error
	Load(userID string) (store.SavedState, bool, error)
	Close() error
}

// storeWrapper adapts a public StateStore to the internal storer interface.
type storeWrapper struct{ s StateStore }

func (w *storeWrapper) Save(userID string, tokens float64, lastUsed int64) error {
	return w.s.Save(userID, SavedState{Tokens: tokens, LastUsed: lastUsed})
}

func (w *storeWrapper) Load(userID string) (store.SavedState, bool, error) {
	ss, found, err := w.s.Load(userID)
	if err != nil || !found {
		return store.SavedState{}, found, err
	}
	return store.SavedState{Tokens: ss.Tokens, LastUsed: ss.LastUsed}, true, nil
}

func (w *storeWrapper) Close() error { return w.s.Close() }
