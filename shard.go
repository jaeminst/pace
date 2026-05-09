package pace

import (
	"hash/fnv"
	"net/http"
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
	users map[string]*userBuckets
}

type endpoint struct {
	cfg    EndpointConfig
	client *http.Client
}

type userBuckets struct {
	buckets  map[string]*bucket.Bucket // immutable after creation; no lock needed for reads
	lastUsed atomic.Int64              // unix nanoseconds; updated atomically
}

func (m *Manager) shardFor(userID string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return m.shards[h.Sum32()&shardMask]
}

func (m *Manager) getOrCreateUser(userID string) *userBuckets {
	sh := m.shardFor(userID)
	// hot path: existing user needs only a read lock
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if ok {
		return u
	}
	// cold path: new user — double-check under write lock to avoid races
	sh.mu.Lock()
	if u, ok = sh.users[userID]; ok {
		sh.mu.Unlock()
		return u
	}
	u = m.createUserBuckets(userID)
	sh.users[userID] = u
	sh.mu.Unlock()
	return u
}

func (m *Manager) createUserBuckets(userID string) *userBuckets {
	u := &userBuckets{buckets: make(map[string]*bucket.Bucket, len(m.endpoints))}
	var saved map[string]store.SavedState
	if m.store != nil {
		if ss, err := m.store.Load(userID); err == nil {
			saved = ss
		} else {
			m.logger.Warn("pace: load user state", "user", userID, "err", err)
		}
	}
	now := m.clock.Now()
	for name, ep := range m.endpoints {
		if ss, ok := saved[name]; ok {
			u.buckets[name] = bucket.RestoreBucket(ep.cfg.RatePerMinute, ep.cfg.Burst, ss.Tokens, time.Unix(0, ss.LastUsed))
			if ss.LastUsed > u.lastUsed.Load() {
				u.lastUsed.Store(ss.LastUsed)
			}
		} else {
			u.buckets[name] = bucket.NewBucket(ep.cfg.RatePerMinute, ep.cfg.Burst)
		}
	}
	if u.lastUsed.Load() == 0 {
		u.lastUsed.Store(now.UnixNano())
	}
	return u
}

func (m *Manager) saveAll() {
	for _, sh := range m.shards {
		sh.mu.RLock()
		for id, u := range sh.users {
			tokens := make(map[string]float64, len(u.buckets))
			for name, b := range u.buckets {
				tokens[name] = b.Tokens()
			}
			if err := m.store.Save(id, tokens, u.lastUsed.Load()); err != nil {
				m.logger.Warn("pace: save on close", "user", id, "err", err)
			}
		}
		sh.mu.RUnlock()
	}
}

func (m *Manager) gcLoop() {
	ticker := time.NewTicker(m.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.collectIdle()
		case <-m.ctx.Done():
			return
		}
	}
}

// evictUser saves state to the store (if configured) and removes userID from
// sh. Must be called with sh.mu held for writing.
func (m *Manager) evictUser(sh *shard, userID string, u *userBuckets) {
	if m.store != nil {
		tokens := make(map[string]float64, len(u.buckets))
		for name, b := range u.buckets {
			tokens[name] = b.Tokens()
		}
		if err := m.store.Save(userID, tokens, u.lastUsed.Load()); err != nil {
			m.logger.Warn("pace: evict save", "user", userID, "err", err)
		}
	}
	delete(sh.users, userID)
}

func (m *Manager) collectIdle() {
	cutoff := m.clock.Now().Add(-m.idleExpiry).UnixNano()
	for _, sh := range m.shards {
		sh.mu.Lock()
		for id, u := range sh.users {
			if u.lastUsed.Load() < cutoff {
				m.evictUser(sh, id, u)
			}
		}
		sh.mu.Unlock()
	}
}
