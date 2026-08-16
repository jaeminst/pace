package pace

import (
	"context"
	"sync"
	"sync/atomic"

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
