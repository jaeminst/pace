// Package registry owns the population of rate-limited users: the sharded map
// they live in, the token bucket each one holds, when their state is read and
// written, and when they are evicted.
//
// It knows nothing about HTTP, quotas as a type, stores as an interface, or
// observers. Everything it needs from its owner is a plain value or a function
// field on [Config], so it never imports the parent.
//
// The division of labour with the owner is worth stating, because the eviction
// paths are where it is least obvious: this package decides *which* users are
// evicted and *when*, and holds the shard locks while doing it. The owner
// decides what persisting one means. Nothing here ever holds a lock across a
// call back into the owner, which is the invariant the three-phase sweep and
// the out-of-lock notifications exist to preserve.
//
// It is public because it is worth reading, not because a caller is expected to
// build one. [Config] is a vtable rather than a set of options — every field is
// required and [New] panics on one it cannot work with — and two of its fields
// are test seams the Limiter wires to its own hooks.
package registry

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/bucket"
)

// DefaultShards is the shard count used when the owner asks for none. It must
// be a power of two for the bitmask fast-path in shardIndex.
const DefaultShards = 256

// Snapshot is one user's state at an instant, taken under a shard lock so that
// persisting it can happen without holding one.
type Snapshot struct {
	UserID   string
	Tokens   float64
	LastUsed time.Time
}

// Reason says why a user was evicted.
type Reason int

const (
	// Idle means the user passed IdleExpiry without a request.
	Idle Reason = iota
	// Explicit means the owner asked for this user by name.
	Explicit
	// Shutdown means every user was discarded at once.
	Shutdown
)

// Eviction describes a user that has just been removed from memory. It is
// always reported outside the shard lock.
type Eviction struct {
	UserID   string
	Reason   Reason
	Tokens   float64
	LastUsed time.Time
}

// Config is everything the registry needs from its owner. Every field is
// required: the registry is constructed at exactly one call site, so nothing
// here is defaulted or nil-checked — except SaveBatch-style optionality, which
// is resolved entirely on the owner's side.
//
// The values are snapshotted at construction, which is safe only because the
// owner's config is immutable after its own defaulting runs. Persists is a
// function rather than a bool for exactly that reason inverted: the owner's
// store can be replaced after construction, so whether state is persisted has
// to be asked rather than remembered.
type Config struct {
	// Shards is the map's shard count, already rounded to a power of two.
	Shards int

	// IdleExpiry is how long a user may go untouched before Sweep drops them.
	IdleExpiry time.Duration

	// Now is the owner's clock, so every timestamp here comes from the source
	// the rest of the package reports against.
	Now func() time.Time

	// QuotaFor resolves a user's rate and burst. It is the owner's caller-
	// supplied hook with defaulting already applied, reduced to the two numbers
	// a bucket is built from — the type it comes wrapped in stays with the
	// owner, where it is exported and documented.
	//
	// It runs caller code, so the registry never calls it holding a lock.
	QuotaFor func(userID string) (rate float64, burst int)

	// Persists reports whether user state should be read and written at all.
	// Asked rather than snapshotted: the owner's store may be swapped after
	// construction, and a shared quota turns the local bucket into a shadow
	// that must never be persisted.
	Persists func() bool

	// Load reads one user's saved state. The owner is responsible for the
	// timeout and for deciding that an error means "no saved state".
	Load func(ctx context.Context, userID string) (Snapshot, bool)

	// Save persists one user synchronously and reports whether it worked. It
	// backs the explicit Evict, whose contract is that state is written by the
	// time it returns.
	Save func(ctx context.Context, s Snapshot) error

	// Flush persists a batch, holding nothing, on a context of the owner's
	// choosing. It is used where a failure is logged rather than returned: the
	// idle sweep and the final flush. Whether the owner's store can take a
	// batch in one round-trip is the owner's business, decided per call.
	Flush func(snaps []Snapshot)

	// Observes reports whether anybody is listening for evictions. When nobody
	// is, the eviction paths skip building the per-user list — which on a sweep
	// of a large population is the difference between one allocation per victim
	// and none.
	Observes func() bool

	// OnEvict reports one eviction. It is never called with a shard lock held:
	// an owner's hook that calls back in would otherwise deadlock against the
	// very lock the caller took.
	OnEvict func(Eviction)

	// OnGetOrCreate and AfterSweep are test seams. Pass method values that read
	// the hook at call time, not the hooks themselves: a test may install one
	// after the registry has started.
	OnGetOrCreate func()
	AfterSweep    func()
}

// User is one rate-limited identity's in-memory state.
type User struct {
	bucket *bucket.Bucket
	// lastUsed is unix nanoseconds, updated atomically so that reading the
	// population's idleness never needs a lock.
	lastUsed atomic.Int64
}

// Bucket returns the token bucket enforcing this user's quota.
//
// It is the whole of User's surface on purpose. The bucket is already a shared
// internal type, so handing it out costs no encapsulation, where mirroring its
// eight methods here would put the registry in the business of rate limiting —
// which is the owner's.
func (u *User) Bucket() *bucket.Bucket { return u.bucket }

// Touch records that this user made a request at now.
func (u *User) Touch(now time.Time) { u.lastUsed.Store(now.UnixNano()) }

// shard is padded to a cache line so that two shards' mutexes never share one.
// Without it, traffic for unrelated users on adjacent shards would contend in
// the cache even though the locks themselves never collide.
type shard struct {
	mu    sync.RWMutex     // 24 B
	users map[string]*User //  8 B
	// live mirrors len(users) so that counting the population does not mean
	// acquiring every shard lock.
	live atomic.Int64 // 8 B
	_    [24]byte     // pad to 64 B
}

// Registry is the sharded user population.
type Registry struct {
	cfg       Config
	shards    []shard
	shardMask uint32
	evictions atomic.Int64
}

// New builds a registry over cfg.Shards shards.
//
// It panics on a Config it cannot work with, rather than later and somewhere
// else. Shards must be a positive power of two — shardIndex masks rather than
// divides, so any other count leaves part of the map unreachable — and every
// function field is required, because this is a vtable rather than a set of
// options: the zero value of any of them is a nil call on the first request.
func New(cfg Config) *Registry {
	switch {
	case cfg.Shards <= 0 || cfg.Shards&(cfg.Shards-1) != 0:
		panic(fmt.Sprintf("registry: Shards = %d, want a positive power of two", cfg.Shards))
	case cfg.Now == nil || cfg.QuotaFor == nil || cfg.Persists == nil:
		panic("registry: Now, QuotaFor and Persists are required")
	case cfg.Load == nil || cfg.Save == nil || cfg.Flush == nil:
		panic("registry: Load, Save and Flush are required")
	case cfg.Observes == nil || cfg.OnEvict == nil:
		panic("registry: Observes and OnEvict are required")
	case cfg.OnGetOrCreate == nil || cfg.AfterSweep == nil:
		panic("registry: OnGetOrCreate and AfterSweep are required; pass a no-op if you have no hook")
	}
	shards := make([]shard, cfg.Shards)
	for i := range shards {
		shards[i].users = make(map[string]*User)
	}
	return &Registry{
		cfg:    cfg,
		shards: shards,
		//nolint:gosec // the shard count is a power of two bounded by the owner
		shardMask: uint32(len(shards) - 1),
	}
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

func (r *Registry) shardFor(userID string) *shard {
	return &r.shards[shardIndex(userID, r.shardMask)]
}

// Lookup returns a user's state if they currently have any, without creating it.
func (r *Registry) Lookup(userID string) (*User, bool) {
	sh := r.shardFor(userID)
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	return u, ok
}

// GetOrCreate returns userID's state, restoring it from the owner's store on
// first sight.
func (r *Registry) GetOrCreate(ctx context.Context, userID string) *User {
	sh := r.shardFor(userID)
	// hot path: existing user needs only a read lock
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if ok {
		return u
	}
	// cold path: new user
	r.cfg.OnGetOrCreate()
	// Load before taking the write lock. A store may be backed by Redis or
	// Postgres, and holding a shard closed across a network round-trip blocks
	// every user that hashes to it. Two concurrent first-requests for the same
	// user may both load, but the read is idempotent and the loser's result is
	// discarded — strictly better than serialising I/O under a lock.
	var (
		snap  Snapshot
		found bool
	)
	if r.cfg.Persists() {
		snap, found = r.cfg.Load(ctx, userID)
	}
	// Resolved here for the same reason the load is: QuotaFor is the owner's
	// caller's code, and no caller-supplied function may run with a shard held
	// shut.
	rate, burst := r.cfg.QuotaFor(userID)

	sh.mu.Lock()
	if u, ok = sh.users[userID]; ok {
		sh.mu.Unlock()
		return u
	}
	u = r.newUser(rate, burst, snap, found)
	sh.users[userID] = u
	sh.live.Add(1)
	sh.mu.Unlock()
	return u
}

func (r *Registry) newUser(rate float64, burst int, snap Snapshot, found bool) *User {
	u := &User{}
	now := r.cfg.Now()
	if found {
		u.bucket = bucket.RestoreBucket(rate, burst, snap.Tokens, snap.LastUsed, now)
		u.lastUsed.Store(snap.LastUsed.UnixNano())
	} else {
		u.bucket = bucket.NewBucket(rate, burst)
	}
	if u.lastUsed.Load() == 0 {
		u.lastUsed.Store(now.UnixNano())
	}
	return u
}

// Users reports how many users currently hold state.
//
// It is sampled per shard rather than under one lock, so it is accurate to the
// moment each shard was read rather than to a single instant — which is the
// right trade for a number whose purpose is a gauge on a dashboard.
func (r *Registry) Users() int64 {
	var n int64
	for i := range r.shards {
		n += r.shards[i].live.Load()
	}
	return n
}

// Evictions reports how many users have been dropped, for any reason, since the
// registry was created.
func (r *Registry) Evictions() int64 { return r.evictions.Load() }

// snapshot reads a user's two persisted numbers at one instant.
//
// It is one method because the pair has to be read together: Tokens is how many
// were left and LastUsed is when, and a bucket restored from a mismatched pair
// was never real. Six call sites used to spell it out, and Evict spelled it out
// twice for the same user — where lastUsed is an atomic load, so the two copies
// could legally disagree.
func (u *User) snapshot(userID string, now time.Time) Snapshot {
	return Snapshot{
		UserID:   userID,
		Tokens:   u.bucket.TokensAt(now),
		LastUsed: time.Unix(0, u.lastUsed.Load()),
	}
}

// eviction is the same three values with the reason attached. Eviction is what
// an observer is told; Snapshot is what a store is given.
func (s Snapshot) eviction(reason Reason) Eviction {
	return Eviction{UserID: s.UserID, Reason: reason, Tokens: s.Tokens, LastUsed: s.LastUsed}
}

// SnapshotAll copies every user's state, taking each shard's read lock in turn
// and holding none by the time it returns.
func (r *Registry) SnapshotAll() []Snapshot {
	now := r.cfg.Now()
	var all []Snapshot
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.RLock()
		for id, u := range sh.users {
			all = append(all, u.snapshot(id, now))
		}
		sh.mu.RUnlock()
	}
	return all
}

// Reload re-resolves every live user's quota through Config.QuotaFor and applies
// it to their bucket.
//
// The clock is read per user rather than once for the whole walk. SetQuotaAt
// stamps the bucket's last-updated instant, so an instant captured before a
// large population's worth of QuotaFor calls would rewind every bucket touched
// after it — and a rewound interval is refilled a second time, handing free
// tokens to anyone who made a request while the reload was in progress.
//
// QuotaFor runs outside the shard lock, per the invariant this package keeps.
func (r *Registry) Reload() {
	type entry struct {
		userID string
		u      *User
	}
	var batch []entry
	for i := range r.shards {
		sh := &r.shards[i]
		sh.mu.RLock()
		batch = batch[:0]
		for id, u := range sh.users {
			batch = append(batch, entry{userID: id, u: u})
		}
		sh.mu.RUnlock()

		for _, e := range batch {
			rate, burst := r.cfg.QuotaFor(e.userID)
			e.u.bucket.SetQuotaAt(r.cfg.Now(), rate, burst)
		}
	}
}
