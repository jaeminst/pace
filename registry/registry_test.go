package registry

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"testing"
	"time"
	"unsafe"

	"github.com/jaeminst/pace/bucket"
)

// testConfig is a registry that persists nothing and tells nobody anything.
// Every field is a plain value or a function, which is the property that lets
// this package be tested without the owner it serves: there is no Limiter here,
// no HTTP, no store, and no import of the parent.
func testConfig() Spec {
	return Spec{
		Shards:        DefaultShards,
		IdleExpiry:    time.Minute,
		Now:           time.Now,
		QuotaFor:      func(string) bucket.Quota { return bucket.Quota{Rate: 1, Burst: 1} },
		Persists:      func() bool { return false },
		Load:          func(context.Context, string) (Snapshot, bool) { return Snapshot{}, false },
		Save:          func(context.Context, Snapshot) error { return nil },
		Flush:         func([]Snapshot) {},
		Observes:      func() bool { return false },
		OnEvict:       func(Eviction) {},
		OnGetOrCreate: func() {},
		AfterSweep:    func() {},
	}
}

// blockingFlush holds every batch until release is closed, and announces when a
// write has actually begun. It is what pins a sweep inside its persistence step
// so a test can observe the shard locks it is or is not holding.
type blockingFlush struct {
	release chan struct{}
	inFlush chan struct{}
	once    bool
}

func newBlockingFlush() *blockingFlush {
	return &blockingFlush{release: make(chan struct{}), inFlush: make(chan struct{})}
}

func (f *blockingFlush) flush([]Snapshot) {
	if !f.once {
		f.once = true
		close(f.inFlush)
	}
	<-f.release
}

// persistingRegistry is a registry whose sweep blocks in the flush.
func persistingRegistry() (*Registry, *blockingFlush) {
	f := newBlockingFlush()
	cfg := testConfig()
	cfg.Persists = func() bool { return true }
	cfg.Flush = f.flush
	return New(cfg), f
}

// expire backdates a key well past any reasonable IdleExpiry.
//
// Backdating rather than setting IdleExpiry to zero: Windows' wall clock is
// coarse enough that a key created and swept within one tick compares equal to
// the cutoff, and the sweep then finds nothing to do.
func expire(u *Entry) { u.Touch(time.Now().Add(-time.Hour)) }

func (r *Registry) has(key string) bool {
	_, ok := r.Lookup(key)
	return ok
}

// sameShardAs returns an ID that hashes to the same shard as want, so a test can
// guarantee a collision rather than rely on 1-in-256 odds.
func (r *Registry) sameShardAs(t *testing.T, want string) string {
	t.Helper()
	target := r.shardFor(want)
	for i := range 1_000_000 {
		id := fmt.Sprintf("probe-%d", i)
		if id != want && r.shardFor(id) == target {
			return id
		}
	}
	t.Fatalf("no ID found sharing a shard with %q", want)
	return ""
}

// TestSweepReleasesShardLockDuringStoreIO is the deterministic form of the
// property the three-phase sweep exists for: it must not hold a shard lock
// while the owner is talking to a store.
//
// It is a test rather than a benchmark because timing cannot show this. A sweep
// locks one shard at a time, so only ~1/256 of requests collide with it, and
// the tail that results is indistinguishable from ordinary scheduler noise — a
// benchmark reports the same millisecond spikes with no store configured at
// all. Blocking the flush explicitly removes the guesswork: either the lookup
// completes while the write is stuck, or it does not.
func TestSweepReleasesShardLockDuringStoreIO(t *testing.T) {
	r, f := persistingRegistry()

	const victim = "victim"
	expire(r.GetOrCreate(context.Background(), victim))
	live := r.sameShardAs(t, victim)

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		r.Sweep()
	}()

	select {
	case <-f.inFlush:
	case <-time.After(10 * time.Second):
		t.Fatal("sweep never reached the flush")
	}

	// The shard holding victim must still be usable. Under a sweep that
	// persisted while holding the write lock, this blocks until release.
	looked := make(chan struct{})
	go func() {
		defer close(looked)
		r.GetOrCreate(context.Background(), live)
	}()

	select {
	case <-looked:
	case <-time.After(5 * time.Second):
		close(f.release)
		<-swept
		t.Fatal("GetOrCreate blocked on a shard lock held across store I/O")
	}

	close(f.release)
	<-swept

	if r.has(victim) {
		t.Error("expired key survived the sweep")
	}
	if !r.has(live) {
		t.Error("a key created after the snapshot was deleted by the sweep")
	}
}

// TestSweepKeepsKeysTouchedMidSweep covers the phase-3 guard: a key who makes
// a request between the snapshot and the delete keeps their live bucket. Losing
// that check would evict a key who is actively sending traffic.
func TestSweepKeepsKeysTouchedMidSweep(t *testing.T) {
	r, f := persistingRegistry()

	const busy = "busy"
	u := r.GetOrCreate(context.Background(), busy)
	expire(u)

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		r.Sweep()
	}()

	select {
	case <-f.inFlush:
	case <-time.After(10 * time.Second):
		t.Fatal("sweep never reached the flush")
	}

	// A request lands after the snapshot was taken but before the delete.
	u.Touch(time.Now())

	close(f.release)
	<-swept

	cur, ok := r.Lookup(busy)
	if !ok {
		t.Fatal("a key active during the sweep was evicted")
	}
	if cur != u {
		t.Error("the surviving entry is not the original key")
	}
}

// TestSweepEvictsInPlaceWithoutPersistence covers the other branch of Sweep:
// with nothing to persist there is no I/O to move out of the lock, so the
// snapshot pass is skipped entirely.
func TestSweepEvictsInPlaceWithoutPersistence(t *testing.T) {
	var swept int
	cfg := testConfig()
	cfg.AfterSweep = func() { swept++ }
	r := New(cfg)

	expire(r.GetOrCreate(context.Background(), "idle"))
	r.GetOrCreate(context.Background(), "active")

	r.Sweep()

	if r.has("idle") {
		t.Error("the idle key survived")
	}
	if !r.has("active") {
		t.Error("the active key was evicted")
	}
	if got := r.Evictions(); got != 1 {
		t.Errorf("Evictions() = %d, want 1", got)
	}
	if swept != 1 {
		t.Errorf("AfterSweep fired %d times, want 1", swept)
	}
}

// TestNobodyListeningStillCounts pins the bulk-count path. When no observer is
// configured the eviction paths skip building a per-key list — 57KB per sweep
// of 2,000 keys on the one path whose entire point is that it does almost
// nothing — but the tally must come out the same either way.
func TestNobodyListeningStillCounts(t *testing.T) {
	for _, observing := range []bool{false, true} {
		t.Run(fmt.Sprintf("observes=%v", observing), func(t *testing.T) {
			var reported int
			cfg := testConfig()
			cfg.Observes = func() bool { return observing }
			cfg.OnEvict = func(Eviction) { reported++ }
			r := New(cfg)

			const keys = 5
			for i := range keys {
				expire(r.GetOrCreate(context.Background(), fmt.Sprintf("u%d", i)))
			}
			r.Sweep()

			if got := r.Evictions(); got != keys {
				t.Errorf("Evictions() = %d, want %d", got, keys)
			}
			want := 0
			if observing {
				want = keys
			}
			if reported != want {
				t.Errorf("OnEvict fired %d times, want %d", reported, want)
			}
		})
	}
}

// TestDropAllEmptiesTheShards pins that shutdown does not merely stop reporting
// keys but actually discards them: a population count taken afterwards must
// not describe keys who no longer exist.
func TestDropAllEmptiesTheShards(t *testing.T) {
	r := New(testConfig())
	for i := range 10 {
		r.GetOrCreate(context.Background(), fmt.Sprintf("u%d", i))
	}
	if got := r.Keys(); got != 10 {
		t.Fatalf("Keys() = %d, want 10", got)
	}

	r.DropAll()

	if got := r.Keys(); got != 0 {
		t.Errorf("Keys() = %d after DropAll, want 0", got)
	}
	if got := r.Evictions(); got != 10 {
		t.Errorf("Evictions() = %d, want 10", got)
	}
}

// TestEvictReportsTheSaveFailure pins the ordering the owner's contract depends
// on: state is written before the eviction is announced, so a failed save is
// returned as an error rather than reported as a clean eviction.
func TestEvictReportsTheSaveFailure(t *testing.T) {
	boom := errors.New("store is down")
	var reported int
	cfg := testConfig()
	cfg.Persists = func() bool { return true }
	cfg.Save = func(context.Context, Snapshot) error { return boom }
	cfg.Observes = func() bool { return true }
	cfg.OnEvict = func(Eviction) { reported++ }
	r := New(cfg)

	r.GetOrCreate(context.Background(), "alice")
	present, err := r.Evict(context.Background(), "alice")

	if !present {
		t.Error("Evict reported the key was absent")
	}
	if err == nil {
		t.Fatal("Evict returned nil after the save failed")
	}
	if reported != 0 {
		t.Error("a failed save was still announced as a clean eviction")
	}
	if r.has("alice") {
		t.Error("the key is still in memory; the map surgery must not depend on the save")
	}
}

func TestEvictAnAbsentKeyIsNotAnError(t *testing.T) {
	r := New(testConfig())
	present, err := r.Evict(context.Background(), "nobody")
	if present || err != nil {
		t.Errorf("Evict(absent) = (%v, %v), want (false, nil)", present, err)
	}
}

// TestReloadReadsTheClockPerKey pins the fix for a bug that handed out free
// tokens. SetQuotaAt stamps the bucket's last-updated instant, so an instant
// captured once before the whole walk rewinds every bucket touched after it —
// and a rewound interval is refilled twice.
func TestReloadReadsTheClockPerKey(t *testing.T) {
	var reads int
	cfg := testConfig()
	cfg.Now = func() time.Time {
		reads++
		return time.Now()
	}
	r := New(cfg)

	const keys = 8
	for i := range keys {
		r.GetOrCreate(context.Background(), fmt.Sprintf("u%d", i))
	}
	reads = 0
	r.Reload()

	if reads != keys {
		t.Errorf("Reload read the clock %d times for %d keys; it must read it once per key, "+
			"or an instant captured before a slow QuotaFor rewinds the buckets walked after it", reads, keys)
	}
}

// TestShardIsCacheLinePadded checks the padding arithmetic rather than trusting
// the comment next to it. If a field is added to shard without adjusting the
// pad, two shards' mutexes start sharing a cache line and unrelated keys
// contend for no reason.
func TestShardIsCacheLinePadded(t *testing.T) {
	const cacheLine = 64
	if got := unsafe.Sizeof(shard{}); got != cacheLine {
		t.Errorf("unsafe.Sizeof(shard{}) = %d, want %d", got, cacheLine)
	}
}

// TestShardIndexMatchesFNV1a pins the hash to the standard algorithm, so the
// inlined loop cannot drift from it unnoticed.
func TestShardIndexMatchesFNV1a(t *testing.T) {
	for _, id := range []string{"", "a", "alice", "user-12345", "사용자-한글", "\x00\xff"} {
		h := fnv.New32a()
		_, _ = h.Write([]byte(id))
		want := h.Sum32() & (DefaultShards - 1)
		if got := shardIndex(id, DefaultShards-1); got != want {
			t.Errorf("shardIndex(%q) = %d, want %d", id, got, want)
		}
	}
}

// TestReloadKeyOnlyTouchesThatKey: the O(1) form must not become a walk, and
// must not create anybody.
func TestReloadKeyOnlyTouchesThatKey(t *testing.T) {
	burst := map[string]int{"alice": 1, "bob": 1}
	cfg := testConfig()
	cfg.Now = func() time.Time { return time.Unix(0, 0) }
	cfg.QuotaFor = func(key string) bucket.Quota { return bucket.Quota{Rate: 1, Burst: burst[key]} }

	r := New(cfg)
	t.Cleanup(func() { r.DropAll() })
	r.GetOrCreate(context.Background(), "alice")
	r.GetOrCreate(context.Background(), "bob")

	burst["alice"], burst["bob"] = 20, 20

	if !r.ReloadKey("alice") {
		t.Fatal("ReloadKey(alice) = false for a key in memory")
	}
	alice, _ := r.Lookup("alice")
	if got := alice.Bucket().Quota().Burst; got != 20 {
		t.Errorf("alice's burst = %d after ReloadKey, want 20", got)
	}
	bob, _ := r.Lookup("bob")
	if got := bob.Bucket().Quota().Burst; got != 1 {
		t.Errorf("bob's burst = %d, want 1: ReloadKey touches one key, it does not walk", got)
	}
}

// TestReloadKeyDoesNotCreateAnything is the sibling of the guard on Reload: a
// reload is not a reason for a key to start existing.
func TestReloadKeyDoesNotCreateAnything(t *testing.T) {
	r := New(testConfig())
	t.Cleanup(func() { r.DropAll() })

	if r.ReloadKey("stranger") {
		t.Error("ReloadKey reported true for a key that was never seen")
	}
	if _, ok := r.Lookup("stranger"); ok {
		t.Error("ReloadKey created the key it was asked about")
	}
}
