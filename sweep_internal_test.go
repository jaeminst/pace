package pace

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"testing"
	"time"
	"unsafe"

	"github.com/jaeminst/pace/internal/store"
)

// blockingStore holds every write until release is closed. Loads return
// immediately, so a test can pin a sweep inside its persistence step without
// also stalling the user lookups it wants to observe.
type blockingStore struct {
	release  chan struct{}
	inSave   chan struct{} // closed once a write has actually begun
	saveOnce bool
}

func newBlockingStore() *blockingStore {
	return &blockingStore{release: make(chan struct{}), inSave: make(chan struct{})}
}

func (s *blockingStore) enter() {
	if !s.saveOnce {
		s.saveOnce = true
		close(s.inSave)
	}
	<-s.release
}

func (s *blockingStore) Save(string, float64, int64) error {
	s.enter()
	return nil
}

func (s *blockingStore) SaveBatch([]store.UserState) error {
	s.enter()
	return nil
}

func (s *blockingStore) Load(string) (store.SavedState, bool, error) {
	return store.SavedState{}, false, nil
}

func (s *blockingStore) Close() error { return nil }

// expire backdates a user well past any reasonable IdleExpiry.
func expire(u *user) {
	u.lastUsed.Store(time.Now().Add(-time.Hour).UnixNano())
}

// sameShardAs returns an ID that hashes to the same shard as want, so the test
// can guarantee a collision rather than rely on 1-in-256 odds.
func (l *Limiter) sameShardAs(t *testing.T, want string) string {
	t.Helper()
	target := l.shardFor(want)
	for i := range 1_000_000 {
		id := fmt.Sprintf("probe-%d", i)
		if id != want && l.shardFor(id) == target {
			return id
		}
	}
	t.Fatalf("no ID found sharing a shard with %q", want)
	return ""
}

// TestSweepReleasesShardLockDuringStoreIO is the deterministic form of the
// property this design exists for: the sweep must not hold a shard lock while
// it talks to the store.
//
// It is written as a test rather than a benchmark because timing cannot show
// this. A sweep locks one shard at a time, so only ~1/256 of requests collide
// with it, and the tail that results is indistinguishable from ordinary
// scheduler noise — a benchmark reports the same millisecond spikes with no
// store configured at all. Blocking the store explicitly removes the guesswork:
// either the lookup completes while the store is stuck, or it does not.
func TestSweepReleasesShardLockDuringStoreIO(t *testing.T) {
	st := newBlockingStore()
	l := &Limiter{
		cfg: Config{
			BaseURL:    "http://example.invalid",
			Rate:       PerMinute(60),
			Burst:      1,
			IdleExpiry: time.Minute,
			Clock:      stdClock{},
			Logger:     slog.New(slog.DiscardHandler),
		},
		store: st,
	}
	l.shards = newShards(numShards)
	l.shardMask = uint32(len(l.shards) - 1)

	const victim = "victim"
	// Backdate rather than rely on IdleExpiry: 0. Windows' wall clock is coarse
	// enough that a user created and swept within one tick compares equal to
	// the cutoff, and the sweep finds nothing to do.
	expire(l.userFor(victim))
	live := l.sameShardAs(t, victim)

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		l.sweep()
	}()

	// Wait until the sweep is genuinely stuck inside the store.
	select {
	case <-st.inSave:
	case <-time.After(10 * time.Second):
		t.Fatal("sweep never reached the store")
	}

	// The shard holding victim must still be usable. Under a sweep that saved
	// while holding the write lock, this blocks until the store is released.
	looked := make(chan struct{})
	go func() {
		defer close(looked)
		l.userFor(live)
	}()

	select {
	case <-looked:
	case <-time.After(5 * time.Second):
		close(st.release)
		<-swept
		t.Fatal("userFor blocked on a shard lock held across store I/O")
	}

	close(st.release)
	<-swept

	// The victim was expired and untouched, so the sweep should have removed
	// it; live was created after the snapshot and must survive.
	sh := l.shardFor(victim)
	sh.mu.RLock()
	_, victimPresent := sh.users[victim]
	_, livePresent := sh.users[live]
	sh.mu.RUnlock()

	if victimPresent {
		t.Error("expired user survived the sweep")
	}
	if !livePresent {
		t.Error("a user created after the snapshot was deleted by the sweep")
	}
}

// TestSweepKeepsUsersTouchedMidSweep covers the phase-3 guard: a user who makes
// a request between the snapshot and the delete keeps their live bucket. Losing
// that check would evict a user who is actively sending traffic.
func TestSweepKeepsUsersTouchedMidSweep(t *testing.T) {
	st := newBlockingStore()
	l := &Limiter{
		cfg: Config{
			BaseURL:    "http://example.invalid",
			Rate:       PerMinute(60),
			Burst:      1,
			IdleExpiry: time.Minute,
			Clock:      stdClock{},
			Logger:     slog.New(slog.DiscardHandler),
		},
		store: st,
	}
	l.shards = newShards(numShards)
	l.shardMask = uint32(len(l.shards) - 1)

	const busy = "busy"
	u := l.userFor(busy)
	expire(u)

	swept := make(chan struct{})
	go func() {
		defer close(swept)
		l.sweep()
	}()

	select {
	case <-st.inSave:
	case <-time.After(10 * time.Second):
		t.Fatal("sweep never reached the store")
	}

	// A request lands after the snapshot was taken but before the delete.
	u.lastUsed.Store(l.cfg.Clock.Now().UnixNano())

	close(st.release)
	<-swept

	sh := l.shardFor(busy)
	sh.mu.RLock()
	cur, ok := sh.users[busy]
	sh.mu.RUnlock()
	if !ok {
		t.Fatal("a user active during the sweep was evicted")
	}
	if cur != u {
		t.Error("the surviving entry is not the original user")
	}
}

// TestShardIsCacheLinePadded checks the padding arithmetic rather than trusting
// the comment next to it. If a field is added to shard without adjusting the
// pad, two shards' mutexes start sharing a cache line and unrelated users
// contend for no reason.
func TestShardIsCacheLinePadded(t *testing.T) {
	const cacheLine = 64
	if got := unsafe.Sizeof(shard{}); got != cacheLine {
		t.Errorf("unsafe.Sizeof(shard{}) = %d, want %d", got, cacheLine)
	}
}

func TestRoundUpPowerOfTwo(t *testing.T) {
	tests := []struct{ in, want int }{
		{0, numShards},
		{-1, numShards},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{256, 256},
		{257, 512},
	}
	for _, tt := range tests {
		if got := roundUpPowerOfTwo(tt.in); got != tt.want {
			t.Errorf("roundUpPowerOfTwo(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// TestShardIndexMatchesFNV1a pins the hash to the standard algorithm, so the
// inlined loop cannot drift from it unnoticed.
func TestShardIndexMatchesFNV1a(t *testing.T) {
	for _, id := range []string{"", "a", "alice", "user-12345", "사용자-한글", "\x00\xff"} {
		h := fnv.New32a()
		_, _ = h.Write([]byte(id))
		want := h.Sum32() & (numShards - 1)
		if got := shardIndex(id, numShards-1); got != want {
			t.Errorf("shardIndex(%q) = %d, want %d", id, got, want)
		}
	}
}
