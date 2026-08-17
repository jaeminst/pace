package limiter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/limit"
	pace "github.com/jaeminst/pace/limiter"
)

func reserveLimiter(t *testing.T, burst int, opts ...func(*pace.Config)) *pace.Limiter {
	t.Helper()
	cfg := pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    limit.PerSecond(1),
		Burst:   burst,
		Clock:   newFakeClock(),
	}
	for _, o := range opts {
		o(&cfg)
	}
	lim, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	return lim
}

// TestReserveWithTokenAvailableHasNoDelay: the common case costs a token and
// reports that the caller may act immediately.
func TestReserveWithTokenAvailableHasNoDelay(t *testing.T) {
	lim := reserveLimiter(t, 2)
	alice := lim.Client("alice")

	r := alice.Reserve(context.Background())
	if !r.OK() {
		t.Fatal("Reserve failed with a full bucket")
	}
	if r.Delay() != 0 {
		t.Errorf("Delay = %v with a token in hand, want 0", r.Delay())
	}
	if got := tokensOf(alice); got != 1 {
		t.Errorf("tokens = %v after one reservation against a burst of 2, want 1", got)
	}
}

// TestReserveReportsTheWaitInsteadOfBlocking is the gap Reserve fills. Allow
// would refuse and tell the caller nothing; Wait would block for a duration the
// caller never gets to see before committing to it.
func TestReserveReportsTheWaitInsteadOfBlocking(t *testing.T) {
	lim := reserveLimiter(t, 1)
	alice := lim.Client("alice")

	first := alice.Reserve(context.Background())
	if !first.OK() || first.Delay() != 0 {
		t.Fatalf("first reservation = (ok %v, delay %v), want (true, 0)", first.OK(), first.Delay())
	}

	// The burst is spent, and at one token per second the next is a second out.
	second := alice.Reserve(context.Background())
	if !second.OK() {
		t.Fatal("Reserve refused a request the bucket can eventually satisfy")
	}
	if got := second.Delay(); got < 900*time.Millisecond || got > 1100*time.Millisecond {
		t.Errorf("Delay = %v, want about 1s at one token per second", got)
	}
}

// TestReserveCancelReturnsTheToken: without this, a caller who decides against
// the request has still charged the user for it.
func TestReserveCancelReturnsTheToken(t *testing.T) {
	lim := reserveLimiter(t, 5)
	alice := lim.Client("alice")
	alice.Reserve(context.Background()).Cancel() // materialise the bucket so Tokens has an answer

	before := tokensOf(alice)
	r := alice.Reserve(context.Background())
	if got := tokensOf(alice); got != before-1 {
		t.Fatalf("tokens = %v after Reserve, want %v: the token is taken immediately", got, before-1)
	}

	r.Cancel()
	if got := tokensOf(alice); got != before {
		t.Errorf("tokens = %v after Cancel, want %v", got, before)
	}
}

// TestReserveCancelIsIdempotent: a second Cancel must not mint a token. The
// natural shape of caller code — a deferred Cancel plus an explicit one on the
// early-return path — makes a double call easy to write by accident.
func TestReserveCancelIsIdempotent(t *testing.T) {
	lim := reserveLimiter(t, 5)
	alice := lim.Client("alice")
	alice.Reserve(context.Background()).Cancel() // materialise the bucket

	before := tokensOf(alice)
	r := alice.Reserve(context.Background())
	r.Cancel()
	r.Cancel()
	r.Cancel()

	if got := tokensOf(alice); got != before {
		t.Errorf("tokens = %v after three Cancels, want %v", got, before)
	}
}

// TestReserveSucceedsAtTheSmallestBurst pins down why OK is documented as
// false only during shutdown: Config defaults a non-positive Burst to 1, and a
// reservation is always for one token, so rate.Limiter's "can never be
// satisfied" refusal is unreachable through this API.
func TestReserveSucceedsAtTheSmallestBurst(t *testing.T) {
	lim := reserveLimiter(t, 0) // defaulted to 1
	r := lim.Client("alice").Reserve(context.Background())
	if !r.OK() {
		t.Error("Reserve failed at the smallest burst pace allows")
	}
}

// TestReserveAfterCloseIsNotOK: Reserve goes through the same shutdown barrier
// as every other entry point that can touch the store.
func TestReserveAfterCloseIsNotOK(t *testing.T) {
	lim := reserveLimiter(t, 5)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	r := lim.Client("alice").Reserve(context.Background())
	if r.OK() {
		t.Error("Reserve succeeded after Close")
	}
	r.Cancel() // must not panic on a reservation that holds nothing
}

// TestReserveIsCountedAndObserved: a reservation is a request as far as the
// metrics are concerned, and a delayed one is a throttle.
func TestReserveIsCountedAndObserved(t *testing.T) {
	var infos []pace.ThrottleInfo
	var mu sync.Mutex

	lim := reserveLimiter(t, 1, func(c *pace.Config) {
		c.Observer = &pace.Observer{
			Throttled: func(_ context.Context, i pace.ThrottleInfo) {
				mu.Lock()
				defer mu.Unlock()
				infos = append(infos, i)
			},
		}
	})
	alice := lim.Client("alice")

	alice.Reserve(context.Background()) // immediate: no throttle
	alice.Reserve(context.Background()) // delayed: throttled

	if got := lim.Stats().Requests; got != 2 {
		t.Errorf("Requests = %d, want 2: a reservation is a request", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(infos) != 1 {
		t.Fatalf("Throttled fired %d times, want 1 (only the delayed reservation)", len(infos))
	}
	if infos[0].UserID != "alice" || infos[0].Delay <= 0 {
		t.Errorf("ThrottleInfo = %+v, want alice with a positive delay", infos[0])
	}
	if infos[0].Burst != 1 {
		t.Errorf("ThrottleInfo.Burst = %d, want 1", infos[0].Burst)
	}
}

// TestReserveUsesTheUsersOwnQuota: like every other report, a reservation is
// measured against that user's quota rather than the Limiter default.
func TestReserveUsesTheUsersOwnQuota(t *testing.T) {
	lim := reserveLimiter(t, 1, func(c *pace.Config) {
		c.QuotaFor = func(userID string) limit.Quota {
			if userID == "paid" {
				return limit.Quota{Burst: 10}
			}
			return limit.Quota{}
		}
	})

	paid := lim.Client("paid")
	for i := range 10 {
		if r := paid.Reserve(context.Background()); !r.OK() || r.Delay() != 0 {
			t.Fatalf("reservation %d = (ok %v, delay %v), want immediate: burst is 10", i, r.OK(), r.Delay())
		}
	}
	if r := lim.Client("free").Reserve(context.Background()); r.Delay() != 0 {
		t.Errorf("the free user's first reservation was delayed by %v, want 0", r.Delay())
	}
	if r := lim.Client("free").Reserve(context.Background()); r.Delay() == 0 {
		t.Error("the free user's second reservation was immediate, want a delay: burst is 1")
	}
}

// TestAllowAndReserveHonourTheContext is why both gained one. Neither waits for
// a token, but both do bounded I/O — a store load on a user's cold path, and a
// backend round-trip when a shared quota is configured — and until now there
// was no way to cancel either. They were the only two entry points in the
// package that did I/O without a context, on the load-shedding path an inbound
// handler calls with a request context already in hand.
func TestAllowAndReserveHonourTheContext(t *testing.T) {
	st := &blockingLoadStore{released: make(chan struct{})}
	lim, err := pace.New(pace.Config{
		BaseURL:      "http://example.invalid",
		Rate:         limit.PerMinute(600),
		Burst:        10,
		Store:        st,
		StoreTimeout: time.Hour, // so only ctx can end the wait
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	defer close(st.released)

	for _, tt := range []struct {
		name string
		call func(ctx context.Context, c *pace.Client)
	}{
		{"Allow", func(ctx context.Context, c *pace.Client) { c.Allow(ctx) }},
		{"Reserve", func(ctx context.Context, c *pace.Client) { c.Reserve(ctx) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			done := make(chan struct{})
			go func() {
				defer close(done)
				tt.call(ctx, lim.Client(tt.name))
			}()

			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Fatalf("%s did not return when its context expired; it is blocked on a "+
					"store load it has no way to cancel", tt.name)
			}
		})
	}
}

// blockingLoadStore blocks every Load until released is closed.
type blockingLoadStore struct{ released chan struct{} }

func (s *blockingLoadStore) Save(context.Context, string, pace.State) error { return nil }

func (s *blockingLoadStore) Load(ctx context.Context, _ string) (pace.State, bool, error) {
	select {
	case <-s.released:
	case <-ctx.Done():
		return pace.State{}, false, ctx.Err()
	}
	return pace.State{}, false, nil
}

func (s *blockingLoadStore) Close() error { return nil }
