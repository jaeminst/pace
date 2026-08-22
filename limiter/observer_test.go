package limiter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/store/memory"
)

// recorder collects observer events for assertions.
type recorder struct {
	mu        sync.Mutex
	throttled []observe.ThrottleInfo
	requests  []observe.RequestInfo
	evicted   []observe.EvictInfo
}

func (r *recorder) observer() *observe.Observer {
	return &observe.Observer{
		Throttled: func(_ context.Context, i observe.ThrottleInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.throttled = append(r.throttled, i)
		},
		RequestFinished: func(_ context.Context, i observe.RequestInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.requests = append(r.requests, i)
		},
		UserEvicted: func(_ context.Context, i observe.EvictInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.evicted = append(r.evicted, i)
		},
	}
}

func (r *recorder) snapshot() ([]observe.ThrottleInfo, []observe.RequestInfo, []observe.EvictInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]observe.ThrottleInfo(nil), r.throttled...),
		append([]observe.RequestInfo(nil), r.requests...),
		append([]observe.EvictInfo(nil), r.evicted...)
}

func TestObserverReportsFinishedRequests(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))
	defer srv.Close()

	rec := &recorder{}
	lim, _ := newTestLimiterOn(t, srv.URL, func(c *pace.Config) { c.Observer = rec.observer() })

	if _, err := lim.Client("alice").Post(context.Background(), "/things"); err != nil {
		t.Fatal(err)
	}

	_, requests, _ := rec.snapshot()
	if len(requests) != 1 {
		t.Fatalf("RequestFinished fired %d times, want 1", len(requests))
	}
	got := requests[0]
	if got.UserID != "alice" || got.Method != http.MethodPost || got.Path != "/things" {
		t.Errorf("request info = %+v, want alice POST /things", got)
	}
	if got.Status != http.StatusTeapot {
		t.Errorf("Status = %d, want %d", got.Status, http.StatusTeapot)
	}
	if got.Err != nil {
		t.Errorf("Err = %v, want nil", got.Err)
	}
}

// TestObserverReportsTransportFailure: a request with no response still gets
// reported, with a zero status and the error attached.
func TestObserverReportsTransportFailure(t *testing.T) {
	rec := &recorder{}
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      rate.PerMinute(600),
		Burst:     10,
		Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, errTransport }),
		Observer:  rec.observer(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	if _, err := lim.Client("alice").Get(context.Background(), "/"); err == nil {
		t.Fatal("the request reported success despite a failing transport")
	}
	_, requests, _ := rec.snapshot()
	if len(requests) != 1 {
		t.Fatalf("RequestFinished fired %d times, want 1", len(requests))
	}
	if requests[0].Status != 0 || requests[0].Err == nil {
		t.Errorf("request info = %+v, want zero status and a non-nil error", requests[0])
	}
}

// TestObserverThrottledCarriesDelay is the improvement over the old callback,
// which reported only that throttling had happened. The delay is the number a
// caller can act on.
func TestObserverThrottledCarriesDelay(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := &recorder{}
	lim, err := pace.New(pace.Config{
		BaseURL:  srv.URL,
		Rate:     rate.PerMinute(6), // one token every 10s
		Burst:    1,
		Observer: rec.observer(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	alice := lim.Client("alice")
	if _, err := alice.Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// The burst is gone; this must report as throttled.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = alice.Get(ctx, "/")

	throttled, _, _ := rec.snapshot()
	if len(throttled) == 0 {
		t.Fatal("Throttled never fired for a request with no token")
	}
	got := throttled[0]
	if got.UserID != "alice" {
		t.Errorf("UserID = %q, want alice", got.UserID)
	}
	// One token per ten seconds, and the bucket is empty.
	if got.Delay < 5*time.Second || got.Delay > 11*time.Second {
		t.Errorf("Delay = %v, want roughly 10s", got.Delay)
	}
	if got.Tokens >= 1 {
		t.Errorf("Tokens = %v at the moment of throttling, want < 1", got.Tokens)
	}
	if got.Burst != 1 || got.Limit != rate.PerMinute(6) {
		t.Errorf("configuration = (limit %v, burst %d), want (6/min, 1)", got.Limit, got.Burst)
	}
}

func TestObserverReportsEvictionReasons(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rec := &recorder{}
	clk := newFakeClock()
	lim, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       rate.PerMinute(6000),
		Burst:      100,
		Clock:      clk,
		IdleExpiry: time.Minute,
		Observer:   rec.observer(),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// Explicit.
	if _, err := lim.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	if _, err := lim.Client("alice").Evict(ctx); err != nil {
		t.Fatal(err)
	}

	// Idle.
	if _, err := lim.Client("bob").Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Hour)
	limiter.CollectIdle(lim)

	// Shutdown.
	if _, err := lim.Client("carol").Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, evicted := rec.snapshot()
	want := map[string]observe.EvictReason{
		"alice": observe.EvictExplicit,
		"bob":   observe.EvictIdle,
		"carol": observe.EvictShutdown,
	}
	got := map[string]observe.EvictReason{}
	seen := map[string]bool{}
	for _, e := range evicted {
		got[e.UserID] = e.Reason
		seen[e.UserID] = true
	}
	for user, reason := range want {
		if !seen[user] {
			// Distinguish "never reported" from "reported as idle": EvictIdle
			// is the zero value, so a missing event would otherwise look right.
			t.Errorf("%s was never reported as evicted, want %v", user, reason)
			continue
		}
		if got[user] != reason {
			t.Errorf("%s evicted with reason %v, want %v", user, got[user], reason)
		}
	}
}

func TestStatsCountRequestsAndUsers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, _ := newTestLimiterOn(t, srv.URL)
	ctx := context.Background()

	if got := lim.Stats(); got.Users != 0 || got.Requests != 0 {
		t.Fatalf("fresh Stats = %+v, want zeroes", got)
	}

	for _, u := range []string{"alice", "bob", "carol"} {
		if _, err := lim.Client(u).Get(ctx, "/"); err != nil {
			t.Fatal(err)
		}
	}

	got := lim.Stats()
	if got.Users != 3 {
		t.Errorf("Users = %d, want 3", got.Users)
	}
	if got.Requests != 3 {
		t.Errorf("Requests = %d, want 3", got.Requests)
	}
	if got.Errors != 0 {
		t.Errorf("Errors = %d, want 0", got.Errors)
	}
}

func TestStatsTrackThrottlingAndEviction(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: rate.PerMinute(6), Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	ctx := context.Background()
	alice := lim.Client("alice")
	if _, err := alice.Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, _ = alice.Get(deadlined, "/")

	got := lim.Stats()
	if got.Throttled == 0 {
		t.Error("Throttled = 0 after a request with no token")
	}
	if got.WaitTotal == 0 {
		t.Error("Wait = 0 despite a throttled request")
	}
	// Errors counts dispatched requests that came back without a response. A
	// request that never got a token was never dispatched, and is accounted
	// for by Throttled instead.
	if got.Errors != 0 {
		t.Errorf("Errors = %d for a request that was throttled rather than dispatched, want 0", got.Errors)
	}

	if _, err := alice.Evict(ctx); err != nil {
		t.Fatal(err)
	}
	if after := lim.Stats(); after.Evictions != 1 || after.Users != 0 {
		t.Errorf("after Evict, Stats = %+v, want 1 eviction and 0 users", after)
	}
}

// TestStatsUsersFallAfterSweep guards the per-shard tally against drift: it is
// maintained separately from the map, so an eviction path that forgets to
// decrement it would leave the gauge climbing forever.
func TestStatsUsersFallAfterSweep(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clk := newFakeClock()
	for _, withStore := range []bool{false, true} {
		name := "store=none"
		if withStore {
			name = "store=memory"
		}
		t.Run(name, func(t *testing.T) {
			cfg := pace.Config{
				BaseURL:    srv.URL,
				Rate:       rate.PerMinute(6000),
				Burst:      100,
				Clock:      clk,
				IdleExpiry: time.Minute,
			}
			if withStore {
				cfg.Store = memory.New()
			}
			lim, err := pace.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer lim.Close()

			ctx := context.Background()
			for i := range 5 {
				if _, err := lim.Client(string(rune('a'+i))).Get(ctx, "/"); err != nil {
					t.Fatal(err)
				}
			}
			if got := lim.Stats().Users; got != 5 {
				t.Fatalf("Users = %d before the sweep, want 5", got)
			}

			clk.advance(time.Hour)
			limiter.CollectIdle(lim)

			if got := lim.Stats().Users; got != 0 {
				t.Errorf("Users = %d after every user was collected, want 0", got)
			}
		})
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var errTransport = errors.New("dial refused")

// TestEvictInfoCarriesTheUsersState is why UserEvicted changed shape. Two loose
// positional parameters could never gain a field, and eviction is the event an
// operator investigates when a store is slow — so the two things they would
// want, the token count and how long the user had been idle, were unreachable
// forever.
func TestEvictInfoCarriesTheUsersState(t *testing.T) {
	var got []observe.EvictInfo
	var mu sync.Mutex

	clk := newFakeClock()
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    rate.PerMinute(60),
		Burst:   10,
		Clock:   clk,
		Observer: &observe.Observer{
			UserEvicted: func(ctx context.Context, i observe.EvictInfo) {
				if ctx == nil {
					t.Error("UserEvicted received a nil context")
				}
				mu.Lock()
				defer mu.Unlock()
				got = append(got, i)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	alice := lim.Client("alice")
	alice.Allow(context.Background())
	alice.Allow(context.Background()) // 8 of 10 left
	evictedAt := clk.Now()

	if _, err := alice.Evict(context.Background()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("UserEvicted fired %d times, want 1", len(got))
	}
	e := got[0]
	if e.UserID != "alice" || e.Reason != observe.EvictExplicit {
		t.Errorf("EvictInfo = %+v, want alice/EvictExplicit", e)
	}
	if e.Tokens != 8 {
		t.Errorf("Tokens = %v, want 8 after two of ten were spent", e.Tokens)
	}
	if !e.LastUsed.Equal(evictedAt) {
		t.Errorf("LastUsed = %v, want %v", e.LastUsed, evictedAt)
	}
}

// TestEvictInfoIsPopulatedOnEveryPath: the three reasons are produced by three
// different pieces of code, and only one of them had the user's state to hand
// without going and getting it.
func TestEvictInfoIsPopulatedOnEveryPath(t *testing.T) {
	for _, tt := range []struct {
		name   string
		reason observe.EvictReason
		run    func(t *testing.T, lim *pace.Limiter)
	}{
		{"idle sweep", observe.EvictIdle, func(t *testing.T, lim *pace.Limiter) {
			t.Helper()
			limiter.CollectIdle(lim)
		}},

		{"shutdown", observe.EvictShutdown, func(t *testing.T, lim *pace.Limiter) {
			t.Helper()
			if err := lim.Close(); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var got []observe.EvictInfo
			var mu sync.Mutex

			clk := newFakeClock()
			lim, err := pace.New(pace.Config{
				BaseURL:    "http://example.invalid",
				Rate:       rate.PerMinute(60),
				Burst:      10,
				Clock:      clk,
				IdleExpiry: time.Minute,
				Observer: &observe.Observer{
					UserEvicted: func(_ context.Context, i observe.EvictInfo) {
						mu.Lock()
						defer mu.Unlock()
						got = append(got, i)
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = lim.Close() })

			lim.Client("alice").Allow(context.Background()) // 9 of 10 left
			lastUsed := clk.Now()
			// Idle long enough to be collected, but not so long that the bucket
			// refills past 9 — PerMinute(60) is one token per second.
			clk.advance(90 * time.Second)
			tt.run(t, lim)

			mu.Lock()
			defer mu.Unlock()
			if len(got) != 1 {
				t.Fatalf("UserEvicted fired %d times, want 1", len(got))
			}
			if got[0].Reason != tt.reason {
				t.Errorf("Reason = %v, want %v", got[0].Reason, tt.reason)
			}
			// The bucket refilled to full while idle, which is the point: a
			// zero here means the path built an EvictInfo without reading it.
			if got[0].Tokens != 10 {
				t.Errorf("Tokens = %v, want the refilled 10; this path built an EvictInfo "+
					"without reading the bucket", got[0].Tokens)
			}
			if !got[0].LastUsed.Equal(lastUsed) {
				t.Errorf("LastUsed = %v, want %v", got[0].LastUsed, lastUsed)
			}
		})
	}
}
