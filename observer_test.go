package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

// recorder collects observer events for assertions.
type recorder struct {
	mu        sync.Mutex
	throttled []pace.ThrottleInfo
	requests  []pace.RequestInfo
	evicted   []evictEvent
	jobs      []pace.JobInfo
}

type evictEvent struct {
	userID string
	reason pace.EvictReason
}

func (r *recorder) observer() *pace.Observer {
	return &pace.Observer{
		Throttled: func(_ context.Context, i pace.ThrottleInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.throttled = append(r.throttled, i)
		},
		RequestFinished: func(_ context.Context, i pace.RequestInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.requests = append(r.requests, i)
		},
		UserEvicted: func(userID string, reason pace.EvictReason) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.evicted = append(r.evicted, evictEvent{userID, reason})
		},
		JobTransition: func(i pace.JobInfo) {
			r.mu.Lock()
			defer r.mu.Unlock()
			r.jobs = append(r.jobs, i)
		},
	}
}

func (r *recorder) snapshot() ([]pace.ThrottleInfo, []pace.RequestInfo, []evictEvent, []pace.JobInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]pace.ThrottleInfo(nil), r.throttled...),
		append([]pace.RequestInfo(nil), r.requests...),
		append([]evictEvent(nil), r.evicted...),
		append([]pace.JobInfo(nil), r.jobs...)
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

	_, requests, _, _ := rec.snapshot()
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
	if got.Durable {
		t.Error("Durable = true for a plain request")
	}
}

// TestObserverReportsTransportFailure: a request with no response still gets
// reported, with a zero status and the error attached.
func TestObserverReportsTransportFailure(t *testing.T) {
	rec := &recorder{}
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      pace.PerMinute(600),
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
	_, requests, _, _ := rec.snapshot()
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
		Rate:     pace.PerMinute(6), // one token every 10s
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

	throttled, _, _, _ := rec.snapshot()
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
	if got.Burst != 1 || got.Limit != pace.PerMinute(6) {
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
		Rate:       pace.PerMinute(6000),
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
	pace.CollectIdle(lim)

	// Shutdown.
	if _, err := lim.Client("carol").Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	_, _, evicted, _ := rec.snapshot()
	want := map[string]pace.EvictReason{
		"alice": pace.EvictExplicit,
		"bob":   pace.EvictIdle,
		"carol": pace.EvictShutdown,
	}
	got := map[string]pace.EvictReason{}
	seen := map[string]bool{}
	for _, e := range evicted {
		got[e.userID] = e.reason
		seen[e.userID] = true
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

func TestObserverReportsJobTransitions(t *testing.T) {
	rec := &recorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL:  srv.URL,
		Rate:     pace.PerMinute(6000),
		Burst:    100,
		DBPath:   filepath.Join(t.TempDir(), "q.db"),
		Observer: rec.observer(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if _, err := durableDo(context.Background(), lim.Client("alice"), "job-1", http.MethodPost, "/"); err != nil {
		t.Fatal(err)
	}

	_, _, _, jobs := rec.snapshot()
	phases := map[pace.JobPhase]bool{}
	for _, j := range jobs {
		phases[j.Phase] = true
		if j.ID != "job-1" {
			t.Errorf("job ID = %q, want job-1", j.ID)
		}
	}
	if !phases[pace.JobClaimed] || !phases[pace.JobCompleted] {
		t.Errorf("saw phases %v, want both claimed and completed", jobs)
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

	lim, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: pace.PerMinute(6), Burst: 1})
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
			name = "store=sqlite"
		}
		t.Run(name, func(t *testing.T) {
			cfg := pace.Config{
				BaseURL:    srv.URL,
				Rate:       pace.PerMinute(6000),
				Burst:      100,
				Clock:      clk,
				IdleExpiry: time.Minute,
			}
			if withStore {
				cfg.DBPath = filepath.Join(t.TempDir(), "s.db")
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
			pace.CollectIdle(lim)

			if got := lim.Stats().Users; got != 0 {
				t.Errorf("Users = %d after every user was collected, want 0", got)
			}
		})
	}
}

func TestEvictReasonAndJobPhaseStrings(t *testing.T) {
	reasons := map[pace.EvictReason]string{
		pace.EvictIdle: "idle", pace.EvictExplicit: "explicit",
		pace.EvictShutdown: "shutdown", pace.EvictReason(9): "unknown",
	}
	for r, want := range reasons {
		if got := r.String(); got != want {
			t.Errorf("EvictReason(%d) = %q, want %q", r, got, want)
		}
	}
	phases := map[pace.JobPhase]string{
		pace.JobClaimed: "claimed", pace.JobCompleted: "completed",
		pace.JobRetrying: "retrying", pace.JobDead: "dead", pace.JobPhase(9): "unknown",
	}
	for p, want := range phases {
		if got := p.String(); got != want {
			t.Errorf("JobPhase(%d) = %q, want %q", p, got, want)
		}
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

var errTransport = errors.New("dial refused")
