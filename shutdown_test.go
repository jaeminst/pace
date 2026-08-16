package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

// blockingServer returns a server whose handler signals when a request has
// arrived and then blocks until release is closed. It replaces a sleep: the
// test knows exactly when the request is in flight.
func blockingServer(t *testing.T) (srv *httptest.Server, arrived <-chan struct{}, release func()) {
	t.Helper()
	arrivedCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var arriveOnce, releaseOnce sync.Once
	release = func() { releaseOnce.Do(func() { close(releaseCh) }) }

	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		arriveOnce.Do(func() { close(arrivedCh) })
		select {
		case <-releaseCh:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	// httptest.Server.Close blocks on outstanding requests, so a failing test
	// must release the handler before the server is torn down. Cleanups run in
	// reverse order, so this one runs first.
	t.Cleanup(srv.Close)
	t.Cleanup(release)
	return srv, arrivedCh, release
}

// TestShutdownWaitsForInFlightRequest is the property Shutdown documents: it
// "waits until all in-flight requests finish".
//
// The active-request counter used to be scoped to the call that hands back the
// builder, which returns before the HTTP round-trip even starts. The counter
// was therefore already zero by the time the request was on the wire, and
// Shutdown returned — and closed the store — while requests were still running.
func TestShutdownWaitsForInFlightRequest(t *testing.T) {
	srv, arrived, release := blockingServer(t)

	lim, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: pace.PerMinute(600), Burst: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })

	done := make(chan error, 1)
	go func() {
		_, err := lim.Client("alice").Get(context.Background(), "/")
		done <- err
	}()

	<-arrived // the request is now on the wire

	shutdownReturned := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		shutdownReturned <- lim.Shutdown(ctx)
	}()

	// Shutdown must still be blocked, because the request has not finished.
	select {
	case err := <-shutdownReturned:
		t.Fatalf("Shutdown returned (%v) while a request was still in flight", err)
	case <-time.After(250 * time.Millisecond):
	}

	release()

	select {
	case err := <-shutdownReturned:
		if err != nil {
			t.Errorf("Shutdown = %v, want nil once the request completed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return after the request completed")
	}

	if err := <-done; err != nil {
		t.Errorf("in-flight request failed: %v", err)
	}
}

// TestShutdownDeadlineCancelsInFlightRequest is the other half: when the
// caller's deadline expires, Shutdown must actually abort the round-trip rather
// than block forever on a server that never answers.
func TestShutdownDeadlineCancelsInFlightRequest(t *testing.T) {
	srv, arrived, release := blockingServer(t)
	defer release()

	lim, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: pace.PerMinute(600), Burst: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })

	done := make(chan error, 1)
	go func() {
		_, err := lim.Client("alice").Get(context.Background(), "/")
		done <- err
	}()

	<-arrived

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err = lim.Shutdown(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Shutdown = %v, want context.DeadlineExceeded", err)
	}

	// Shutdown force-cancels, so the request must come back rather than hang.
	select {
	case reqErr := <-done:
		if reqErr == nil {
			t.Error("in-flight request succeeded, want cancellation")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown returned but the request was never cancelled")
	}
}

// TestNoStoreAccessAfterClose guards the invariant that makes the shutdown
// ordering matter: once the store is closed, nothing may touch it.
func TestNoStoreAccessAfterClose(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &recordingStore{}
	lim, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(600),
		Burst:      10,
		Store:      st,
		GCInterval: time.Millisecond,
		IdleExpiry: time.Nanosecond, // every user is instantly collectable
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for _, u := range []string{"a", "b", "c", "d", "e"} {
		if _, err := lim.Client(u).Get(ctx, "/"); err != nil {
			t.Fatalf("%s: %v", u, err)
		}
	}

	if err := lim.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// The GC goroutine may still have been mid-sweep when Close ran; if the
	// shutdown sequence does not wait for it, a Save lands after Close.
	pace.WaitGCLoop(lim)

	if n := st.opsAfterClose(); n > 0 {
		t.Errorf("%d store operations ran after Close", n)
	}
}

// TestAllowAndEvictRespectTheShutdownBarrier covers the two entry points that
// reach the store without going through a request. Both used to check
// shuttingDown loosely or not at all, so a call racing Close could load or save
// through a handle Close had already shut.
func TestAllowAndEvictRespectTheShutdownBarrier(t *testing.T) {
	st := &recordingStore{}
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Users with live state, so Evict has something to persist.
	for _, u := range []string{"a", "b", "c", "d"} {
		lim.Client(u).Allow()
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, u := range []string{"a", "b", "c", "d", "e", "f"} {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for range 200 {
				lim.Client(u).Allow()
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			for range 200 {
				// The error is the point of the call, not a failure: once the
				// Limiter is closing, Evict must refuse rather than write.
				_, _ = lim.Client(u).Evict(context.Background())
			}
		}()
	}

	close(start)
	if err := lim.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	if n := st.opsAfterClose(); n > 0 {
		t.Errorf("%d store operations ran after Close", n)
	}
}

// TestEvictObserverIsNotCalledUnderTheShardLock: the UserEvicted hook used to
// fire with the shard write lock held, so an observer that asked the Limiter
// anything about a user on the same shard deadlocked against the eviction that
// notified it. Reading your own state is the first thing such a hook would do.
func TestEvictObserverIsNotCalledUnderTheShardLock(t *testing.T) {
	var lim *pace.Limiter
	var seen int

	var err error
	lim, err = pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(600),
		Burst:   10,
		// One shard, so every user collides and the deadlock is certain rather
		// than dependent on the hash.
		Shards: 1,
		Observer: &pace.Observer{
			UserEvicted: func(_ context.Context, i pace.EvictInfo) {
				seen++
				// Calls back into the Limiter, taking the same shard's lock.
				lim.Client(i.UserID).Tokens()
				lim.Stats()
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	for _, u := range []string{"alice", "bob"} {
		lim.Client(u).Allow()
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		evict(t, lim.Client("alice"))
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Evict deadlocked: the observer was called with the shard lock held")
	}
	if seen != 1 {
		t.Errorf("UserEvicted fired %d times, want 1", seen)
	}
}

// TestStatsPopulationIsZeroAfterClose: shutdown reported every remaining user
// as evicted but left them in the shards, so Stats returned "N users" and "+N
// evictions" in the same snapshot — two claims that cannot both be true.
func TestStatsPopulationIsZeroAfterClose(t *testing.T) {
	for _, tt := range []struct {
		name     string
		observer *pace.Observer
	}{
		{"without an observer", nil},
		{"with an observer", &pace.Observer{UserEvicted: func(context.Context, pace.EvictInfo) {}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lim, err := pace.New(pace.Config{
				BaseURL:  "http://example.invalid",
				Rate:     pace.PerMinute(600),
				Burst:    10,
				Observer: tt.observer,
			})
			if err != nil {
				t.Fatal(err)
			}

			users := []string{"a", "b", "c", "d", "e"}
			for _, u := range users {
				lim.Client(u).Allow()
			}
			if got := lim.Stats().Users; got != int64(len(users)) {
				t.Fatalf("Users = %d before Close, want %d", got, len(users))
			}

			if err := lim.Close(); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got := lim.Stats()
			if got.Users != 0 {
				t.Errorf("Users = %d after Close, want 0", got.Users)
			}
			// The population did not vanish silently: it was reported as gone.
			if got.Evictions != int64(len(users)) {
				t.Errorf("Evictions = %d, want %d", got.Evictions, len(users))
			}
		})
	}
}
