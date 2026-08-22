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

	lim, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: rate.PerMinute(600), Burst: 10})
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

	lim, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: rate.PerMinute(600), Burst: 10})
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
		Rate:       rate.PerMinute(600),
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
	limiter.WaitGCLoop(lim)

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
		Rate:    rate.PerMinute(6000),
		Burst:   100,
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Users with live state, so Evict has something to persist.
	for _, u := range []string{"a", "b", "c", "d"} {
		lim.Client(u).Allow(context.Background())
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, u := range []string{"a", "b", "c", "d", "e", "f"} {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			for range 200 {
				lim.Client(u).Allow(context.Background())
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
		Rate:    rate.PerMinute(600),
		Burst:   10,
		// One shard, so every user collides and the deadlock is certain rather
		// than dependent on the hash.
		Shards: 1,
		Observer: &observe.Observer{
			UserEvicted: func(_ context.Context, i observe.EvictInfo) {
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
		lim.Client(u).Allow(context.Background())
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
		observer *observe.Observer
	}{
		{"without an observer", nil},
		{"with an observer", &observe.Observer{UserEvicted: func(context.Context, observe.EvictInfo) {}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			lim, err := pace.New(pace.Config{
				BaseURL:  "http://example.invalid",
				Rate:     rate.PerMinute(600),
				Burst:    10,
				Observer: tt.observer,
			})
			if err != nil {
				t.Fatal(err)
			}

			users := []string{"a", "b", "c", "d", "e"}
			for _, u := range users {
				lim.Client(u).Allow(context.Background())
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

func TestContextCancellation(t *testing.T) {
	client, err := pace.New(pace.Config{
		// 1/min so the second request blocks for ~60s.
		BaseURL: "http://127.0.0.1:0",
		Rate:    rate.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Exhaust the token (no HTTP call needed — Request() just waits for a token).
	if err := client.Client("u").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	// Second request should return when context times out.
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = client.Client("u").Wait(ctx2)
	if err == nil {
		t.Fatal("want error from cancelled context")
	}
}

func TestClose_StoreError(t *testing.T) {
	// Create a client with a store, pre-populate a user so saveAll has work to do,
	// then close the underlying db — Close() must log (not panic) on both
	// saveAll write errors and store.Close errors.
	srv := newEchoServer(t)
	defer srv.Close()
	st := newBreakableStore()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6000),
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	// Close the underlying db so saveAll + store.Close both fail.
	limiter.CloseLimiterStore(client)

	// Close must not panic or block; it should just log warnings.
	client.Close()
}

func TestClose_StoreCloseError(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Make a request so saveAll has a user to flush.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// Inject a mock that errors on Close; Close must not panic.
	limiter.SetLimiterStore(client, &mockCloseErrStore{})
	client.Close()
}

// --- Graceful Shutdown tests ---

func TestShutdown_GracefulFinish(t *testing.T) {
	// Shutdown with a generous deadline: all in-flight requests complete before
	// the timeout, so Shutdown returns nil.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6000),
		Burst:   10,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for range 3 {
		go func() {
			defer wg.Done()
			_, _ = client.Client("u").Get(context.Background(), "/")
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("expected graceful shutdown, got %v", err)
	}
}

func TestShutdown_ForcedOnTimeout(t *testing.T) {
	// Shutdown with an expired context: force-cancel path is taken.
	client, err := pace.New(pace.Config{
		// rate=1/min, burst=1: second request blocks for ~60s
		BaseURL: "http://127.0.0.1:1",
		Rate:    rate.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the token so subsequent requests block in Wait.
	if err := client.Client("u").Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Start a goroutine that will block in bucket.Wait.
	waiting := make(chan struct{})
	limiter.SetBeforeWaitHook(client, sync.OnceFunc(func() { close(waiting) }))
	go func() { _ = client.Client("u").Wait(context.Background()) }()
	<-waiting

	// Shutdown with an already-cancelled context → forced path.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err = client.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestShutdown_RejectsNewRequests(t *testing.T) {
	// After Shutdown sets shuttingDown=true, new Request calls must return
	// ErrClosed via the shutting-down branch (not the ctx.Done branch, which
	// fires only after Close is called). We keep an in-flight request alive so
	// Shutdown blocks on activeWg.Wait() and never reaches Close during the test.
	client, err := pace.New(pace.Config{
		// rate=1/min so the second goroutine blocks in bucket.Wait for ~60s.
		BaseURL: "http://127.0.0.1:1",
		Rate:    rate.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the single burst token.
	if err := client.Client("u").Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	// This goroutine blocks inside bucket.Wait, keeping activeWg at 1 so
	// Shutdown cannot proceed to Close() yet.
	waiting := make(chan struct{})
	limiter.SetBeforeWaitHook(client, sync.OnceFunc(func() { close(waiting) }))
	go func() { _ = client.Client("u").Wait(context.Background()) }()
	<-waiting

	// Start Shutdown in a goroutine. It closes the door to new requests
	// immediately, then blocks on activeWg.Wait() because the goroutine above
	// is still in Wait.
	flagged := make(chan struct{})
	limiter.SetShuttingDownHook(client, sync.OnceFunc(func() { close(flagged) }))
	shutdownDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = client.Shutdown(ctx)
		close(shutdownDone)
	}()
	<-flagged // the door is shut, but nothing has been cancelled yet

	// m.ctx is still alive (Close not called yet), but shuttingDown=true.
	// Request must return ErrClosed via the shuttingDown branch.
	err = client.Client("u2").Wait(context.Background())
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ErrClosed from shuttingDown branch, got %v", err)
	}
	<-shutdownDone
}
