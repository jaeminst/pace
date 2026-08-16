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
