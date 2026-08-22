package limiter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

func TestLimitErrorNotErrClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6), Burst: 1}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	if err := pool.Client("alice").Wait(ctx); err != nil {
		t.Fatalf("first request: %v", err)
	}

	// The bucket is empty and refills in ten seconds; this deadline cannot be met.
	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = pool.Client("alice").Wait(deadlined)
	if err == nil {
		t.Fatal("second request succeeded, want a rate-limit error")
	}
	if errors.Is(err, limiter.ErrClosed) {
		t.Fatalf("got ErrClosed, but the pool is open: %v", err)
	}

	var le *limiter.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("got %T (%v), want *limiter.LimitError", err, err)
	}
	if le.UserID != "alice" {
		t.Errorf("LimitError.UserID = %q, want %q", le.UserID, "alice")
	}
	if le.Limit != bucket.PerMinute(6) {
		t.Errorf("LimitError.Limit = %v, want %v", le.Limit, bucket.PerMinute(6))
	}
	if le.Burst != 1 {
		t.Errorf("LimitError.Burst = %d, want 1", le.Burst)
	}
}

// TestErrClosedStillReportedWhenClosed guards the other side of the same
// branch: a genuinely closed Client must still say so.
func TestErrClosedStillReportedWhenClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	pool, err := client.New(config.Config{BaseURL: srv.URL, QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1})})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	if err := pool.Client("alice").Wait(context.Background()); !errors.Is(err, limiter.ErrClosed) {
		t.Fatalf("Request after Close = %v, want ErrClosed", err)
	}
}

func TestLimitErrorMessageAndUnwrap(t *testing.T) {
	base := errors.New("boom")
	e := &limiter.LimitError{UserID: "bob", Limit: bucket.PerMinute(30), Burst: 5, Err: base}
	if !errors.Is(e, base) {
		t.Error("LimitError does not unwrap to its cause")
	}
	if got, want := e.Error(), `pace: rate limit for "bob" (30/min, burst 5): boom`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	withDelay := &limiter.LimitError{UserID: "bob", Limit: bucket.PerMinute(30), Burst: 5, Delay: 2 * time.Second, Err: base}
	if got, want := withDelay.Error(), `pace: rate limit for "bob" (30/min, burst 5): boom; retry in 2s`; got != want {
		t.Errorf("Error() with delay = %q, want %q", got, want)
	}
}

func TestLimitErrorCarriesDelay(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6), Burst: 1}),
	})
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
	_, err = alice.Get(deadlined, "/")

	var le *limiter.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("got %T (%v), want *limiter.LimitError", err, err)
	}
	if le.Delay < 5*time.Second || le.Delay > 11*time.Second {
		t.Errorf("LimitError.Delay = %v, want roughly 10s", le.Delay)
	}
}
