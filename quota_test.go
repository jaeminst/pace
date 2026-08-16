package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

// TestAbandonedRequestCostsNothing pins where the token is taken. Acquiring it
// when the builder was handed out meant a caller who built a Request and then
// returned early — on a validation failure, a branch, a panic — silently burned
// quota that nothing could give back.
func TestAbandonedRequestCostsNothing(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = pace.PerMinute(6) // one token per 10s: refill cannot mask a spend
		c.Burst = 3
	})
	alice := lim.Client("alice")

	// Force the bucket into existence so Tokens() reports real state.
	if !alice.Allow() {
		t.Fatal("Allow = false on a fresh burst of 3")
	}
	before := alice.Tokens()

	for range 10 {
		_ = alice.Request().SetHeader("X-Test", "1").SetBody([]byte("body"))
	}

	if after := alice.Tokens(); after != before {
		t.Errorf("10 abandoned Requests moved the token count from %v to %v", before, after)
	}
}

// TestRequestTokenTakenAtSendTime is the other half: the token must still be
// spent when the request actually goes out.
func TestRequestTokenTakenAtSendTime(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = pace.PerMinute(6)
		c.Burst = 3
	})
	alice := lim.Client("alice")
	ctx := context.Background()

	if _, err := alice.Request().Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	after := alice.Tokens()
	if after > 2.01 {
		t.Errorf("Tokens() = %v after one request against a burst of 3, want ~2", after)
	}
}

// TestDurableEmptyIDIsRejected covers a bug with two heads. Dispatch keyed on a
// non-empty ID string, so Durable("") took the plain branch — and the plain
// branch assumed the caller had already paid for a token, which Durable never
// did. A single typo therefore turned off rate limiting entirely for that call.
func TestDurableEmptyIDIsRejected(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   1,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	pace.WaitReplay(lim)

	req, err := lim.Client("alice").Durable("")
	if !errors.Is(err, pace.ErrInvalidID) {
		t.Fatalf("Durable(\"\") = %v, want ErrInvalidID", err)
	}
	if req != nil {
		t.Error("Durable returned a usable Request for an empty ID")
	}
	if n := served.Load(); n != 0 {
		t.Errorf("%d requests reached the server despite the rejected ID", n)
	}
}

// TestDurableConsumesToken proves the durable path pays the same price as the
// plain one; the empty-ID bug above existed precisely because it did not.
func TestDurableConsumesToken(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6),
		Burst:   3,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	pace.WaitReplay(lim)

	alice := lim.Client("alice")
	if _, err := durableDo(context.Background(), alice, "job-1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if got := alice.Tokens(); got > 2.01 {
		t.Errorf("Tokens() = %v after one durable request against a burst of 3, want ~2", got)
	}
}

func TestDurableWithoutQueue(t *testing.T) {
	lim, _ := newTestLimiter(t)
	req, err := lim.Client("alice").Durable("job-1")
	if !errors.Is(err, pace.ErrNoQueue) {
		t.Errorf("Durable without DBPath = %v, want ErrNoQueue", err)
	}
	if req != nil {
		t.Error("Durable returned a usable Request with no queue configured")
	}
}

func TestAllowDoesNotBlockAndConsumes(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = pace.PerMinute(6) // 10s per token: no refill during the test
		c.Burst = 2
	})
	alice := lim.Client("alice")

	if !alice.Allow() {
		t.Fatal("first Allow = false, want true")
	}
	if !alice.Allow() {
		t.Fatal("second Allow = false, want true")
	}
	if alice.Allow() {
		t.Error("third Allow = true after the burst of 2 was spent, want false")
	}
	// A different user is unaffected.
	if !lim.Client("bob").Allow() {
		t.Error("bob's Allow = false, want true")
	}
}

func TestAllowAfterClose(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if lim.Client("alice").Allow() {
		t.Error("Allow = true on a closed Limiter, want false")
	}
}

func TestWaitConsumesToken(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = pace.PerMinute(6)
		c.Burst = 2
	})
	alice := lim.Client("alice")
	ctx := context.Background()

	if err := alice.Wait(ctx); err != nil {
		t.Fatalf("first Wait: %v", err)
	}
	if err := alice.Wait(ctx); err != nil {
		t.Fatalf("second Wait: %v", err)
	}

	// The burst is gone and refill is 10s away, so a short deadline must fail
	// with a LimitError rather than block.
	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var le *pace.LimitError
	if err := alice.Wait(deadlined); !errors.As(err, &le) {
		t.Errorf("third Wait = %v, want *pace.LimitError", err)
	}
}

func TestWaitAfterClose(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lim.Client("alice").Wait(context.Background()); !errors.Is(err, pace.ErrClosed) {
		t.Errorf("Wait on a closed Limiter = %v, want ErrClosed", err)
	}
}
