package limiter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace/limit"
	pace "github.com/jaeminst/pace/limiter"
)

// TestResultTTLExpiresCachedResponses covers the growth term nothing else
// bounds. The result cache is what makes a repeated Durable call cheap, but on
// a busy service it is the dominant contributor to the database file's size,
// and before this nothing ever deleted from it.
func TestResultTTLExpiresCachedResponses(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clk := newFakeClock()
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   100,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
		Clock:   clk,
		Queue: pace.QueueConfig{
			ResultTTL: time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	ctx := context.Background()
	alice := lim.Client("alice")

	if _, err := durableDo(ctx, alice, "job-1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	// While cached, a repeat costs nothing.
	if _, err := durableDo(ctx, alice, "job-1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if got := served.Load(); got != 1 {
		t.Fatalf("the server was called %d times while the result was cached, want 1", got)
	}

	// Past the TTL, the purge drops it.
	clk.advance(2 * time.Hour)
	pace.PurgeResults(lim)

	if _, err := durableDo(ctx, alice, "job-1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if got := served.Load(); got != 2 {
		t.Errorf("the server was called %d times after the cache expired, want 2", got)
	}
}

func TestResultTTLNegativeKeepsForever(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	clk := newFakeClock()
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   100,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
		Clock:   clk,
		Queue: pace.QueueConfig{
			ResultTTL: -1,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	ctx := context.Background()
	if _, err := durableDo(ctx, lim.Client("alice"), "job-1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}

	clk.advance(10_000 * time.Hour)
	pace.PurgeResults(lim)

	if _, err := durableDo(ctx, lim.Client("alice"), "job-1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if got := served.Load(); got != 1 {
		t.Errorf("a result was purged despite ResultTTL being negative: %d calls", got)
	}
}

func TestPurgeResultsWithoutQueueIsNoOp(t *testing.T) {
	lim, _ := newTestLimiter(t)
	pace.PurgeResults(lim) // must not panic with no SQLite store configured
}
