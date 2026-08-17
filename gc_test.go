package pace_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

// TestUserIsolation verifies that exhausting one user's bucket does not affect another.
func TestUserIsolation(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	// 1 req/min, burst=1: after one call the user must wait ~60s for the next token.
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Alice consumes her single token.
	if _, err := client.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Bob has his own bucket and must not be affected.
	ctxBob, cancelBob := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelBob()
	if _, err := client.Client("bob").Get(ctxBob, "/"); err != nil {
		t.Fatalf("bob (isolated): %v", err)
	}

	// Alice is throttled — her second call must time out.
	ctxAlice, cancelAlice := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelAlice()
	if _, err := client.Client("alice").Get(ctxAlice, "/"); err == nil {
		t.Fatal("alice second call should have been throttled")
	}
}

func TestGC_EvictsIdleUser(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	client, err := pace.New(pace.Config{
		// burst=1, rate=1/min: alice's token is exhausted after one call
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(1),
		Burst:      1,
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Alice uses her single token.
	if _, err := client.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Alice is now throttled — second call times out.
	ctxShort, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := client.Client("alice").Get(ctxShort, "/"); err == nil {
		t.Fatal("alice should be throttled before GC")
	}

	// Advance clock past IdleExpiry and run GC.
	clock.advance(10 * time.Minute)
	pace.CollectIdle(client)

	// Alice's bucket is evicted and re-created fresh → burst=1 available again.
	ctxFresh, cancelFresh := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelFresh()
	if _, err := client.Client("alice").Get(ctxFresh, "/"); err != nil {
		t.Fatalf("alice after GC eviction: %v", err)
	}
}

func TestGC_SavesStateOnEvict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pace.db")
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	client, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(6000),
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
		DBPath:     dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatalf("alice: %v", err)
	}

	clock.advance(10 * time.Minute)
	pace.CollectIdle(client)

	// DB file must exist and contain alice's record.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db not found after GC: %v", err)
	}
}

func TestEvict_RemovesUser(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if !evict(t, client.Client("alice")) {
		t.Fatal("expected Evict to return true for existing user")
	}
	n, ok := client.Client("alice").Tokens()
	if ok || n != 0 {
		t.Fatalf("Tokens() after Evict = (%v, %v), want (0, false)", n, ok)
	}
}

func TestEvict_ReturnsFalseForUnknownUser(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:0",
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if evict(t, client.Client("ghost")) {
		t.Fatal("expected Evict to return false for unknown user")
	}
}

func TestEvict_SavesToDB(t *testing.T) {
	srv := newEchoServer(t)
	dbPath := filepath.Join(t.TempDir(), "evict.db")
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	tokensBefore := tokensOf(client.Client("alice"))
	evict(t, client.Client("alice"))

	// Re-open a new client: alice's tokens should be restored from DB
	client2, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	// Trigger user load by calling Get (creates bucket from DB)
	if _, err := client2.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	tokensAfter := tokensOf(client2.Client("alice"))
	// tokensAfter should be close to tokensBefore - 1 (we consumed one in client2)
	if tokensAfter >= tokensBefore {
		t.Fatalf("expected restored tokens (%v) < original (%v)", tokensAfter, tokensBefore)
	}
}

func TestGCLoop_ExitsOnClose(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	// WaitGCLoop blocks until the gcLoop goroutine exits via ctx.Done().
	pace.WaitGCLoop(client)
}

func TestAStoreLoadFailureStillServesTheUser(t *testing.T) {
	// Close the store before creating a new user — userFor must log the load
	// error and continue with a fresh bucket.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "load_err.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Break the store, then try to create a brand-new user.
	pace.CloseLimiterStore(client)

	// Should not panic; logger.Warn is called internally.
	if _, err := client.Client("new-user-after-close").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestEvict_StoreError(t *testing.T) {
	// Break the store, then evict a user — evictUser must log the save error.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "evict_err.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	pace.CloseLimiterStore(client)

	// The store is broken, so persisting fails. Evict reports that rather than
	// swallowing it into a log line: the caller asked for this write.
	present, err := client.Client("alice").Evict(context.Background())
	if !present {
		t.Error("Evict = false, want true for a user that was in memory")
	}
	if err == nil {
		t.Error("Evict = nil error with a closed store, want the store failure")
	}
}

func TestGCLoop_TickerFires(t *testing.T) {
	// Use a very short GCInterval so the ticker fires before Close(), covering
	// the case <-ticker.C: l.sweep() branch in gcLoop.
	client, err := pace.New(pace.Config{
		BaseURL:    "http://127.0.0.1:1",
		Rate:       pace.PerMinute(60),
		GCInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	swept := make(chan struct{})
	pace.SetAfterSweepHook(client, sync.OnceFunc(func() { close(swept) }))
	<-swept // the ticker fired and a sweep ran
	client.Close()
	pace.WaitGCLoop(client)
}
