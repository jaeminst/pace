package limiter_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/store/memory"
)

// TestUserIsolation verifies that exhausting one user's bucket does not affect another.
func TestUserIsolation(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	// 1 req/min, burst=1: after one call the user must wait ~60s for the next token.
	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Alice consumes her single token.
	if _, err := pool.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Bob has his own bucket and must not be affected.
	ctxBob, cancelBob := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelBob()
	if _, err := pool.Client("bob").Get(ctxBob, "/"); err != nil {
		t.Fatalf("bob (isolated): %v", err)
	}

	// Alice is throttled — her second call must time out.
	ctxAlice, cancelAlice := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelAlice()
	if _, err := pool.Client("alice").Get(ctxAlice, "/"); err == nil {
		t.Fatal("alice second call should have been throttled")
	}
}

func TestGC_EvictsIdleUser(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	pool, err := client.New(config.Config{
		// burst=1, rate=1/min: alice's token is exhausted after one call
		BaseURL:    srv.URL,
		Rate:       bucket.PerMinute(1),
		Burst:      1,
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()

	// Alice uses her single token.
	if _, err := pool.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Alice is now throttled — second call times out.
	ctxShort, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := pool.Client("alice").Get(ctxShort, "/"); err == nil {
		t.Fatal("alice should be throttled before GC")
	}

	// Advance clock past IdleExpiry and run GC.
	clock.advance(10 * time.Minute)
	limiter.CollectIdle(pool.Limiter())

	// Alice's bucket is evicted and re-created fresh → burst=1 available again.
	ctxFresh, cancelFresh := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelFresh()
	if _, err := pool.Client("alice").Get(ctxFresh, "/"); err != nil {
		t.Fatalf("alice after GC eviction: %v", err)
	}
}

func TestGC_SavesStateOnEvict(t *testing.T) {
	st := memory.New()
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	pool, err := client.New(config.Config{
		BaseURL:    srv.URL,
		Rate:       bucket.PerMinute(6000),
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
		Store:      st,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatalf("alice: %v", err)
	}

	clock.advance(10 * time.Minute)
	limiter.CollectIdle(pool.Limiter())

	// The evicted user's state must have reached the store.
	if n := st.Len(); n == 0 {
		t.Fatal("the store holds nothing after a GC eviction")
	}
}

func TestEvict_RemovesUser(t *testing.T) {
	srv := newEchoServer(t)
	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if !evict(t, pool.Client("alice")) {
		t.Fatal("expected Evict to return true for existing user")
	}
	n, ok := pool.Client("alice").Tokens()
	if ok || n != 0 {
		t.Fatalf("Tokens() after Evict = (%v, %v), want (0, false)", n, ok)
	}
}

func TestEvict_ReturnsFalseForUnknownUser(t *testing.T) {
	pool, err := client.New(config.Config{
		BaseURL: "http://127.0.0.1:0",
		Rate:    bucket.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if evict(t, pool.Client("ghost")) {
		t.Fatal("expected Evict to return false for unknown user")
	}
}

func TestEvict_SavesToDB(t *testing.T) {
	srv := newEchoServer(t)
	st := memory.New()
	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(60),
		Burst:   3,
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	tokensBefore := tokensOf(pool.Client("alice"))
	evict(t, pool.Client("alice"))

	// Re-open a new pool: alice's tokens should be restored from DB
	client2, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(60),
		Burst:   3,
		Store:   st,
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
	pool, err := client.New(config.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    bucket.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	// WaitGCLoop blocks until the gcLoop goroutine exits via ctx.Done().
	limiter.WaitGCLoop(pool.Limiter())
}

func TestAStoreLoadFailureStillServesTheUser(t *testing.T) {
	// Close the store before creating a new user — userFor must log the load
	// error and continue with a fresh bucket.
	srv := newEchoServer(t)
	defer srv.Close()
	st := newBreakableStore()

	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(6000),
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Break the store, then try to create a brand-new user.
	limiter.CloseLimiterStore(pool.Limiter())

	// Should not panic; logger.Warn is called internally.
	if _, err := pool.Client("new-user-after-close").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestEvict_StoreError(t *testing.T) {
	// Break the store, then evict a user — evictUser must log the save error.
	srv := newEchoServer(t)
	defer srv.Close()
	st := newBreakableStore()

	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(6000),
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	limiter.CloseLimiterStore(pool.Limiter())

	// The store is broken, so persisting fails. Evict reports that rather than
	// swallowing it into a log line: the caller asked for this write.
	present, err := pool.Client("alice").Evict(context.Background())
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
	pool, err := client.New(config.Config{
		BaseURL:    "http://127.0.0.1:1",
		Rate:       bucket.PerMinute(60),
		GCInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	swept := make(chan struct{})
	limiter.SetAfterSweepHook(pool.Limiter(), sync.OnceFunc(func() { close(swept) }))
	<-swept // the ticker fired and a sweep ran
	pool.Close()
	limiter.WaitGCLoop(pool.Limiter())
}
