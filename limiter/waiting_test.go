// waiting_test.go covers what happens to a caller while it is blocked on a
// token: the shutdown barrier closing under it, its own context being
// cancelled, and two first-ever requests racing to create the same user.
//
// They reach the engine through the HTTP request path, which is another package
// now, but they belong here rather than beside it. Each one needs a hook only
// export_test.go can install — the window before a caller blocks, and the
// window inside the registry's cold path — and each is asserting on the
// engine's behaviour rather than on the request that provoked it.

package limiter_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

func TestRequest_ErrClosed_WhileWaiting(t *testing.T) {
	// Client with rate=1/min, burst=1: consume the first token then close the
	// pool while the second request is waiting — it must return ErrClosed.
	pool, err := client.New(config.Config{
		BaseURL:  "http://127.0.0.1:1",
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(1), Burst: 1}),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Exhaust the single token.
	if err := pool.Client("u").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	waiting := make(chan struct{})
	limiter.SetBeforeWaitHook(pool.Limiter(), sync.OnceFunc(func() { close(waiting) }))

	errCh := make(chan error, 1)
	go func() {
		// This will block waiting for a token.
		err := pool.Client("u").Wait(ctx)
		errCh <- err
	}()

	<-waiting // the goroutine is genuinely blocked, not merely started
	pool.Close()

	err = <-errCh
	if !errors.Is(err, limiter.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestConcurrentFirstRequestsShareOneUser(t *testing.T) {
	// Verify the double-check path: when two goroutines race to create the same
	// user, the second one finds it already in the shard under the write lock.
	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 100}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// hookReady: goroutine A signals it has released the read lock and is paused.
	// hookDone:  main goroutine signals B has created the user; A can proceed.
	hookReady := make(chan struct{})
	hookDone := make(chan struct{})

	var once sync.Once
	limiter.SetGetOrCreateHook(pool.Limiter(), func() {
		once.Do(func() {
			close(hookReady) // A is about to acquire the write lock
			<-hookDone       // wait until B has already created the user
		})
	})

	raceDone := make(chan struct{})
	go func() {
		defer close(raceDone)
		// Goroutine A: will pause at the hook, then find the user in the
		// double-check (created by main goroutine B below).
		_, _ = pool.Client("race-user").Get(context.Background(), "/")
	}()

	<-hookReady // A released read lock and is paused before write lock

	// Clear the hook so the main goroutine's call doesn't also block.
	limiter.SetGetOrCreateHook(pool.Limiter(), nil)

	// Main goroutine (B): creates "race-user" while A is paused.
	if _, err := pool.Client("race-user").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	close(hookDone) // release A; it will acquire write lock and hit double-check
	<-raceDone      // A finished; the double-check branch has run
}

func TestRequest_CallerCtxCancelledWhileWaiting(t *testing.T) {
	// Cover the `return nil, err` branch in Request: bucket.Wait returns an error
	// AND ctx.Err() is non-nil because the CALLER's context was cancelled while
	// the request was truly blocked (not pre-empted by rate-limiter deadline logic).
	pool, err := client.New(config.Config{
		BaseURL:  "http://127.0.0.1:1",
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(1), Burst: 1}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	// Exhaust the single token.
	if err := pool.Client("u").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	// Use WithCancel (not WithTimeout) so the rate limiter cannot detect the
	// deadline upfront and return early — it will truly block in Wait.
	ctx2, cancel := context.WithCancel(ctx)

	waiting := make(chan struct{})
	limiter.SetBeforeWaitHook(pool.Limiter(), sync.OnceFunc(func() { close(waiting) }))

	errCh := make(chan error, 1)
	go func() {
		err := pool.Client("u").Wait(ctx2)
		errCh <- err
	}()

	<-waiting
	cancel()

	err = <-errCh
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if errors.Is(err, limiter.ErrClosed) {
		t.Fatalf("expected ctx cancellation error, not ErrClosed; got %v", err)
	}
}
