// persistence_test.go is what pace does with a caller's store, end to end
// through a Limiter: the bounded context every call gets, the batch path when
// the backend offers one, the flush at shutdown, and what happens when any of
// it fails.
//
// It was statestore_test.go, named after an interface since renamed. The
// contract itself is not tested here and must not be — store/storetest is the
// executable contract, and it asserts against a backend rather than against a
// Limiter. This file asserts the policy over one.

package limiter_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/store"
	"github.com/jaeminst/pace/store/memory"
)

// ctxStore records the contexts it is handed, which is the whole point of the
// interface change: a backend that talks over a network needs a context to
// honour, and the previous signature gave it none.
type ctxStore struct {
	mu        sync.Mutex
	loadCtx   context.Context
	saveCtx   context.Context
	batchCtx  context.Context
	saveCount int
	batchRuns int
	batchSize int
	// saveErrAtCall and batchErrAtCall record ctx.Err() as observed inside the
	// call. Inspecting the stored context afterwards would always report
	// Canceled, because the caller cancels it as soon as the call returns.
	saveErrAtCall  error
	batchErrAtCall error
}

func (s *ctxStore) Save(ctx context.Context, _ string, _ store.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCtx = ctx
	s.saveErrAtCall = ctx.Err()
	s.saveCount++
	return nil
}

func (s *ctxStore) Load(ctx context.Context, _ string) (store.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadCtx = ctx
	return store.State{}, false, nil
}

func (s *ctxStore) Close() error { return nil }

func (s *ctxStore) snapshot() (load, save context.Context, saves int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadCtx, s.saveCtx, s.saveCount
}

// batchCtxStore adds the optional batch extension.
type batchCtxStore struct{ ctxStore }

func (s *batchCtxStore) SaveBatch(ctx context.Context, states []store.KeyState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchCtx = ctx
	s.batchErrAtCall = ctx.Err()
	s.batchRuns++
	s.batchSize += len(states)
	return nil
}

func TestAStoreReceivesABoundedContext(t *testing.T) {
	st := &ctxStore{}
	lim, _ := newTestLimiter(t, func(c *config.Config) {
		c.Store = st
		c.StoreTimeout = 2 * time.Second
	})

	if _, err := lim.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	loadCtx, _, _ := st.snapshot()
	if loadCtx == nil {
		t.Fatal("Load received no context")
	}
	deadline, ok := loadCtx.Deadline()
	if !ok {
		t.Error("Load's context carries no deadline, so StoreTimeout is not applied")
	} else if until := time.Until(deadline); until <= 0 || until > 2*time.Second {
		t.Errorf("Load deadline is %v away, want within (0, 2s]", until)
	}
}

// TestStoreTimeoutDegradesGracefully covers what StoreTimeout is for: a
// wedged backend must not wedge the request. A key whose state cannot be
// loaded starts from a fresh bucket rather than failing.
func TestStoreTimeoutDegradesGracefully(t *testing.T) {
	st := &hangingStore{entered: make(chan struct{}, 1)}
	lim, _ := newTestLimiter(t, func(c *config.Config) {
		c.Store = st
		c.StoreTimeout = 100 * time.Millisecond
	})

	done := make(chan error, 1)
	go func() {
		_, err := lim.Client("alice").Get(context.Background(), "/")
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("request failed because the store hung: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("request never returned: StoreTimeout did not bound the load")
	}
}

// hangingStore blocks until its context is cancelled, which only happens if a
// deadline is actually attached.
type hangingStore struct {
	entered chan struct{}
}

func (s *hangingStore) Save(ctx context.Context, _ string, _ store.State) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *hangingStore) Load(ctx context.Context, _ string) (store.State, bool, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return store.State{}, false, ctx.Err()
}

func (s *hangingStore) Close() error { return nil }

// TestABatchStoreIsPreferred proves the optional extension is detected: a
// sweep evicting many keys must reach the backend once, not once per key.
func TestABatchStoreIsPreferred(t *testing.T) {
	st := &batchCtxStore{}
	clk := newFakeClock()
	lim, _ := newTestLimiter(t, func(c *config.Config) {
		c.Store = st
		c.IdleExpiry = time.Minute
		c.Clock = clk
	})

	ctx := context.Background()
	const keys = 20
	for i := range keys {
		if _, err := lim.Client(fmt.Sprintf("user-%d", i)).Get(ctx, "/"); err != nil {
			t.Fatal(err)
		}
	}
	clk.advance(time.Hour)

	limiter.CollectIdle(lim.Limiter())

	st.mu.Lock()
	runs, size, saves := st.batchRuns, st.batchSize, st.saveCount
	batchCtx := st.batchCtx
	st.mu.Unlock()

	if runs == 0 {
		t.Fatal("SaveBatch was never called; the batch extension was not detected")
	}
	if saves != 0 {
		t.Errorf("per-key Save was called %d times despite SaveBatch being available", saves)
	}
	if size != keys {
		t.Errorf("SaveBatch received %d keys across %d calls, want %d", size, runs, keys)
	}
	if batchCtx == nil {
		t.Error("SaveBatch received no context")
	} else if _, ok := batchCtx.Deadline(); !ok {
		t.Error("SaveBatch's context carries no deadline")
	}
}

// TestAPlainStoreFallsBackToSave is the other half: a store that does not
// implement the extension must still be driven correctly, one key at a time.
func TestAPlainStoreFallsBackToSave(t *testing.T) {
	st := &ctxStore{}
	clk := newFakeClock()
	lim, _ := newTestLimiter(t, func(c *config.Config) {
		c.Store = st
		c.IdleExpiry = time.Minute
		c.Clock = clk
	})

	ctx := context.Background()
	const keys = 5
	for i := range keys {
		if _, err := lim.Client(fmt.Sprintf("user-%d", i)).Get(ctx, "/"); err != nil {
			t.Fatal(err)
		}
	}
	clk.advance(time.Hour)

	limiter.CollectIdle(lim.Limiter())

	_, saveCtx, saves := st.snapshot()
	if saves != keys {
		t.Errorf("Save called %d times, want %d", saves, keys)
	}
	if saveCtx == nil {
		t.Fatal("Save received no context")
	}
	if _, ok := saveCtx.Deadline(); !ok {
		t.Error("Save's context carries no deadline")
	}
}

// TestFinalFlushSurvivesLimiterCancellation guards a subtlety in the shutdown
// sequence: Close cancels the Limiter's context before flushing, so a flush
// that inherited that context would save nothing at exactly the moment saving
// matters most.
func TestFinalFlushSurvivesLimiterCancellation(t *testing.T) {
	st := &ctxStore{}
	lim, _ := newTestLimiter(t, func(c *config.Config) { c.Store = st })

	if _, err := lim.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	st.mu.Lock()
	saves, errAtCall := st.saveCount, st.saveErrAtCall
	st.mu.Unlock()
	if saves == 0 {
		t.Fatal("Close flushed nothing")
	}
	if errAtCall != nil {
		t.Errorf("the final flush ran on an already-dead context: %v", errAtCall)
	}
}

// twoMethodStore implements store.Store and nothing else — no Close. That it
// compiles at all is the point of narrowing the interface: it used to force
// every implementation to carry a Close whether it had resources or not, and the
// README's own example wrote one that returned nil because the interface
// demanded it.
type twoMethodStore struct {
	mu    sync.Mutex
	saves int
}

func (s *twoMethodStore) Save(context.Context, string, store.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saves++
	return nil
}

func (s *twoMethodStore) Load(context.Context, string) (store.State, bool, error) {
	return store.State{}, false, nil
}

func TestAStoreNeedsNoClose(t *testing.T) {
	var _ store.Store = (*twoMethodStore)(nil)

	st := &twoMethodStore{}
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(600), Burst: 10},
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}

	lim.Client("alice").Allow(context.Background())
	if err := lim.Close(); err != nil {
		t.Fatalf("Close = %v, want nil for a store that cannot be closed", err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if st.saves == 0 {
		t.Error("the store was never written to")
	}
}

// closableStore records that pace closed it, which is the behaviour
// Config.Store now documents rather than leaves for a caller to discover.
type closableStore struct {
	twoMethodStore
	closed bool
}

func (s *closableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func TestAStoreIsClosedWhenItImplementsCloser(t *testing.T) {
	st := &closableStore{}
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(600), Burst: 10},
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if !st.closed {
		t.Error("pace did not close a Store that implements io.Closer")
	}
}

func TestStoreReceivesTheFinalFlush(t *testing.T) {
	st := memory.New()

	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6000)},
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	pool.Close()

	if n := st.Len(); n == 0 {
		t.Fatal("Close flushed nothing to the store")
	}
}

// TestStorePersistenceThrottles checks that token state persists across Client restarts.
// A very low rate (6/min = 1 token per 10s) ensures the gap between close and
// re-open is too small to restore even one token.
func TestStorePersistenceThrottles(t *testing.T) {
	st := memory.New()

	srv := newEchoServer(t)
	defer srv.Close()

	cfg := config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6), Burst: 1},
		Store:   st,
	}

	// client1: consume Alice's single token then close (persists ≈0 tokens).
	client1, err := client.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client1.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatalf("client1 alice: %v", err)
	}
	client1.Close()

	// client2: restore from DB — Alice should still be throttled.
	client2, err := client.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := client2.Client("alice").Get(ctx, "/"); err == nil {
		t.Fatal("alice should still be throttled after restore")
	}
}

func TestSaveAll_StoreError(t *testing.T) {
	// Already covered by TestClose_StoreError which closes the db before Close().
	// This explicit test triggers saveAll via GC eviction with a broken store,
	// exercising the warn path in saveAll independently.
	srv := newEchoServer(t)
	defer srv.Close()
	st := newBreakableStore()

	clock := newFakeClock()
	pool, err := client.New(config.Config{
		BaseURL:    srv.URL,
		Quota:      bucket.Quota{Rate: bucket.PerMinute(6000)},
		Store:      st,
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	limiter.CloseLimiterStore(pool.Limiter())

	// Advance past idle expiry and trigger GC — saveAll would be called on Close,
	// but evictUser (which calls store.Save) is exercised here via CollectIdle.
	clock.advance(10 * time.Minute)
	limiter.CollectIdle(pool.Limiter()) // evictUser → store.Save fails → warn
}

func TestCustomStore_LoadError(t *testing.T) {
	// Config.Store.Load returns an error — wrapper must propagate it; Client
	// logs a warning and falls back to a fresh bucket.
	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6000)},
		Store:   &errLoadStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Must not panic; the load error is logged and a fresh bucket is used.
	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

// The two below arrived from a file called config_test.go that held no Config
// test at all. They are about a custom Store on the cold path, which is this
// file's subject.
func TestNew_CustomStore_NoopLoad(t *testing.T) {
	// Config.Store with a no-op backend: entryFor calls Load on it.
	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 5},
		Store:   &noopStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestNew_CustomStore_WithSavedState(t *testing.T) {
	// Config.Store returns saved state so the wrapper.Load conversion path runs.
	srv := newEchoServer(t)
	defer srv.Close()

	now := time.Now()
	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 3},
		Store: &savedStateStore{state: store.State{
			Tokens: 1.5, LastUsed: now,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	// Key is loaded from the custom store — should have tokens available.
	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}
