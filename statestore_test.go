package pace_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
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
	// errAtCall records ctx.Err() as observed inside the call. Inspecting the
	// stored context afterwards would always report Canceled, because the
	// caller cancels it as soon as the call returns.
	saveErrAtCall  error
	batchErrAtCall error
}

func (s *ctxStore) Save(ctx context.Context, _ string, _ pace.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveCtx = ctx
	s.saveErrAtCall = ctx.Err()
	s.saveCount++
	return nil
}

func (s *ctxStore) Load(ctx context.Context, _ string) (pace.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadCtx = ctx
	return pace.State{}, false, nil
}

func (s *ctxStore) Close() error { return nil }

func (s *ctxStore) snapshot() (load, save context.Context, saves int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.loadCtx, s.saveCtx, s.saveCount
}

// batchCtxStore adds the optional batch extension.
type batchCtxStore struct{ ctxStore }

func (s *batchCtxStore) SaveBatch(ctx context.Context, states []pace.UserState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.batchCtx = ctx
	s.batchErrAtCall = ctx.Err()
	s.batchRuns++
	s.batchSize += len(states)
	return nil
}

func TestStateStoreReceivesBoundedContext(t *testing.T) {
	st := &ctxStore{}
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
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

// TestStateStoreTimeoutDegradesGracefully covers what StoreTimeout is for: a
// wedged backend must not wedge the request. A user whose state cannot be
// loaded starts from a fresh bucket rather than failing.
func TestStateStoreTimeoutDegradesGracefully(t *testing.T) {
	st := &hangingStore{entered: make(chan struct{}, 1)}
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
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

func (s *hangingStore) Save(ctx context.Context, _ string, _ pace.State) error {
	<-ctx.Done()
	return ctx.Err()
}

func (s *hangingStore) Load(ctx context.Context, _ string) (pace.State, bool, error) {
	select {
	case s.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return pace.State{}, false, ctx.Err()
}

func (s *hangingStore) Close() error { return nil }

// TestBatchStateStoreIsPreferred proves the optional extension is detected: a
// sweep evicting many users must reach the backend once, not once per user.
func TestBatchStateStoreIsPreferred(t *testing.T) {
	st := &batchCtxStore{}
	clk := newFakeClock()
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Store = st
		c.IdleExpiry = time.Minute
		c.Clock = clk
	})

	ctx := context.Background()
	const users = 20
	for i := range users {
		if _, err := lim.Client(fmt.Sprintf("user-%d", i)).Get(ctx, "/"); err != nil {
			t.Fatal(err)
		}
	}
	clk.advance(time.Hour)

	pace.CollectIdle(lim)

	st.mu.Lock()
	runs, size, saves := st.batchRuns, st.batchSize, st.saveCount
	batchCtx := st.batchCtx
	st.mu.Unlock()

	if runs == 0 {
		t.Fatal("SaveBatch was never called; the batch extension was not detected")
	}
	if saves != 0 {
		t.Errorf("per-user Save was called %d times despite SaveBatch being available", saves)
	}
	if size != users {
		t.Errorf("SaveBatch received %d users across %d calls, want %d", size, runs, users)
	}
	if batchCtx == nil {
		t.Error("SaveBatch received no context")
	} else if _, ok := batchCtx.Deadline(); !ok {
		t.Error("SaveBatch's context carries no deadline")
	}
}

// TestPlainStateStoreFallsBackToSave is the other half: a store that does not
// implement the extension must still be driven correctly, one user at a time.
func TestPlainStateStoreFallsBackToSave(t *testing.T) {
	st := &ctxStore{}
	clk := newFakeClock()
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Store = st
		c.IdleExpiry = time.Minute
		c.Clock = clk
	})

	ctx := context.Background()
	const users = 5
	for i := range users {
		if _, err := lim.Client(fmt.Sprintf("user-%d", i)).Get(ctx, "/"); err != nil {
			t.Fatal(err)
		}
	}
	clk.advance(time.Hour)

	pace.CollectIdle(lim)

	_, saveCtx, saves := st.snapshot()
	if saves != users {
		t.Errorf("Save called %d times, want %d", saves, users)
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
	lim, _ := newTestLimiter(t, func(c *pace.Config) { c.Store = st })

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

// TestStoreAndDBPathCoexist covers the configuration New used to reject
// outright. The two fields persist different things — Store owns per-user
// token state, DBPath owns the durable queue — so forbidding both meant a
// caller with the Redis or Postgres backend the README advertises could never
// have a durable queue at all, and got no signal saying so.
func TestStoreAndDBPathCoexist(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := &recordingStore{}
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		Store:   st,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatalf("New with both Store and DBPath = %v, want nil", err)
	}
	pace.WaitReplay(lim)

	// The queue works, which is the whole point.
	resp, err := durableDo(context.Background(), lim.Client("alice"), "job-1", http.MethodPost, "/pay")
	if err != nil {
		t.Fatalf("durable request = %v, want nil", err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode())
	}

	if err := lim.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}

	// The custom Store took the user state, and SQLite's user_state stayed out
	// of it.
	if st.saveCount() == 0 {
		t.Error("the custom Store was never written to; SQLite took the user state")
	}
	if n := userStateRows(t, dbPath); n != 0 {
		t.Errorf("user_state holds %d rows, want 0: Store owns token state", n)
	}
}

// TestStoreAndDBPathBothClose guards the hazard the coexistence creates: two
// separate handles where there used to be one. Closing only l.store would leak
// the SQLite file, which on Windows surfaces as a t.TempDir cleanup failure
// rather than as anything that names the cause.
func TestStoreAndDBPathBothClose(t *testing.T) {
	st := &recordingStore{}
	dbPath := filepath.Join(t.TempDir(), "queue.db")
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(600),
		Burst:   10,
		Store:   st,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(lim)
	if err := lim.Close(); err != nil {
		t.Fatalf("Close = %v, want nil", err)
	}

	if !st.isClosed() {
		t.Error("the custom Store was not closed")
	}
	// A closed SQLite handle is what lets the file be removed. If it is still
	// open, this fails on Windows and passes on Linux — so assert it directly.
	if err := os.Remove(dbPath); err != nil {
		t.Errorf("the SQLite file could not be removed after Close, so its handle leaked: %v", err)
	}
}

// userStateRows counts rows in the SQLite user_state table.
func userStateRows(t *testing.T, dbPath string) int {
	t.Helper()
	db := openRawDB(t, dbPath)
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM user_state`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
