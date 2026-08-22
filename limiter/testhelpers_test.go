package limiter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/store"
)

// build resolves cfg, applies the options, and returns a Limiter that closes
// itself when the test ends.
//
// Every limiter fixture in this package ends the same eight lines: apply the
// options, call pace.New, fail on the error, register the Close. They differ
// only in the Config they start from, which is what each of them is actually
// for.
//
// It goes through pace.New rather than limiter.New because that is what
// resolves a Config into a Spec, and a test that hand-wrote the Spec would be
// asserting against its own defaulting rather than the library's.
func build(t *testing.T, cfg config.Config, opts ...func(*config.Config)) *client.Pool {
	t.Helper()
	for _, o := range opts {
		o(&cfg)
	}
	lim, err := client.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	return lim
}

// waitFor polls until cond holds, or fails the test. Used only where the thing
// being waited on is a background goroutine reaching a state, which no channel
// in the public API exposes.
//
// The sleep here is a poll interval, not a synchronisation primitive: the test
// still fails if cond never holds, and never passes because the timing happened
// to work out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// breakableStore is an in-memory store that starts failing once it is closed.
//
// It exists because the error paths a live backend only produces when its
// database has gone away are otherwise unreachable: limiter.CloseLimiterStore
// closes whatever store is installed, and a plain map has nothing to break.
type breakableStore struct {
	mu     sync.Mutex
	state  map[string]store.State
	closed bool
}

func newBreakableStore() *breakableStore {
	return &breakableStore{state: make(map[string]store.State)}
}

func (s *breakableStore) Save(_ context.Context, userID string, st store.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("store is closed")
	}
	s.state[userID] = st
	return nil
}

func (s *breakableStore) Load(_ context.Context, userID string) (store.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return store.State{}, false, errors.New("store is closed")
	}
	st, ok := s.state[userID]
	return st, ok, nil
}

func (s *breakableStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func tokensOf(c *client.Client) float64 {
	n, _ := c.Tokens()
	return n
}

// evict is Evict for tests that only care whether the user was present.
func evict(t *testing.T, c *client.Client) bool {
	t.Helper()
	present, err := c.Evict(context.Background())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	return present
}

// fakeClock is an injectable Clock whose Now() can be advanced.
type fakeClock struct {
	mu    sync.Mutex
	now   time.Time
	calls int
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(0, 0)} }

func (c *fakeClock) callCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func newEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Method", r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
}

// mockCloseErrStore implements store.Store but returns an error on Close.
type mockCloseErrStore struct{}

func (m *mockCloseErrStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (m *mockCloseErrStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return store.State{}, false, nil
}
func (m *mockCloseErrStore) Close() error { return errors.New("mock close error") }

// --- store fakes, each for one path a real backend only reaches when it
// has broken ---

// noopStore is a store.Store that always succeeds and returns no saved state.
type noopStore struct{}

func (s *noopStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (s *noopStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return store.State{}, false, nil
}
func (s *noopStore) Close() error { return nil }

// savedStateStore hands back state as though a previous run had persisted it,
// which is the only way to reach the restore path.
type savedStateStore struct{ state store.State }

func (s *savedStateStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (s *savedStateStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return s.state, true, nil
}
func (s *savedStateStore) Close() error { return nil }

// errLoadStore causes Load to return an error.
type errLoadStore struct{}

func (s *errLoadStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (s *errLoadStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return store.State{}, false, errors.New("load failed")
}
func (s *errLoadStore) Close() error { return nil }
