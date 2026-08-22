package limiter_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/store"
)

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
// database has gone away are otherwise unreachable: pace.CloseLimiterStore
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

func tokensOf(c *pace.Client) float64 {
	n, _ := c.Tokens()
	return n
}

// evict is Evict for tests that only care whether the user was present.
func evict(t *testing.T, c *pace.Client) bool {
	t.Helper()
	present, err := c.Evict(context.Background())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	return present
}

// fakeClock is an injectable Clock whose Now() can be advanced.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(0, 0)} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
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

// --- 100% coverage tests ---

// failTransport is an http.RoundTripper that always returns an error.
type failTransport struct{ err error }

func (f failTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, f.err }

// errBodyTransport returns a 200 response whose body errors on Read.
type errBodyTransport struct{}

func (errBodyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: 200,
		Status:     "200 OK",
		Header:     make(http.Header),
		Body:       io.NopCloser(&errReader{}),
		Request:    r,
	}, nil
}

type errReader struct{}

func (*errReader) Read([]byte) (int, error) { return 0, errors.New("body read error") }

// mockCloseErrStore implements StateStore but returns an error on Close.
type mockCloseErrStore struct{}

func (m *mockCloseErrStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (m *mockCloseErrStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return store.State{}, false, nil
}
func (m *mockCloseErrStore) Close() error { return errors.New("mock close error") }

// --- StateStore (pluggable backend) tests ---

// noopStore is a StateStore that always succeeds and returns no saved state.
type noopStore struct{}

func (s *noopStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (s *noopStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return store.State{}, false, nil
}
func (s *noopStore) Close() error { return nil }

// loadStateStore returns predefined saved state so RestoreBucket is exercised.
type loadStateStore struct{ state store.State }

func (s *loadStateStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (s *loadStateStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return s.state, true, nil
}
func (s *loadStateStore) Close() error { return nil }

// errLoadStore causes Load to return an error.
type errLoadStore struct{}

func (s *errLoadStore) Save(_ context.Context, _ string, _ store.State) error { return nil }
func (s *errLoadStore) Load(_ context.Context, _ string) (store.State, bool, error) {
	return store.State{}, false, errors.New("load failed")
}
func (s *errLoadStore) Close() error { return nil }
