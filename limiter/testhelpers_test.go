package limiter_test

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	_ "modernc.org/sqlite" // direct DB access for planting queue rows

	"github.com/jaeminst/pace/limit"
	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/response"
	"github.com/jaeminst/pace/store"
)

// durableDo runs a durable request end to end.
//
// It used to exist to fold Durable's second return value into the result.
// Durable is chainable now, so this is a one-liner kept for the call sites that
// read better without the builder spelled out.
func durableDo(ctx context.Context, c *pace.Client, id, method, path string) (*response.Response, error) {
	return c.Durable(id).Do(ctx, method, path)
}

// migrateDB creates the schema at path by opening and closing a Limiter, so
// tests that plant rows directly do not duplicate the schema definition.
//
// It skips an existing file. That is not just an optimisation: a Limiter with a
// queue configured drains it on startup, so migrating again after seeding would
// try to deliver the very jobs the test is about to set up.
func migrateDB(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		return
	}
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    limit.PerMinute(60),
		DBPath:  path,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(lim)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
}

func openRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedQueuedJob plants a job that was persisted but never dispatched — the
// unambiguous case, where replaying is simply correct.
func seedQueuedJob(t *testing.T, path, id, userID, method, reqPath string) {
	t.Helper()
	migrateDB(t, path)
	db := openRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO pending_jobs (id, user_id, method, path, headers, body, created_at, state, attempts, next_attempt_at)
		VALUES (?, ?, ?, ?, '{}', NULL, ?, 'queued', 0, 0)
	`, id, userID, method, reqPath, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
}

// strandSendingJob plants a job left mid-flight: its intent to send was
// committed, then the process died before the outcome was recorded. The lease
// is already expired, as it would be after a restart.
func strandSendingJob(t *testing.T, path, id, userID, method, reqPath string) {
	t.Helper()
	migrateDB(t, path)
	db := openRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO pending_jobs (id, user_id, method, path, headers, body, created_at, state, attempts, next_attempt_at, lease_until, owner)
		VALUES (?, ?, ?, ?, '{}', NULL, ?, 'sending', 1, 0, 1, 'dead-process')
	`, id, userID, method, reqPath, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
}

// tokensOf reports a user's token count, for assertions that do not care
// whether the user has in-memory state.
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
