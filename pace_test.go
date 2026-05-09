package pace_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

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

func TestNew_NoEndpoints(t *testing.T) {
	_, err := pace.New(pace.Config{})
	if err == nil {
		t.Fatal("want error for empty endpoints")
	}
}

func TestNew_ZeroRate(t *testing.T) {
	_, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"x": {BaseURL: "http://x", RatePerMinute: 0},
		},
	})
	if err == nil {
		t.Fatal("want error for zero RatePerMinute")
	}
}

func TestGet(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	resp, err := mgr.Get(context.Background(), "u1", "test", "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
	if string(resp.Body()) != "ok" {
		t.Fatalf("want body ok, got %q", resp.Body())
	}
}

func TestRequest_SetHeader(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Trace-ID")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	req, err := mgr.Request(context.Background(), "u1", "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := req.SetHeader("X-Trace-ID", "abc123").Get("/"); err != nil {
		t.Fatal(err)
	}
	if gotHeader != "abc123" {
		t.Fatalf("want abc123, got %q", gotHeader)
	}
}

func TestRequest_Methods(t *testing.T) {
	var gotMethod atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod.Store(r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	for _, tc := range []struct {
		name string
		call func(*pace.Request) (*pace.Response, error)
		want string
	}{
		{"Post", func(r *pace.Request) (*pace.Response, error) { return r.Post("/") }, "POST"},
		{"Put", func(r *pace.Request) (*pace.Response, error) { return r.Put("/") }, "PUT"},
		{"Delete", func(r *pace.Request) (*pace.Response, error) { return r.Delete("/") }, "DELETE"},
		{"Patch", func(r *pace.Request) (*pace.Response, error) { return r.Patch("/") }, "PATCH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := mgr.Request(context.Background(), "u1", "test")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tc.call(req); err != nil {
				t.Fatal(err)
			}
			if m, _ := gotMethod.Load().(string); m != tc.want {
				t.Fatalf("want %s, got %s", tc.want, m)
			}
		})
	}
}

func TestErrUnknownEndpoint(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"real": {BaseURL: "http://x", RatePerMinute: 60},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.Request(context.Background(), "u", "ghost")
	if !errors.Is(err, pace.ErrUnknownEndpoint) {
		t.Fatalf("want ErrUnknownEndpoint, got %v", err)
	}
}

func TestErrClosed(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: "http://x", RatePerMinute: 60},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.Close()

	_, err = mgr.Request(context.Background(), "u", "test")
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

// TestUserIsolation verifies that exhausting one user's bucket does not affect another.
func TestUserIsolation(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	// 1 req/min, burst=1: after one call the user must wait ~60s for the next token.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 1, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Alice consumes her single token.
	if _, err := mgr.Get(ctx, "alice", "test", "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Bob has his own bucket and must not be affected.
	ctxBob, cancelBob := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelBob()
	if _, err := mgr.Get(ctxBob, "bob", "test", "/"); err != nil {
		t.Fatalf("bob (isolated): %v", err)
	}

	// Alice is throttled — her second call must time out.
	ctxAlice, cancelAlice := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelAlice()
	if _, err := mgr.Get(ctxAlice, "alice", "test", "/"); err == nil {
		t.Fatal("alice second call should have been throttled")
	}
}

func TestContextCancellation(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			// 1/min so the second request blocks for ~60s.
			"test": {BaseURL: "http://127.0.0.1:0", RatePerMinute: 1, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Exhaust the token (no HTTP call needed — Request() just waits for a token).
	if _, err := mgr.Request(ctx, "u", "test"); err != nil {
		t.Fatal(err)
	}

	// Second request should return when context times out.
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = mgr.Request(ctx2, "u", "test")
	if err == nil {
		t.Fatal("want error from cancelled context")
	}
}

func TestMultipleEndpoints(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"a": {BaseURL: srv.URL, RatePerMinute: 6000},
			"b": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ctx := context.Background()
	for _, ep := range []string{"a", "b"} {
		if _, err := mgr.Get(ctx, "u", ep, "/"); err != nil {
			t.Fatalf("endpoint %s: %v", ep, err)
		}
	}
}

func TestConcurrentUsers(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	const n = 20
	errs := make(chan error, n)
	ctx := context.Background()
	for i := range n {
		go func(id int) {
			_, err := mgr.Get(ctx, fmt.Sprintf("user-%d", id), "test", "/")
			errs <- err
		}(i)
	}
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("concurrent call: %v", err)
		}
	}
}

func TestStoreCreatesFile(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pace.db")

	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Get(context.Background(), "alice", "test", "/"); err != nil {
		t.Fatal(err)
	}
	mgr.Close()

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file not created: %v", err)
	}
}

// TestStorePersistenceThrottles checks that token state persists across Manager restarts.
// A very low rate (6/min = 1 token per 10s) ensures the gap between close and
// re-open is too small to restore even one token.
func TestStorePersistenceThrottles(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pace.db")

	srv := newEchoServer(t)
	defer srv.Close()

	cfg := pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6, Burst: 1},
		},
		DBPath: dbPath,
	}

	// mgr1: consume Alice's single token then close (persists ≈0 tokens).
	mgr1, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr1.Get(context.Background(), "alice", "test", "/"); err != nil {
		t.Fatalf("mgr1 alice: %v", err)
	}
	mgr1.Close()

	// mgr2: restore from DB — Alice should still be throttled.
	mgr2, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := mgr2.Get(ctx, "alice", "test", "/"); err == nil {
		t.Fatal("alice should still be throttled after restore")
	}
}

func TestNew_EmptyBaseURL(t *testing.T) {
	_, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"bad": {BaseURL: "", RatePerMinute: 60},
		},
	})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
	}
}

func TestRequest_SetBody(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = b
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	req, err := mgr.Request(context.Background(), "u1", "test")
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"hello":"world"}`)
	if _, err := req.SetBody(payload).Post("/"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("want body %q, got %q", payload, gotBody)
	}
}

func TestResponse_StatusAndHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Custom", "hello")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	resp, err := mgr.Get(context.Background(), "u1", "test", "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusCreated {
		t.Fatalf("want 201, got %d", resp.StatusCode())
	}
	if resp.Status() != "201 Created" {
		t.Fatalf("want '201 Created', got %q", resp.Status())
	}
	if resp.Header().Get("X-Custom") != "hello" {
		t.Fatalf("want header X-Custom=hello, got %q", resp.Header().Get("X-Custom"))
	}
}

func TestGC_EvictsIdleUser(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			// burst=1, rate=1/min: alice's token is exhausted after one call
			"test": {BaseURL: srv.URL, RatePerMinute: 1, Burst: 1},
		},
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ctx := context.Background()

	// Alice uses her single token.
	if _, err := mgr.Get(ctx, "alice", "test", "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Alice is now throttled — second call times out.
	ctxShort, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := mgr.Get(ctxShort, "alice", "test", "/"); err == nil {
		t.Fatal("alice should be throttled before GC")
	}

	// Advance clock past IdleExpiry and run GC.
	clock.advance(10 * time.Minute)
	pace.CollectIdle(mgr)

	// Alice's bucket is evicted and re-created fresh → burst=1 available again.
	ctxFresh, cancelFresh := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelFresh()
	if _, err := mgr.Get(ctxFresh, "alice", "test", "/"); err != nil {
		t.Fatalf("alice after GC eviction: %v", err)
	}
}

func TestGC_SavesStateOnEvict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pace.db")
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
		DBPath:     dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if _, err := mgr.Get(context.Background(), "alice", "test", "/"); err != nil {
		t.Fatalf("alice: %v", err)
	}

	clock.advance(10 * time.Minute)
	pace.CollectIdle(mgr)

	// DB file must exist and contain alice's record.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db not found after GC: %v", err)
	}
}

func TestErrClosed_Concurrent(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"test": {BaseURL: "http://127.0.0.1:0", RatePerMinute: 6000, Burst: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n + 1)

	go func() {
		defer wg.Done()
		mgr.Close()
	}()
	for i := range n {
		go func(id int) {
			defer wg.Done()
			_, _ = mgr.Request(context.Background(), fmt.Sprintf("u%d", id), "test")
		}(i)
	}
	wg.Wait()
}
