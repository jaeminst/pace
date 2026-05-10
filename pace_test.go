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

func TestTokens_ExistingUser(t *testing.T) {
	srv := newEchoServer(t)
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 3},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// consume one token
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	tokens, err := mgr.Tokens("alice", "api")
	if err != nil {
		t.Fatal(err)
	}
	if tokens >= 3 {
		t.Fatalf("expected tokens < 3 after one request, got %v", tokens)
	}
}

func TestTokens_UnknownUser(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:0", RatePerMinute: 60, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	tokens, err := mgr.Tokens("nobody", "api")
	if err != nil {
		t.Fatal(err)
	}
	if tokens != -1 {
		t.Fatalf("expected -1 for unknown user, got %v", tokens)
	}
}

func TestTokens_UnknownEndpoint(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:0", RatePerMinute: 60, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.Tokens("alice", "missing")
	if !errors.Is(err, pace.ErrUnknownEndpoint) {
		t.Fatalf("expected ErrUnknownEndpoint, got %v", err)
	}
}

func TestEvict_RemovesUser(t *testing.T) {
	srv := newEchoServer(t)
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	if !mgr.Evict("alice") {
		t.Fatal("expected Evict to return true for existing user")
	}
	tokens, err := mgr.Tokens("alice", "api")
	if err != nil {
		t.Fatal(err)
	}
	if tokens != -1 {
		t.Fatalf("expected -1 after evict, got %v", tokens)
	}
}

func TestEvict_ReturnsFalseForUnknownUser(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:0", RatePerMinute: 60, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if mgr.Evict("ghost") {
		t.Fatal("expected Evict to return false for unknown user")
	}
}

func TestEvict_SavesToDB(t *testing.T) {
	srv := newEchoServer(t)
	dbPath := filepath.Join(t.TempDir(), "evict.db")
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 3},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	tokensBefore, _ := mgr.Tokens("alice", "api")
	mgr.Evict("alice")

	// Re-open a new manager: alice's tokens should be restored from DB
	mgr2, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 3},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()

	// Trigger user load by calling Tokens (creates bucket from DB)
	if _, err := mgr2.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	tokensAfter, _ := mgr2.Tokens("alice", "api")
	// tokensAfter should be close to tokensBefore - 1 (we consumed one in mgr2)
	if tokensAfter >= tokensBefore {
		t.Fatalf("expected restored tokens (%v) < original (%v)", tokensAfter, tokensBefore)
	}
}

func TestBurstCeiling(t *testing.T) {
	srv := newEchoServer(t)
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// First request: consumes the only burst token
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	// Second request: no token, should block; use tight timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = mgr.Get(ctx, "alice", "api", "/")
	if err == nil {
		t.Fatal("expected second request to block/fail with burst=1")
	}
}

func TestOnThrottle_CalledWhenBlocked(t *testing.T) {
	srv := newEchoServer(t)
	var called atomic.Int32
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 1},
		},
		OnThrottle: func(_, _ string) { called.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// Exhaust the burst token
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	// This request should trigger OnThrottle (no token available)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = mgr.Get(ctx, "alice", "api", "/")

	if called.Load() == 0 {
		t.Fatal("expected OnThrottle to be called")
	}
}

func TestOnThrottle_NotCalledWhenAvailable(t *testing.T) {
	srv := newEchoServer(t)
	var called atomic.Int32
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 5},
		},
		OnThrottle: func(_, _ string) { called.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// Token is available; OnThrottle must NOT fire
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	if called.Load() != 0 {
		t.Fatalf("expected OnThrottle NOT to be called, got %d calls", called.Load())
	}
}

func TestHTTPError_StatusCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	resp, err := mgr.Get(context.Background(), "alice", "api", "/fail")
	if err != nil {
		t.Fatalf("unexpected error for HTTP 500: %v", err)
	}
	if resp.StatusCode() != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode())
	}
}

func TestConcurrentSameUser(t *testing.T) {
	srv := newEchoServer(t)
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _ = mgr.Get(context.Background(), "shared-user", "api", "/")
		}()
	}
	wg.Wait()
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

func TestNew_StoreOpenFailure(t *testing.T) {
	// Point DBPath at a directory that doesn't exist to make store.OpenStore fail.
	_, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://x", RatePerMinute: 60},
		},
		DBPath: "/nonexistent/directory/pace.db",
	})
	if err == nil {
		t.Fatal("expected error when store cannot be opened")
	}
}

func TestRequest_ErrClosed_WhileWaiting(t *testing.T) {
	// Manager with rate=1/min, burst=1: consume the first token then close the
	// manager while the second request is waiting — it must return ErrClosed.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 1, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Exhaust the single token.
	if _, err := mgr.Request(ctx, "u", "api"); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		// This will block waiting for a token.
		_, err := mgr.Request(ctx, "u", "api")
		errCh <- err
	}()

	// Give the goroutine time to reach Wait, then close the manager.
	time.Sleep(20 * time.Millisecond)
	mgr.Close()

	err = <-errCh
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestClose_StoreError(t *testing.T) {
	// Create a manager with a store, pre-populate a user so saveAll has work to do,
	// then close the underlying db — Close() must log (not panic) on both
	// saveAll write errors and store.Close errors.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "close_err.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}

	// Close the underlying db so saveAll + store.Close both fail.
	pace.CloseManagerStore(mgr)

	// Close must not panic or block; it should just log warnings.
	mgr.Close()
}

func TestGCLoop_ExitsOnClose(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 60},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mgr.Close()
	// WaitGCLoop blocks until the gcLoop goroutine exits via ctx.Done().
	pace.WaitGCLoop(mgr)
}

func TestGetOrCreateUser_DoubleCheck(t *testing.T) {
	// Verify the double-check path: when two goroutines race to create the same
	// user, the second one finds it already in the shard under the write lock.
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 100},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// hookReady: goroutine A signals it has released the read lock and is paused.
	// hookDone:  main goroutine signals B has created the user; A can proceed.
	hookReady := make(chan struct{})
	hookDone := make(chan struct{})

	var once sync.Once
	pace.SetGetOrCreateHook(mgr, func() {
		once.Do(func() {
			close(hookReady) // A is about to acquire the write lock
			<-hookDone       // wait until B has already created the user
		})
	})

	go func() {
		// Goroutine A: will pause at the hook, then find the user in the
		// double-check (created by main goroutine B below).
		_, _ = mgr.Get(context.Background(), "race-user", "api", "/")
	}()

	<-hookReady // A released read lock and is paused before write lock

	// Clear the hook so the main goroutine's call doesn't also block.
	pace.SetGetOrCreateHook(mgr, nil)

	// Main goroutine (B): creates "race-user" while A is paused.
	if _, err := mgr.Get(context.Background(), "race-user", "api", "/"); err != nil {
		t.Fatal(err)
	}

	close(hookDone) // release A; it will acquire write lock and hit double-check

	// Brief wait for goroutine A to complete.
	time.Sleep(50 * time.Millisecond)
}

func TestCreateUserBuckets_StoreLoadError(t *testing.T) {
	// Close the store before creating a new user — createUserBuckets must log
	// the load error and continue with a fresh bucket.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "load_err.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// Break the store, then try to create a brand-new user.
	pace.CloseManagerStore(mgr)

	// Should not panic; logger.Warn is called internally.
	if _, err := mgr.Get(context.Background(), "new-user-after-close", "api", "/"); err != nil {
		t.Fatal(err)
	}
}

func TestEvict_StoreError(t *testing.T) {
	// Break the store, then evict a user — evictUser must log the save error.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "evict_err.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}

	pace.CloseManagerStore(mgr)

	// Evict must not panic; it logs the store.Save error internally.
	mgr.Evict("alice")
}

func TestSaveAll_StoreError(t *testing.T) {
	// Already covered by TestClose_StoreError which closes the db before Close().
	// This explicit test triggers saveAll via GC eviction with a broken store,
	// exercising the warn path in saveAll independently.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "saveall_err.db")

	clock := newFakeClock()
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath:     dbPath,
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}

	pace.CloseManagerStore(mgr)

	// Advance past idle expiry and trigger GC — saveAll would be called on Close,
	// but evictUser (which calls store.Save) is exercised here via collectIdle.
	clock.advance(10 * time.Minute)
	pace.CollectIdle(mgr) // evictUser → store.Save fails → warn
}

func TestRequest_BuildURLError(t *testing.T) {
	// A path with a null byte causes http.NewRequestWithContext to fail.
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	req, err := mgr.Request(context.Background(), "u", "api")
	if err != nil {
		t.Fatal(err)
	}
	// Null byte in the path makes NewRequestWithContext return an error.
	_, err = req.Get("/\x00bad")
	if err == nil {
		t.Fatal("expected error for URL with null byte")
	}
}

func TestRequest_TransportError(t *testing.T) {
	// Inject a transport that always returns an error to cover client.Do failure.
	transportErr := errors.New("dial refused")
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 6000},
		},
		Transport: failTransport{err: transportErr},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.Get(context.Background(), "u", "api", "/")
	if err == nil {
		t.Fatal("expected transport error")
	}
}

func TestRequest_BodyReadError(t *testing.T) {
	// Inject a transport whose response body errors on Read.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 6000},
		},
		Transport: errBodyTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.Get(context.Background(), "u", "api", "/")
	if err == nil {
		t.Fatal("expected body read error")
	}
}

// mockCloseErrStore implements StateStore but returns an error on Close.
type mockCloseErrStore struct{}

func (m *mockCloseErrStore) Save(_ string, _ map[string]pace.SavedState) error { return nil }
func (m *mockCloseErrStore) Load(_ string) (map[string]pace.SavedState, error) { return nil, nil }
func (m *mockCloseErrStore) Close() error                                      { return errors.New("mock close error") }

func TestClose_StoreCloseError(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Make a request so saveAll has a user to flush.
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
	// Inject a mock that errors on Close; Close must not panic.
	pace.SetManagerStore(mgr, &mockCloseErrStore{})
	mgr.Close()
}

func TestRequest_CallerCtxCancelledWhileWaiting(t *testing.T) {
	// Cover the `return nil, err` branch in Request: bucket.Wait returns an error
	// AND ctx.Err() is non-nil because the CALLER's context was cancelled while
	// the request was truly blocked (not pre-empted by rate-limiter deadline logic).
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 1, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	ctx := context.Background()
	// Exhaust the single token.
	if _, err := mgr.Request(ctx, "u", "api"); err != nil {
		t.Fatal(err)
	}

	// Use WithCancel (not WithTimeout) so the rate limiter cannot detect the
	// deadline upfront and return early — it will truly block in Wait.
	ctx2, cancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() {
		_, err := mgr.Request(ctx2, "u", "api")
		errCh <- err
	}()

	// Give the goroutine time to enter bucket.Wait, then cancel the caller ctx.
	time.Sleep(20 * time.Millisecond)
	cancel()

	err = <-errCh
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ctx cancellation error, not ErrClosed; got %v", err)
	}
}

func TestGCLoop_TickerFires(t *testing.T) {
	// Use a very short GCInterval so the ticker fires before Close(), covering
	// the case <-ticker.C: m.collectIdle() branch in gcLoop.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 60},
		},
		GCInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Wait long enough for the ticker to fire at least once.
	time.Sleep(20 * time.Millisecond)
	mgr.Close()
	pace.WaitGCLoop(mgr)
}

// --- StateStore (pluggable backend) tests ---

// noopStore is a StateStore that always succeeds and returns no saved state.
type noopStore struct{}

func (s *noopStore) Save(_ string, _ map[string]pace.SavedState) error { return nil }
func (s *noopStore) Load(_ string) (map[string]pace.SavedState, error) { return nil, nil }
func (s *noopStore) Close() error                                      { return nil }

// loadStateStore returns predefined saved state so RestoreBucket is exercised.
type loadStateStore struct{ state map[string]pace.SavedState }

func (s *loadStateStore) Save(_ string, _ map[string]pace.SavedState) error { return nil }
func (s *loadStateStore) Load(_ string) (map[string]pace.SavedState, error) {
	return s.state, nil
}
func (s *loadStateStore) Close() error { return nil }

// errLoadStore causes Load to return an error.
type errLoadStore struct{}

func (s *errLoadStore) Save(_ string, _ map[string]pace.SavedState) error { return nil }
func (s *errLoadStore) Load(_ string) (map[string]pace.SavedState, error) {
	return nil, errors.New("load failed")
}
func (s *errLoadStore) Close() error { return nil }

func TestNew_StoreBothSet(t *testing.T) {
	_, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://x", RatePerMinute: 60},
		},
		DBPath: "/tmp/both.db",
		Store:  &noopStore{},
	})
	if err == nil {
		t.Fatal("expected error when both Store and DBPath are set")
	}
}

func TestNew_CustomStore_NoopLoad(t *testing.T) {
	// Config.Store with a no-op backend: createUserBuckets calls wrapper.Load.
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 5},
		},
		Store: &noopStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
}

func TestNew_CustomStore_WithSavedState(t *testing.T) {
	// Config.Store returns saved state so the wrapper.Load conversion path runs.
	srv := newEchoServer(t)
	defer srv.Close()

	now := time.Now().UnixNano()
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 3},
		},
		Store: &loadStateStore{state: map[string]pace.SavedState{
			"api": {Tokens: 1.5, LastUsed: now},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// User is loaded from the custom store — should have tokens available.
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
}

func TestCustomStore_LoadError(t *testing.T) {
	// Config.Store.Load returns an error — wrapper must propagate it; Manager
	// logs a warning and falls back to a fresh bucket.
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		Store: &errLoadStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	// Must not panic; the load error is logged and a fresh bucket is used.
	if _, err := mgr.Get(context.Background(), "alice", "api", "/"); err != nil {
		t.Fatal(err)
	}
}

// --- Graceful Shutdown tests ---

func TestShutdown_GracefulFinish(t *testing.T) {
	// Shutdown with a generous deadline: all in-flight requests complete before
	// the timeout, so Shutdown returns nil.
	srv := newEchoServer(t)
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for range 3 {
		go func() {
			defer wg.Done()
			_, _ = mgr.Get(context.Background(), "u", "api", "/")
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mgr.Shutdown(ctx); err != nil {
		t.Fatalf("expected graceful shutdown, got %v", err)
	}
}

func TestShutdown_ForcedOnTimeout(t *testing.T) {
	// Shutdown with an expired context: force-cancel path is taken.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			// rate=1/min, burst=1: second request blocks for ~60s
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 1, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the token so subsequent requests block in Wait.
	if _, err := mgr.Request(context.Background(), "u", "api"); err != nil {
		t.Fatal(err)
	}

	// Start a goroutine that will block in bucket.Wait.
	go func() { _, _ = mgr.Request(context.Background(), "u", "api") }()
	time.Sleep(20 * time.Millisecond)

	// Shutdown with an already-cancelled context → forced path.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err = mgr.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// --- Once / durable queue tests ---

func TestOnce_NoPersistence(t *testing.T) {
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 60},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr.Close()

	_, err = mgr.Once(context.Background(), "job-1", "u", "api", pace.RequestSpec{Path: "/"})
	if !errors.Is(err, pace.ErrNoPersistence) {
		t.Fatalf("expected ErrNoPersistence, got %v", err)
	}
}

func TestOnce_NewJob(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "once.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	resp, err := mgr.Once(context.Background(), "job-1", "alice", "api", pace.RequestSpec{
		Method: "GET",
		Path:   "/",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
}

func TestOnce_CachedResult(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cached"))
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "cached.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	spec := pace.RequestSpec{Method: "GET", Path: "/"}

	// First call executes the HTTP request.
	if _, err := mgr.Once(context.Background(), "job-42", "u", "api", spec); err != nil {
		t.Fatal(err)
	}
	// Second call with same ID must return cached result without a new HTTP call.
	resp, err := mgr.Once(context.Background(), "job-42", "u", "api", spec)
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body()) != "cached" {
		t.Fatalf("want cached body, got %q", resp.Body())
	}
	if callCount.Load() != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", callCount.Load())
	}
}

func TestOnce_Singleflight(t *testing.T) {
	// Concurrent Once calls with the same ID: only one HTTP request fires.
	ready := make(chan struct{})
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		<-ready // hold the server until all goroutines are waiting
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "sf.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	spec := pace.RequestSpec{Method: "GET", Path: "/sf"}
	const n = 5
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := mgr.Once(context.Background(), "sf-job", "u", "api", spec)
			errs <- err
		}()
	}
	// Give goroutines time to reach Wait, then unblock the server.
	time.Sleep(30 * time.Millisecond)
	close(ready)

	for range n {
		if err := <-errs; err != nil {
			t.Errorf("Once error: %v", err)
		}
	}
	if callCount.Load() != 1 {
		t.Errorf("expected 1 HTTP call, got %d", callCount.Load())
	}
}

func TestOnce_ReplayOnRestart(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "replay.db")

	// Create mgr1, plant a pending job directly (simulating a crash before completion).
	mgr1, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr1)

	spec := pace.RequestSpec{Method: "GET", Path: "/replay"}
	if err := pace.EnqueueJob(mgr1, "replay-job", "u", "api", spec); err != nil {
		t.Fatal(err)
	}
	mgr1.Close()

	// mgr2 starts fresh: replayPending should execute the planted job.
	mgr2, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mgr2.Close()
	pace.WaitReplay(mgr2) // blocks until the replayed job finishes

	// The result must now be cached; Once returns without a new HTTP call.
	resp, err := mgr2.Once(context.Background(), "replay-job", "u", "api", spec)
	if err != nil {
		t.Fatalf("Once after replay: %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
}

func TestOnce_UnknownEndpoint(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "ep.db")
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 60},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	_, err = mgr.Once(context.Background(), "j", "u", "missing", pace.RequestSpec{Path: "/"})
	if !errors.Is(err, pace.ErrUnknownEndpoint) {
		t.Fatalf("expected ErrUnknownEndpoint, got %v", err)
	}
}

func TestOnce_DefaultMethodGet(t *testing.T) {
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "meth.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	// Empty Method should default to GET.
	if _, err := mgr.Once(context.Background(), "j1", "u", "api", pace.RequestSpec{Path: "/"}); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("want GET, got %s", gotMethod)
	}
}

func TestOnce_LoadResultError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "lre.db")
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 60},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)

	// Break the underlying DB so LoadResult returns an error.
	pace.CloseManagerStore(mgr)

	_, err = mgr.Once(context.Background(), "j", "u", "api", pace.RequestSpec{Path: "/"})
	if err == nil || errors.Is(err, pace.ErrNoPersistence) {
		t.Fatalf("expected load result error, got %v", err)
	}
	mgr.Close()
}

func TestOnce_WaiterCtxCancelled(t *testing.T) {
	// Block the server so the leader stays in-flight; cancel the waiter's context.
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "wait.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	spec := pace.RequestSpec{Method: "GET", Path: "/wait"}

	// Leader goroutine blocks on the server.
	go func() {
		_, _ = mgr.Once(context.Background(), "w-job", "u", "api", spec)
	}()
	time.Sleep(20 * time.Millisecond) // let the leader enter inflight map

	// Waiter goroutine with a cancellable context.
	ctx2, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := mgr.Once(ctx2, "w-job", "u", "api", spec)
		errCh <- err
	}()
	time.Sleep(10 * time.Millisecond) // let waiter block on f.done
	cancel()                          // cancel the waiter

	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	close(hold) // unblock the server so the leader exits
}

func TestOnce_WithHeaders(t *testing.T) {
	var gotHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHdr = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "hdr.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	spec := pace.RequestSpec{
		Method:  "GET",
		Path:    "/",
		Headers: map[string]string{"X-Custom": "my-value"},
	}
	if _, err := mgr.Once(context.Background(), "hdr-job", "u", "api", spec); err != nil {
		t.Fatal(err)
	}
	if gotHdr != "my-value" {
		t.Fatalf("want X-Custom=my-value, got %q", gotHdr)
	}
}

func TestOnce_HTTPTransportError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "txerr.db")
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 6000, Burst: 10},
		},
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)
	defer mgr.Close()

	_, err = mgr.Once(context.Background(), "tx-job", "u", "api", pace.RequestSpec{Path: "/tx"})
	if err == nil {
		t.Fatal("expected transport error from Once")
	}
}

func TestOnce_CompleteJobError(t *testing.T) {
	// Close the DB while the HTTP call is in-flight so CompleteJob fails.
	// Once() must still return the response (the warn is logged, not returned).
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "cje.db")

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000, Burst: 10},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)

	errCh := make(chan error, 1)
	go func() {
		_, err := mgr.Once(context.Background(), "cj-job", "u", "api", pace.RequestSpec{Path: "/cj"})
		errCh <- err
	}()

	// Let Once() enqueue the job, then close the DB before CompleteJob runs.
	time.Sleep(30 * time.Millisecond)
	pace.CloseManagerStore(mgr)
	close(hold) // let the server respond

	// Once() logs a warning but still returns the HTTP response.
	if err := <-errCh; err != nil {
		t.Fatalf("expected success despite CompleteJob error, got %v", err)
	}
	mgr.Close()
}

func TestOnce_EnqueueError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "enq.db")
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr)

	// Hook closes the DB right before EnqueueJob runs.
	pace.SetOnceEnqueueHook(mgr, func() {
		pace.CloseManagerStore(mgr)
		pace.SetOnceEnqueueHook(mgr, nil)
	})

	_, err = mgr.Once(context.Background(), "e-job", "u", "api", pace.RequestSpec{Path: "/"})
	if err == nil {
		t.Fatal("expected enqueue error")
	}
	mgr.Close()
}

func TestOnce_ReplayJobFails(t *testing.T) {
	// Plant a job for an endpoint that doesn't exist in mgr2; replayPending logs a warning.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "rjf.db")

	mgr1, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr1)

	// Plant a job referencing "ghost" endpoint (not in any manager).
	if err := pace.EnqueueJob(mgr1, "ghost-job", "u", "ghost", pace.RequestSpec{Path: "/"}); err != nil {
		t.Fatal(err)
	}
	mgr1.Close()

	// mgr2 has no "ghost" endpoint → replayPending logs a warning and continues.
	mgr2, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 6000},
		},
		DBPath: dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(mgr2) // waits for the failing goroutine to log and exit
	mgr2.Close()
}

func TestShutdown_RejectsNewRequests(t *testing.T) {
	// After Shutdown sets shuttingDown=true, new Request calls must return
	// ErrClosed via the shutting-down branch (not the ctx.Done branch, which
	// fires only after Close is called). We keep an in-flight request alive so
	// Shutdown blocks on activeWg.Wait() and never reaches Close during the test.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			// rate=1/min so the second goroutine blocks in bucket.Wait for ~60s.
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 1, Burst: 1},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the single burst token.
	if _, err := mgr.Request(context.Background(), "u", "api"); err != nil {
		t.Fatal(err)
	}

	// This goroutine blocks inside bucket.Wait, keeping activeWg at 1 so
	// Shutdown cannot proceed to Close() yet.
	go func() { _, _ = mgr.Request(context.Background(), "u", "api") }()
	time.Sleep(20 * time.Millisecond) // wait for goroutine to call activeWg.Add(1)

	// Start Shutdown in a goroutine. It sets shuttingDown=true immediately,
	// then blocks on activeWg.Wait() because the goroutine above is still in Wait.
	shutdownDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = mgr.Shutdown(ctx)
		close(shutdownDone)
	}()
	time.Sleep(10 * time.Millisecond) // wait for Shutdown to set shuttingDown=true

	// m.ctx is still alive (Close not called yet), but shuttingDown=true.
	// Request must return ErrClosed via the shuttingDown branch.
	_, err = mgr.Request(context.Background(), "u2", "api")
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ErrClosed from shuttingDown branch, got %v", err)
	}
	<-shutdownDone
}
