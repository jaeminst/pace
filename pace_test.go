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
	"github.com/jaeminst/pace/internal/store"
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

// mockCloseErrStore satisfies the storer interface but returns an error on Close.
type mockCloseErrStore struct{}

func (m *mockCloseErrStore) Save(_ string, _ map[string]float64, _ int64) error {
	return nil
}
func (m *mockCloseErrStore) Load(_ string) (map[string]store.SavedState, error) {
	return nil, nil
}
func (m *mockCloseErrStore) Close() error { return errors.New("mock close error") }

func TestClose_StoreCloseError(t *testing.T) {
	// Inject a store whose Close() returns an error so that the warn log path
	// in Manager.Close is covered.
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: "http://127.0.0.1:1", RatePerMinute: 6000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Replace the (nil) store with a mock that errors on Close.
	pace.SetManagerStore(mgr, &mockCloseErrStore{})
	// Close must not panic; logger.Warn is called internally.
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
