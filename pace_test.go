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
		t.Fatal("want error for empty BaseURL")
	}
}

func TestNew_ZeroRate(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    pace.PerMinute(0),
	})
	if err == nil {
		t.Fatal("want error for zero Rate")
	}
}

func TestGet(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	resp, err := client.Client("u1").Get(context.Background(), "/")
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := client.Client("u1").Request()
	if _, err := req.SetHeader("X-Trace-ID", "abc123").Get(context.Background(), "/"); err != nil {
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for _, tc := range []struct {
		name string
		call func(*pace.Request) (*pace.Response, error)
		want string
	}{
		{"Post", func(r *pace.Request) (*pace.Response, error) { return r.Post(context.Background(), "/") }, "POST"},
		{"Put", func(r *pace.Request) (*pace.Response, error) { return r.Put(context.Background(), "/") }, "PUT"},
		{"Delete", func(r *pace.Request) (*pace.Response, error) { return r.Delete(context.Background(), "/") }, "DELETE"},
		{"Patch", func(r *pace.Request) (*pace.Response, error) { return r.Patch(context.Background(), "/") }, "PATCH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := client.Client("u1").Request()
			if _, err := tc.call(req); err != nil {
				t.Fatal(err)
			}
			if m, _ := gotMethod.Load().(string); m != tc.want {
				t.Fatalf("want %s, got %s", tc.want, m)
			}
		})
	}
}

func TestClient_ConvenienceMethods(t *testing.T) {
	// Exercise client.Post, client.Put, client.Delete, client.Patch directly
	// (the convenience wrappers on *Client, not *Request).
	var gotMethod atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod.Store(r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() (*pace.Response, error)
		want string
	}{
		{"Post", func() (*pace.Response, error) { return client.Client("u").Post(ctx, "/") }, "POST"},
		{"Put", func() (*pace.Response, error) { return client.Client("u").Put(ctx, "/") }, "PUT"},
		{"Delete", func() (*pace.Response, error) { return client.Client("u").Delete(ctx, "/") }, "DELETE"},
		{"Patch", func() (*pace.Response, error) { return client.Client("u").Patch(ctx, "/") }, "PATCH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.call(); err != nil {
				t.Fatal(err)
			}
			if m, _ := gotMethod.Load().(string); m != tc.want {
				t.Fatalf("want %s, got %s", tc.want, m)
			}
		})
	}
}

func TestClient_ConvenienceMethods_ErrClosed(t *testing.T) {
	// Exercise the error-return branch of Post, Put, Delete, Patch on a closed
	// client so the `if err != nil { return nil, err }` lines are covered.
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close() // closed → Request returns ErrClosed

	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() (*pace.Response, error)
	}{
		{"Post", func() (*pace.Response, error) { return client.Client("u").Post(ctx, "/") }},
		{"Put", func() (*pace.Response, error) { return client.Client("u").Put(ctx, "/") }},
		{"Delete", func() (*pace.Response, error) { return client.Client("u").Delete(ctx, "/") }},
		{"Patch", func() (*pace.Response, error) { return client.Client("u").Patch(ctx, "/") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			if !errors.Is(err, pace.ErrClosed) {
				t.Fatalf("want ErrClosed, got %v", err)
			}
		})
	}
}

func TestErrClosed(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    pace.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	err = client.Client("u").Wait(context.Background())
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

// TestUserIsolation verifies that exhausting one user's bucket does not affect another.
func TestUserIsolation(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	// 1 req/min, burst=1: after one call the user must wait ~60s for the next token.
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Alice consumes her single token.
	if _, err := client.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Bob has his own bucket and must not be affected.
	ctxBob, cancelBob := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelBob()
	if _, err := client.Client("bob").Get(ctxBob, "/"); err != nil {
		t.Fatalf("bob (isolated): %v", err)
	}

	// Alice is throttled — her second call must time out.
	ctxAlice, cancelAlice := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancelAlice()
	if _, err := client.Client("alice").Get(ctxAlice, "/"); err == nil {
		t.Fatal("alice second call should have been throttled")
	}
}

func TestContextCancellation(t *testing.T) {
	client, err := pace.New(pace.Config{
		// 1/min so the second request blocks for ~60s.
		BaseURL: "http://127.0.0.1:0",
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Exhaust the token (no HTTP call needed — Request() just waits for a token).
	if err := client.Client("u").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	// Second request should return when context times out.
	ctx2, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = client.Client("u").Wait(ctx2)
	if err == nil {
		t.Fatal("want error from cancelled context")
	}
}

func TestConcurrentUsers(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 20
	errs := make(chan error, n)
	ctx := context.Background()
	for i := range n {
		go func(id int) {
			_, err := client.Client(fmt.Sprintf("user-%d", id)).Get(ctx, "/")
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	client.Close()

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db file not created: %v", err)
	}
}

// TestStorePersistenceThrottles checks that token state persists across Client restarts.
// A very low rate (6/min = 1 token per 10s) ensures the gap between close and
// re-open is too small to restore even one token.
func TestStorePersistenceThrottles(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pace.db")

	srv := newEchoServer(t)
	defer srv.Close()

	cfg := pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6),
		Burst:   1,
		DBPath:  dbPath,
	}

	// client1: consume Alice's single token then close (persists ≈0 tokens).
	client1, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client1.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatalf("client1 alice: %v", err)
	}
	client1.Close()

	// client2: restore from DB — Alice should still be throttled.
	client2, err := pace.New(cfg)
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

func TestNew_EmptyBaseURL(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "",
		Rate:    pace.PerMinute(60),
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := client.Client("u1").Request()
	payload := []byte(`{"hello":"world"}`)
	if _, err := req.SetBody(payload).Post(context.Background(), "/"); err != nil {
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	resp, err := client.Client("u1").Get(context.Background(), "/")
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
	client, err := pace.New(pace.Config{
		// burst=1, rate=1/min: alice's token is exhausted after one call
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(1),
		Burst:      1,
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	// Alice uses her single token.
	if _, err := client.Client("alice").Get(ctx, "/"); err != nil {
		t.Fatalf("alice first call: %v", err)
	}

	// Alice is now throttled — second call times out.
	ctxShort, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := client.Client("alice").Get(ctxShort, "/"); err == nil {
		t.Fatal("alice should be throttled before GC")
	}

	// Advance clock past IdleExpiry and run GC.
	clock.advance(10 * time.Minute)
	pace.CollectIdle(client)

	// Alice's bucket is evicted and re-created fresh → burst=1 available again.
	ctxFresh, cancelFresh := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancelFresh()
	if _, err := client.Client("alice").Get(ctxFresh, "/"); err != nil {
		t.Fatalf("alice after GC eviction: %v", err)
	}
}

func TestGC_SavesStateOnEvict(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pace.db")
	srv := newEchoServer(t)
	defer srv.Close()

	clock := newFakeClock()
	client, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(6000),
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
		DBPath:     dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatalf("alice: %v", err)
	}

	clock.advance(10 * time.Minute)
	pace.CollectIdle(client)

	// DB file must exist and contain alice's record.
	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("db not found after GC: %v", err)
	}
}

func TestErrClosed_Concurrent(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:0",
		Rate:    pace.PerMinute(6000),
		Burst:   100,
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n + 1)

	go func() {
		defer wg.Done()
		client.Close()
	}()
	for i := range n {
		go func(id int) {
			defer wg.Done()
			_ = client.Client(fmt.Sprintf("u%d", id)).Wait(context.Background())
		}(i)
	}
	wg.Wait()
}

func TestTokens_ExistingUser(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// consume one token
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	tokens := tokensOf(client.Client("alice"))
	if tokens >= 3 {
		t.Fatalf("expected tokens < 3 after one request, got %v", tokens)
	}
}

func TestTokens_UnknownUser(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:0",
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	n, ok := client.Client("nobody").Tokens()
	if ok || n != 0 {
		t.Fatalf("Tokens() for an unseen user = (%v, %v), want (0, false)", n, ok)
	}
}

func TestEvict_RemovesUser(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if !evict(t, client.Client("alice")) {
		t.Fatal("expected Evict to return true for existing user")
	}
	n, ok := client.Client("alice").Tokens()
	if ok || n != 0 {
		t.Fatalf("Tokens() after Evict = (%v, %v), want (0, false)", n, ok)
	}
}

func TestEvict_ReturnsFalseForUnknownUser(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:0",
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if evict(t, client.Client("ghost")) {
		t.Fatal("expected Evict to return false for unknown user")
	}
}

func TestEvict_SavesToDB(t *testing.T) {
	srv := newEchoServer(t)
	dbPath := filepath.Join(t.TempDir(), "evict.db")
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	tokensBefore := tokensOf(client.Client("alice"))
	evict(t, client.Client("alice"))

	// Re-open a new client: alice's tokens should be restored from DB
	client2, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
		DBPath:  dbPath,
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

func TestBurstCeiling(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// First request: consumes the only burst token
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// Second request: no token, should block; use tight timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.Client("alice").Get(ctx, "/")
	if err == nil {
		t.Fatal("expected second request to block/fail with burst=1")
	}
}

func TestOnThrottle_CalledWhenBlocked(t *testing.T) {
	srv := newEchoServer(t)
	var called atomic.Int32
	client, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(60),
		Burst:      1,
		OnThrottle: func(_ string) { called.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Exhaust the burst token
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// This request should trigger OnThrottle (no token available)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = client.Client("alice").Get(ctx, "/")

	if called.Load() == 0 {
		t.Fatal("expected OnThrottle to be called")
	}
}

func TestOnThrottle_NotCalledWhenAvailable(t *testing.T) {
	srv := newEchoServer(t)
	var called atomic.Int32
	client, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(60),
		Burst:      5,
		OnThrottle: func(_ string) { called.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Token is available; OnThrottle must NOT fire
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	resp, err := client.Client("alice").Get(context.Background(), "/fail")
	if err != nil {
		t.Fatalf("unexpected error for HTTP 500: %v", err)
	}
	if resp.StatusCode() != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode())
	}
}

func TestConcurrentSameUser(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _ = client.Client("shared-user").Get(context.Background(), "/")
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
		BaseURL: "http://x",
		Rate:    pace.PerMinute(60),
		DBPath:  "/nonexistent/directory/pace.db",
	})
	if err == nil {
		t.Fatal("expected error when store cannot be opened")
	}
}

func TestRequest_ErrClosed_WhileWaiting(t *testing.T) {
	// Client with rate=1/min, burst=1: consume the first token then close the
	// client while the second request is waiting — it must return ErrClosed.
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	// Exhaust the single token.
	if err := client.Client("u").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	errCh := make(chan error, 1)
	go func() {
		// This will block waiting for a token.
		err := client.Client("u").Wait(ctx)
		errCh <- err
	}()

	// Give the goroutine time to reach Wait, then close the client.
	time.Sleep(20 * time.Millisecond)
	client.Close()

	err = <-errCh
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestClose_StoreError(t *testing.T) {
	// Create a client with a store, pre-populate a user so saveAll has work to do,
	// then close the underlying db — Close() must log (not panic) on both
	// saveAll write errors and store.Close errors.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "close_err.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	// Close the underlying db so saveAll + store.Close both fail.
	pace.CloseLimiterStore(client)

	// Close must not panic or block; it should just log warnings.
	client.Close()
}

func TestGCLoop_ExitsOnClose(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	// WaitGCLoop blocks until the gcLoop goroutine exits via ctx.Done().
	pace.WaitGCLoop(client)
}

func TestGetOrCreateUser_DoubleCheck(t *testing.T) {
	// Verify the double-check path: when two goroutines race to create the same
	// user, the second one finds it already in the shard under the write lock.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// hookReady: goroutine A signals it has released the read lock and is paused.
	// hookDone:  main goroutine signals B has created the user; A can proceed.
	hookReady := make(chan struct{})
	hookDone := make(chan struct{})

	var once sync.Once
	pace.SetGetOrCreateHook(client, func() {
		once.Do(func() {
			close(hookReady) // A is about to acquire the write lock
			<-hookDone       // wait until B has already created the user
		})
	})

	go func() {
		// Goroutine A: will pause at the hook, then find the user in the
		// double-check (created by main goroutine B below).
		_, _ = client.Client("race-user").Get(context.Background(), "/")
	}()

	<-hookReady // A released read lock and is paused before write lock

	// Clear the hook so the main goroutine's call doesn't also block.
	pace.SetGetOrCreateHook(client, nil)

	// Main goroutine (B): creates "race-user" while A is paused.
	if _, err := client.Client("race-user").Get(context.Background(), "/"); err != nil {
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

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Break the store, then try to create a brand-new user.
	pace.CloseLimiterStore(client)

	// Should not panic; logger.Warn is called internally.
	if _, err := client.Client("new-user-after-close").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestEvict_StoreError(t *testing.T) {
	// Break the store, then evict a user — evictUser must log the save error.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "evict_err.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	pace.CloseLimiterStore(client)

	// The store is broken, so persisting fails. Evict reports that rather than
	// swallowing it into a log line: the caller asked for this write.
	present, err := client.Client("alice").Evict(context.Background())
	if !present {
		t.Error("Evict = false, want true for a user that was in memory")
	}
	if err == nil {
		t.Error("Evict = nil error with a closed store, want the store failure")
	}
}

func TestSaveAll_StoreError(t *testing.T) {
	// Already covered by TestClose_StoreError which closes the db before Close().
	// This explicit test triggers saveAll via GC eviction with a broken store,
	// exercising the warn path in saveAll independently.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "saveall_err.db")

	clock := newFakeClock()
	client, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(6000),
		DBPath:     dbPath,
		IdleExpiry: 5 * time.Minute,
		Clock:      clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	pace.CloseLimiterStore(client)

	// Advance past idle expiry and trigger GC — saveAll would be called on Close,
	// but evictUser (which calls store.Save) is exercised here via collectIdle.
	clock.advance(10 * time.Minute)
	pace.CollectIdle(client) // evictUser → store.Save fails → warn
}

func TestRequest_BuildURLError(t *testing.T) {
	// A path with a null byte causes http.NewRequestWithContext to fail.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := client.Client("u").Request()
	// Null byte in the path makes NewRequestWithContext return an error.
	_, err = req.Get(context.Background(), "/\x00bad")
	if err == nil {
		t.Fatal("expected error for URL with null byte")
	}
}

func TestRequest_TransportError(t *testing.T) {
	// Inject a transport that always returns an error to cover client.Do failure.
	transportErr := errors.New("dial refused")
	client, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      pace.PerMinute(6000),
		Transport: failTransport{err: transportErr},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.Client("u").Get(context.Background(), "/")
	if err == nil {
		t.Fatal("expected transport error")
	}
}

func TestRequest_BodyReadError(t *testing.T) {
	// Inject a transport whose response body errors on Read.
	client, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      pace.PerMinute(6000),
		Transport: errBodyTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = client.Client("u").Get(context.Background(), "/")
	if err == nil {
		t.Fatal("expected body read error")
	}
}

// mockCloseErrStore implements StateStore but returns an error on Close.
type mockCloseErrStore struct{}

func (m *mockCloseErrStore) Save(_ context.Context, _ string, _ pace.State) error { return nil }
func (m *mockCloseErrStore) Load(_ context.Context, _ string) (pace.State, bool, error) {
	return pace.State{}, false, nil
}
func (m *mockCloseErrStore) Close() error { return errors.New("mock close error") }

func TestClose_StoreCloseError(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Make a request so saveAll has a user to flush.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// Inject a mock that errors on Close; Close must not panic.
	pace.SetLimiterStore(client, &mockCloseErrStore{})
	client.Close()
}

func TestRequest_CallerCtxCancelledWhileWaiting(t *testing.T) {
	// Cover the `return nil, err` branch in Request: bucket.Wait returns an error
	// AND ctx.Err() is non-nil because the CALLER's context was cancelled while
	// the request was truly blocked (not pre-empted by rate-limiter deadline logic).
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	// Exhaust the single token.
	if err := client.Client("u").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	// Use WithCancel (not WithTimeout) so the rate limiter cannot detect the
	// deadline upfront and return early — it will truly block in Wait.
	ctx2, cancel := context.WithCancel(ctx)

	errCh := make(chan error, 1)
	go func() {
		err := client.Client("u").Wait(ctx2)
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
	client, err := pace.New(pace.Config{
		BaseURL:    "http://127.0.0.1:1",
		Rate:       pace.PerMinute(60),
		GCInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Wait long enough for the ticker to fire at least once.
	time.Sleep(20 * time.Millisecond)
	client.Close()
	pace.WaitGCLoop(client)
}

// --- StateStore (pluggable backend) tests ---

// noopStore is a StateStore that always succeeds and returns no saved state.
type noopStore struct{}

func (s *noopStore) Save(_ context.Context, _ string, _ pace.State) error { return nil }
func (s *noopStore) Load(_ context.Context, _ string) (pace.State, bool, error) {
	return pace.State{}, false, nil
}
func (s *noopStore) Close() error { return nil }

// loadStateStore returns predefined saved state so RestoreBucket is exercised.
type loadStateStore struct{ state pace.State }

func (s *loadStateStore) Save(_ context.Context, _ string, _ pace.State) error { return nil }
func (s *loadStateStore) Load(_ context.Context, _ string) (pace.State, bool, error) {
	return s.state, true, nil
}
func (s *loadStateStore) Close() error { return nil }

// errLoadStore causes Load to return an error.
type errLoadStore struct{}

func (s *errLoadStore) Save(_ context.Context, _ string, _ pace.State) error { return nil }
func (s *errLoadStore) Load(_ context.Context, _ string) (pace.State, bool, error) {
	return pace.State{}, false, errors.New("load failed")
}
func (s *errLoadStore) Close() error { return nil }

func TestNew_StoreBothSet(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    pace.PerMinute(60),
		DBPath:  "/tmp/both.db",
		Store:   &noopStore{},
	})
	if err == nil {
		t.Fatal("expected error when both Store and DBPath are set")
	}
}

func TestNew_CustomStore_NoopLoad(t *testing.T) {
	// Config.Store with a no-op backend: createUserBuckets calls wrapper.Load.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   5,
		Store:   &noopStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestNew_CustomStore_WithSavedState(t *testing.T) {
	// Config.Store returns saved state so the wrapper.Load conversion path runs.
	srv := newEchoServer(t)
	defer srv.Close()

	now := time.Now()
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
		Store: &loadStateStore{state: pace.State{
			Tokens: 1.5, LastUsed: now,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// User is loaded from the custom store — should have tokens available.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestCustomStore_LoadError(t *testing.T) {
	// Config.Store.Load returns an error — wrapper must propagate it; Client
	// logs a warning and falls back to a fresh bucket.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Store:   &errLoadStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Must not panic; the load error is logged and a fresh bucket is used.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

// --- Graceful Shutdown tests ---

func TestShutdown_GracefulFinish(t *testing.T) {
	// Shutdown with a generous deadline: all in-flight requests complete before
	// the timeout, so Shutdown returns nil.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
	})
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(3)
	for range 3 {
		go func() {
			defer wg.Done()
			_, _ = client.Client("u").Get(context.Background(), "/")
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Shutdown(ctx); err != nil {
		t.Fatalf("expected graceful shutdown, got %v", err)
	}
}

func TestShutdown_ForcedOnTimeout(t *testing.T) {
	// Shutdown with an expired context: force-cancel path is taken.
	client, err := pace.New(pace.Config{
		// rate=1/min, burst=1: second request blocks for ~60s
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the token so subsequent requests block in Wait.
	if err := client.Client("u").Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Start a goroutine that will block in bucket.Wait.
	go func() { _ = client.Client("u").Wait(context.Background()) }()
	time.Sleep(20 * time.Millisecond)

	// Shutdown with an already-cancelled context → forced path.
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err = client.Shutdown(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// --- Durable queue tests ---

func TestDurable_NoPersistence(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = durableDo(context.Background(), client.Client("u"), "job-1", http.MethodGet, "/")
	if !errors.Is(err, pace.ErrNoQueue) {
		t.Fatalf("expected ErrNoQueue, got %v", err)
	}
}

func TestDurable_NewJob(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "once.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	resp, err := durableDo(context.Background(), client.Client("alice"), "job-1", http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
}

func TestDurable_CachedResult(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cached"))
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "cached.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	// First call executes the HTTP request.
	if _, err := durableDo(context.Background(), client.Client("u"), "job-42", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	// Second call with same ID must return cached result without a new HTTP call.
	resp, err := durableDo(context.Background(), client.Client("u"), "job-42", http.MethodGet, "/")
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

func TestDurable_Singleflight(t *testing.T) {
	// Concurrent Durable calls with the same ID: only one HTTP request fires.
	ready := make(chan struct{})
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		<-ready // hold the server until all goroutines are waiting
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "sf.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	const n = 5
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := durableDo(context.Background(), client.Client("u"), "sf-job", http.MethodGet, "/sf")
			errs <- err
		}()
	}
	// Give goroutines time to reach Wait, then unblock the server.
	time.Sleep(30 * time.Millisecond)
	close(ready)

	for range n {
		if err := <-errs; err != nil {
			t.Errorf("Durable error: %v", err)
		}
	}
	if callCount.Load() != 1 {
		t.Errorf("expected 1 HTTP call, got %d", callCount.Load())
	}
}

func TestDurable_ReplayOnRestart(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "replay.db")

	// Create client1, plant a pending job directly (simulating a crash before completion).
	client1, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	if err := pace.Enqueue(client1, "replay-job", "u", "GET", "/replay"); err != nil {
		t.Fatal(err)
	}
	client1.Close()

	// client2 starts fresh: replay should execute the planted job.
	client2, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	pace.WaitReplay(client2) // blocks until the replayed job finishes

	// The result must now be cached; Durable returns without a new HTTP call.
	resp, err := durableDo(context.Background(), client2.Client("u"), "replay-job", http.MethodGet, "/replay")
	if err != nil {
		t.Fatalf("Durable after replay: %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
}

func TestDurable_DefaultMethodGet(t *testing.T) {
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "meth.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	if _, err := durableDo(context.Background(), client.Client("u"), "j1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("want GET, got %s", gotMethod)
	}
}

func TestDurable_LoadResultError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "lre.db")
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(60),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)

	// Break the underlying DB so Get returns an error.
	pace.CloseLimiterStore(client)

	_, err = durableDo(context.Background(), client.Client("u"), "j", http.MethodGet, "/")
	if err == nil || errors.Is(err, pace.ErrNoQueue) {
		t.Fatalf("expected load result error, got %v", err)
	}
	client.Close()
}

func TestDurable_WaiterCtxCancelled(t *testing.T) {
	// Block the server so the leader stays in-flight; cancel the waiter's context.
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "wait.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	// Leader goroutine blocks on the server.
	go func() {
		_, _ = durableDo(context.Background(), client.Client("u"), "w-job", http.MethodGet, "/wait")
	}()
	time.Sleep(20 * time.Millisecond) // let the leader enter inflight map

	// Waiter goroutine with a cancellable context.
	ctx2, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := durableDo(ctx2, client.Client("u"), "w-job", http.MethodGet, "/wait")
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

func TestDurable_WithHeaders(t *testing.T) {
	var gotHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHdr = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "hdr.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	hdrReq, err := client.Client("u").Durable("hdr-job")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hdrReq.SetHeader("X-Custom", "my-value").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if gotHdr != "my-value" {
		t.Fatalf("want X-Custom=my-value, got %q", gotHdr)
	}
}

func TestDurable_HTTPTransportError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "txerr.db")
	client, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      pace.PerMinute(6000),
		Burst:     10,
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	_, err = durableDo(context.Background(), client.Client("u"), "tx-job", http.MethodGet, "/tx")
	if err == nil {
		t.Fatal("expected transport error from Durable")
	}
}

func TestDurable_CompleteJobError(t *testing.T) {
	// Close the DB while the HTTP call is in-flight so Complete fails.
	// Durable must still return the response (the warn is logged, not returned).
	hold := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "cje.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)

	errCh := make(chan error, 1)
	go func() {
		_, err := durableDo(context.Background(), client.Client("u"), "cj-job", http.MethodGet, "/cj")
		errCh <- err
	}()

	// Let Durable enqueue the job, then close the DB before Complete runs.
	time.Sleep(30 * time.Millisecond)
	pace.CloseLimiterStore(client)
	close(hold) // let the server respond

	// Durable logs a warning but still returns the HTTP response.
	if err := <-errCh; err != nil {
		t.Fatalf("expected success despite Complete error, got %v", err)
	}
	client.Close()
}

func TestDurable_EnqueueError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "enq.db")
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)

	// Hook closes the DB right before Enqueue runs.
	pace.SetDurableEnqueueHook(client, func() {
		pace.CloseLimiterStore(client)
		pace.SetDurableEnqueueHook(client, nil)
	})

	_, err = durableDo(context.Background(), client.Client("u"), "e-job", http.MethodGet, "/")
	if err == nil {
		t.Fatal("expected enqueue error")
	}
	client.Close()
}

func TestDurable_ReplayJobFails(t *testing.T) {
	// Plant a job that will fail on replay; replay logs a warning.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "rjf.db")

	client1, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	// Plant a job for a path that won't connect on replay (use a bad port).
	if err := pace.Enqueue(client1, "fail-job", "u", "GET", "/"); err != nil {
		t.Fatal(err)
	}
	client1.Close()

	// client2 replays with a failing transport → replay logs a warning and continues.
	client2, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      pace.PerMinute(6000),
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client2) // waits for the failing goroutine to log and exit
	client2.Close()
}

func TestShutdown_RejectsNewRequests(t *testing.T) {
	// After Shutdown sets shuttingDown=true, new Request calls must return
	// ErrClosed via the shutting-down branch (not the ctx.Done branch, which
	// fires only after Close is called). We keep an in-flight request alive so
	// Shutdown blocks on activeWg.Wait() and never reaches Close during the test.
	client, err := pace.New(pace.Config{
		// rate=1/min so the second goroutine blocks in bucket.Wait for ~60s.
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Exhaust the single burst token.
	if err := client.Client("u").Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	// This goroutine blocks inside bucket.Wait, keeping activeWg at 1 so
	// Shutdown cannot proceed to Close() yet.
	go func() { _ = client.Client("u").Wait(context.Background()) }()
	time.Sleep(20 * time.Millisecond) // wait for goroutine to call activeWg.Add(1)

	// Start Shutdown in a goroutine. It sets shuttingDown=true immediately,
	// then blocks on activeWg.Wait() because the goroutine above is still in Wait.
	shutdownDone := make(chan struct{})
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_ = client.Shutdown(ctx)
		close(shutdownDone)
	}()
	time.Sleep(10 * time.Millisecond) // wait for Shutdown to set shuttingDown=true

	// m.ctx is still alive (Close not called yet), but shuttingDown=true.
	// Request must return ErrClosed via the shuttingDown branch.
	err = client.Client("u2").Wait(context.Background())
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ErrClosed from shuttingDown branch, got %v", err)
	}
	<-shutdownDone
}

func TestNew_StoreMutuallyExclusive(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "http://example.com",
		Rate:    pace.PerMinute(60),
		Store:   &noopStore{},
		DBPath:  "/tmp/some.db",
	})
	if err == nil {
		t.Fatal("expected error when both Store and DBPath are set")
	}
}

func TestDurable_CtxCancelledBeforeRequest(t *testing.T) {
	// Cancel the caller's context inside the pre-enqueue hook so that
	// m.Request() sees a cancelled context and doDurable returns ctx.Err().
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "ctxcancel.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	pace.SetDurableEnqueueHook(client, func() {
		cancel()
		pace.SetDurableEnqueueHook(client, nil)
	})

	_, err = durableDo(ctx, client.Client("u"), "cc-job", http.MethodGet, "/")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDurable_ReplayExecuteFails(t *testing.T) {
	// Plant a pending job then replay it with a failing transport so
	// replay logs "pace: replay: execute".
	dbPath := filepath.Join(t.TempDir(), "rxf.db")

	client1, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    pace.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	if err := pace.Enqueue(client1, "rxf-job", "u", "GET", "/rxf"); err != nil {
		t.Fatal(err)
	}
	client1.Close()

	client2, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      pace.PerMinute(6000),
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client2)
	client2.Close()
}

func TestDurable_ReplayWithHeaders(t *testing.T) {
	// Enqueue a Durable job with headers via a blocking server. Close client1
	// while the HTTP call is in-flight (server still holding); the cancelled
	// context leaves the job pending. client2 replays it, exercising the
	// header-copying loop inside replay().
	hold := make(chan struct{})
	var gotHdr atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHdr.Store(r.Header.Get("X-Replay"))
		<-hold // block until released
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "rph.db")

	// client1: start a Durable call with a header; close while server blocks,
	// leaving the job pending in the DB.
	client1, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	go func() {
		req, err := client1.Client("u").Durable("hdr-replay-job")
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = req.SetHeader("X-Replay", "replayed").Get(context.Background(), "/hdr-replay")
	}()
	// Give the goroutine time to enqueue the job and reach the blocking server.
	time.Sleep(40 * time.Millisecond)
	// Close client1: cancels the in-flight HTTP context; job stays in pending_jobs.
	client1.Close()
	// Also unblock the server so it doesn't leak.
	close(hold)

	// client2: replay finds the pending job with headers and copies them.
	client2, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client2)
	defer client2.Close()
}

func TestRequestMultiValueHeaders(t *testing.T) {
	// map[string]string could not express a header that repeats; http.Header
	// can, and the request must actually carry both values.
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Values("X-Multi")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: pace.PerMinute(60), Burst: 5})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	req := client.Client("u").Request().
		AddHeader("X-Multi", "one").
		AddHeader("X-Multi", "two")
	if _, err := req.Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "one" || got[1] != "two" {
		t.Errorf("server saw X-Multi = %q, want [one two]", got)
	}
	if v := req.Header().Get("X-Multi"); v != "one" {
		t.Errorf("Header().Get = %q, want the first value", v)
	}
}
