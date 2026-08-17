package limiter_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	pace "github.com/jaeminst/pace/limiter"
)

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

	waiting := make(chan struct{})
	pace.SetBeforeWaitHook(client, sync.OnceFunc(func() { close(waiting) }))

	errCh := make(chan error, 1)
	go func() {
		// This will block waiting for a token.
		err := client.Client("u").Wait(ctx)
		errCh <- err
	}()

	<-waiting // the goroutine is genuinely blocked, not merely started
	client.Close()

	err = <-errCh
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestConcurrentFirstRequestsShareOneUser(t *testing.T) {
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

	raceDone := make(chan struct{})
	go func() {
		defer close(raceDone)
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
	<-raceDone      // A finished; the double-check branch has run
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

	waiting := make(chan struct{})
	pace.SetBeforeWaitHook(client, sync.OnceFunc(func() { close(waiting) }))

	errCh := make(chan error, 1)
	go func() {
		err := client.Client("u").Wait(ctx2)
		errCh <- err
	}()

	<-waiting
	cancel()

	err = <-errCh
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if errors.Is(err, pace.ErrClosed) {
		t.Fatalf("expected ctx cancellation error, not ErrClosed; got %v", err)
	}
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
