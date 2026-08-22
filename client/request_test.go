package client_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

func TestGet(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	resp, err := pool.Client("u1").Get(context.Background(), "/")
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

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	req := pool.Client("u1").Request()
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

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	for _, tc := range []struct {
		name string
		call func(*client.Request) (*client.Response, error)
		want string
	}{
		{"Post", func(r *client.Request) (*client.Response, error) { return r.Post(context.Background(), "/") }, "POST"},
		{"Put", func(r *client.Request) (*client.Response, error) { return r.Put(context.Background(), "/") }, "PUT"},
		{"Delete", func(r *client.Request) (*client.Response, error) { return r.Delete(context.Background(), "/") }, "DELETE"},
		{"Patch", func(r *client.Request) (*client.Response, error) { return r.Patch(context.Background(), "/") }, "PATCH"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := pool.Client("u1").Request()
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
	// Exercise pool.Post, pool.Put, pool.Delete, pool.Patch directly
	// (the convenience wrappers on *Client, not *Request).
	var gotMethod atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod.Store(r.Method)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() (*client.Response, error)
		want string
	}{
		{"Post", func() (*client.Response, error) { return pool.Client("u").Post(ctx, "/") }, "POST"},
		{"Put", func() (*client.Response, error) { return pool.Client("u").Put(ctx, "/") }, "PUT"},
		{"Delete", func() (*client.Response, error) { return pool.Client("u").Delete(ctx, "/") }, "DELETE"},
		{"Patch", func() (*client.Response, error) { return pool.Client("u").Patch(ctx, "/") }, "PATCH"},
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
	// pool so the `if err != nil { return nil, err }` lines are covered.
	pool, err := client.New(config.Config{
		BaseURL:  "http://127.0.0.1:1",
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close() // closed → Request returns ErrClosed

	ctx := context.Background()
	for _, tc := range []struct {
		name string
		call func() (*client.Response, error)
	}{
		{"Post", func() (*client.Response, error) { return pool.Client("u").Post(ctx, "/") }},
		{"Put", func() (*client.Response, error) { return pool.Client("u").Put(ctx, "/") }},
		{"Delete", func() (*client.Response, error) { return pool.Client("u").Delete(ctx, "/") }},
		{"Patch", func() (*client.Response, error) { return pool.Client("u").Patch(ctx, "/") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := tc.call()
			if !errors.Is(err, limiter.ErrClosed) {
				t.Fatalf("want ErrClosed, got %v", err)
			}
		})
	}
}

func TestErrClosed(t *testing.T) {
	pool, err := client.New(config.Config{
		BaseURL:  "http://x",
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()

	err = pool.Client("u").Wait(context.Background())
	if !errors.Is(err, limiter.ErrClosed) {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

// TestErrClosedOnTheBuilderPath is the same barrier reached by a different
// route. A Request defers its work to the terminal method, so a Limiter closed
// after the builder was obtained has to be reported there rather than at
// Client.Request — which never fails and never blocks.
func TestErrClosedOnTheBuilderPath(t *testing.T) {
	pool, err := client.New(config.Config{
		BaseURL:  "http://x",
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := pool.Client("alice").Request()
	pool.Close()

	if _, err := req.Do(context.Background(), "GET", "/"); !errors.Is(err, limiter.ErrClosed) {
		t.Fatalf("errors.Is(err, limiter.ErrClosed) was false for %#v", err)
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

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	req := pool.Client("u1").Request()
	payload := []byte(`{"hello":"world"}`)
	if _, err := req.SetBody(payload).Post(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotBody, payload) {
		t.Fatalf("want body %q, got %q", payload, gotBody)
	}
}

func TestErrClosed_Concurrent(t *testing.T) {
	pool, err := client.New(config.Config{
		BaseURL:  "http://127.0.0.1:0",
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 100}),
	})
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n + 1)

	go func() {
		defer wg.Done()
		pool.Close()
	}()
	for i := range n {
		go func(id int) {
			defer wg.Done()
			_ = pool.Client(fmt.Sprintf("u%d", id)).Wait(context.Background())
		}(i)
	}
	wg.Wait()
}

func TestHTTPError_StatusCode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	resp, err := pool.Client("alice").Get(context.Background(), "/fail")
	if err != nil {
		t.Fatalf("unexpected error for HTTP 500: %v", err)
	}
	if resp.StatusCode() != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", resp.StatusCode())
	}
}

func TestRequest_BuildURLError(t *testing.T) {
	// A path with a null byte causes http.NewRequestWithContext to fail.
	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL:  srv.URL,
		QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	req := pool.Client("u").Request()
	// Null byte in the path makes NewRequestWithContext return an error.
	_, err = req.Get(context.Background(), "/\x00bad")
	if err == nil {
		t.Fatal("expected error for URL with null byte")
	}
}

func TestRequest_TransportError(t *testing.T) {
	// Inject a transport that always returns an error to cover pool.Do failure.
	transportErr := errors.New("dial refused")
	pool, err := client.New(config.Config{
		BaseURL:   "http://127.0.0.1:1",
		QuotaFor:  config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
		Transport: failTransport{err: transportErr},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, err = pool.Client("u").Get(context.Background(), "/")
	if err == nil {
		t.Fatal("expected transport error")
	}
}

func TestRequest_BodyReadError(t *testing.T) {
	// Inject a transport whose response body errors on Read.
	pool, err := client.New(config.Config{
		BaseURL:   "http://127.0.0.1:1",
		QuotaFor:  config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6000)}),
		Transport: errBodyTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	_, err = pool.Client("u").Get(context.Background(), "/")
	if err == nil {
		t.Fatal("expected body read error")
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

	pool, err := client.New(config.Config{BaseURL: srv.URL, QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60), Burst: 5})})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	req := pool.Client("u").Request().
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

// The two below are Request's own query builder — AddQuery appends where
// SetQueryValues replaces. What used to sit beside them, about how a URL is
// built, is urlx/urlx_test.go now.
// urlEcho reports back the exact target the server received.
func urlEcho(t *testing.T) (*httptest.Server, func() string) {
	t.Helper()
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() string { return got }
}

func TestAddQueryKeepsMultipleValues(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	_, err := lim.Client("alice").Request().
		AddQuery("tag", "red").
		AddQuery("tag", "blue").
		Get(context.Background(), "/items")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.ParseRequestURI(got())
	tags := parsed.Query()["tag"]
	if len(tags) != 2 || tags[0] != "red" || tags[1] != "blue" {
		t.Errorf("tag = %q, want both values", tags)
	}
}

func TestSetQueryValuesReplacesWholesale(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	_, err := lim.Client("alice").Request().
		SetQuery("dropped", "yes").
		SetQueryValues(url.Values{"kept": {"1"}}).
		Get(context.Background(), "/items")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.ParseRequestURI(got())
	q := parsed.Query()
	if q.Has("dropped") {
		t.Error("SetQueryValues did not replace the earlier parameters")
	}
	if q.Get("kept") != "1" {
		t.Errorf("query = %v, want kept=1", q)
	}
}
