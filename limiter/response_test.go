package limiter_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/rate"
)

func bodyServer(t *testing.T, body []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestMaxResponseBytesRejectsOversizedBody covers the OOM path. Reading an
// unbounded body into memory is how a hostile or merely misbehaving upstream
// takes the process down, and nothing bounded it before.
func TestMaxResponseBytesRejectsOversizedBody(t *testing.T) {
	srv := bodyServer(t, bytes.Repeat([]byte("x"), 4096))

	lim, err := pace.New(pace.Config{
		BaseURL:          srv.URL,
		Rate:             rate.PerMinute(600),
		Burst:            10,
		MaxResponseBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	_, err = lim.Client("alice").Get(context.Background(), "/")
	if !errors.Is(err, pace.ErrBodyTooLarge) {
		t.Errorf("Get with a 4KiB body and a 1KiB cap = %v, want ErrBodyTooLarge", err)
	}
}

// TestMaxResponseBytesAllowsExactlyTheLimit: the cap is inclusive, and a body
// at exactly the limit must not be reported as truncated.
func TestMaxResponseBytesAllowsExactlyTheLimit(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 1024)
	srv := bodyServer(t, payload)

	lim, err := pace.New(pace.Config{
		BaseURL:          srv.URL,
		Rate:             rate.PerMinute(600),
		Burst:            10,
		MaxResponseBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	resp, err := lim.Client("alice").Get(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Body()) != len(payload) {
		t.Errorf("body is %d bytes, want %d", len(resp.Body()), len(payload))
	}
}

func TestMaxResponseBytesUnsetIsUnlimited(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 100_000)
	srv := bodyServer(t, payload)

	lim, _ := newTestLimiterOn(t, srv.URL)
	resp, err := lim.Client("alice").Get(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Body()) != len(payload) {
		t.Errorf("body is %d bytes, want %d", len(resp.Body()), len(payload))
	}
}

// TestStreamDoesNotBufferBody: the escape hatch for responses too large to hold
// in memory. The cap deliberately does not apply, because nothing is buffered.
func TestStreamDoesNotBufferBody(t *testing.T) {
	payload := bytes.Repeat([]byte("y"), 64*1024)
	srv := bodyServer(t, payload)

	lim, err := pace.New(pace.Config{
		BaseURL:          srv.URL,
		Rate:             rate.PerMinute(600),
		Burst:            10,
		MaxResponseBytes: 512, // would reject this body if it were buffered
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	resp, err := lim.Client("alice").Request().Stream(context.Background(), http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	n, err := io.Copy(io.Discard, resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if n != int64(len(payload)) {
		t.Errorf("streamed %d bytes, want %d", n, len(payload))
	}
}

// TestStreamConsumesAToken: streaming is still rate-limited; it is a different
// way to read the response, not a way around the limiter.
func TestStreamConsumesAToken(t *testing.T) {
	srv := bodyServer(t, []byte("ok"))
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6),
		Burst:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	alice := lim.Client("alice")
	resp, err := alice.Request().Stream(context.Background(), http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()

	if got := tokensOf(alice); got > 1.01 {
		t.Errorf("Tokens() = %v after one stream against a burst of 2, want ~1", got)
	}
}

// TestStreamShutdownWaitsForBodyClose: an unread streamed body keeps the
// request in flight, so Shutdown must wait for the caller to close it.
func TestStreamShutdownWaitsForBodyClose(t *testing.T) {
	srv := bodyServer(t, []byte("payload"))
	lim, _ := newTestLimiterOn(t, srv.URL)

	resp, err := lim.Client("alice").Request().Stream(context.Background(), http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}

	returned := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		returned <- lim.Shutdown(ctx)
	}()

	select {
	case err := <-returned:
		t.Fatalf("Shutdown returned (%v) while a streamed body was still open", err)
	case <-time.After(200 * time.Millisecond):
	}

	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-returned:
		if err != nil {
			t.Errorf("Shutdown = %v, want nil once the body was closed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Shutdown did not return after the body was closed")
	}
}

// TestStreamIsObservedAndCounted: a streamed request used to be counted in
// Stats.Requests — acquire does that for every caller — but skipped both
// Stats.Errors and Observer.RequestFinished, so the two halves of the metric
// described different populations.
func TestStreamIsObservedAndCounted(t *testing.T) {
	var got []observe.RequestInfo
	var mu sync.Mutex

	srv := bodyServer(t, []byte("payload"))
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6000),
		Burst:   10,
		Observer: &observe.Observer{
			RequestFinished: func(_ context.Context, info observe.RequestInfo) {
				mu.Lock()
				defer mu.Unlock()
				got = append(got, info)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	resp, err := lim.Client("alice").Request().Stream(context.Background(), http.MethodGet, "/thing")
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Body.Close(); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("RequestFinished fired %d times for a stream, want 1", len(got))
	}
	if got[0].Method != http.MethodGet || got[0].Path != "/thing" {
		t.Errorf("observed %s %s, want GET /thing", got[0].Method, got[0].Path)
	}
	if got[0].Status != http.StatusOK {
		t.Errorf("Status = %d, want 200", got[0].Status)
	}
	if got[0].Err != nil {
		t.Errorf("Err = %v, want nil", got[0].Err)
	}
}

// TestStreamCountsTransportErrors: the counters must agree with each other. A
// failed stream is a failed request.
func TestStreamCountsTransportErrors(t *testing.T) {
	lim, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1", // refuses connections
		Rate:    rate.PerMinute(6000),
		Burst:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	resp, err := lim.Client("alice").Request().Stream(context.Background(), http.MethodGet, "/")
	if err == nil {
		_ = resp.Body.Close()
		t.Fatal("Stream against a closed port reported success")
	}

	st := lim.Stats()
	if st.Requests != 1 {
		t.Errorf("Requests = %d, want 1", st.Requests)
	}
	if st.Errors != 1 {
		t.Errorf("Errors = %d, want 1: a failed stream is a failed request", st.Errors)
	}
}

func TestResponseOK(t *testing.T) {
	// 1xx is not testable through a real server: net/http treats it as an
	// informational response and follows it with the implicit 200.
	tests := map[int]bool{200: true, 204: true, 299: true, 300: false, 404: false, 500: false}
	for code, want := range tests {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(code)
		}))
		lim, _ := newTestLimiterOn(t, srv.URL)
		resp, err := lim.Client("alice").Get(context.Background(), "/")
		srv.Close()
		if err != nil {
			t.Fatalf("status %d: %v", code, err)
		}
		if got := resp.OK(); got != want {
			t.Errorf("status %d: OK() = %v, want %v", code, got, want)
		}
	}
}

func TestResponseJSON(t *testing.T) {
	srv := bodyServer(t, []byte(`{"name":"alice","count":3}`))
	lim, _ := newTestLimiterOn(t, srv.URL)

	resp, err := lim.Client("alice").Get(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	if err := resp.JSON(&got); err != nil {
		t.Fatal(err)
	}
	if got.Name != "alice" || got.Count != 3 {
		t.Errorf("decoded %+v, want {alice 3}", got)
	}

	if err := resp.JSON(&struct{ Name int }{}); err == nil {
		t.Error("JSON accepted a body that does not fit the target type")
	}
}

func TestRequestSetJSON(t *testing.T) {
	var gotBody []byte
	var gotType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, _ := newTestLimiterOn(t, srv.URL)
	payload := map[string]any{"action": "create"}
	if _, err := lim.Client("alice").Request().SetJSON(payload).Post(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(gotBody), `"action":"create"`) {
		t.Errorf("server received %q", gotBody)
	}
	if gotType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotType)
	}
}

// TestRequestSetJSONDeferredError: an encoding failure surfaces from the
// terminal call, and costs no token because the request is never built.
func TestRequestSetJSONDeferredError(t *testing.T) {
	srv := bodyServer(t, []byte("ok"))
	// A frozen clock: with a live one the bucket refills between the two
	// readings, and "did this spend a token" is no longer an exact comparison.
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL, Rate: rate.PerMinute(6), Burst: 2, Clock: newFakeClock(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	alice := lim.Client("alice")
	if !alice.Allow(context.Background()) {
		t.Fatal("could not prime the bucket")
	}
	before := tokensOf(alice)

	_, err = alice.Request().SetJSON(make(chan int)).Post(context.Background(), "/")
	if err == nil {
		t.Fatal("SetJSON with an unencodable value reported success")
	}
	if after := tokensOf(alice); after != before {
		t.Errorf("a request that could not be encoded spent a token: %v then %v", before, after)
	}
}

func TestResponseRetryAfter(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   time.Duration
		ok     bool
	}{
		{"absent", "", 0, false},
		{"delta seconds", "120", 2 * time.Minute, true},
		{"zero seconds", "0", 0, true},
		{"negative seconds", "-5", 0, false},
		{"garbage", "soon", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.header != "" {
					w.Header().Set("Retry-After", tt.header)
				}
				w.WriteHeader(http.StatusTooManyRequests)
			}))
			defer srv.Close()

			lim, _ := newTestLimiterOn(t, srv.URL)
			resp, err := lim.Client("alice").Get(context.Background(), "/")
			if err != nil {
				t.Fatal(err)
			}
			got, ok := resp.RetryAfter()
			if ok != tt.ok {
				t.Fatalf("RetryAfter ok = %v, want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("RetryAfter = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestResponseRetryAfterHTTPDate: the header's other legal form. An absolute
// time is reported relative to now, and a date already past means "now".
func TestResponseRetryAfterHTTPDate(t *testing.T) {
	for _, tc := range []struct {
		name  string
		when  time.Time
		check func(time.Duration) bool
	}{
		{"future", time.Now().Add(time.Hour), func(d time.Duration) bool { return d > 55*time.Minute && d <= time.Hour }},
		{"past", time.Now().Add(-time.Hour), func(d time.Duration) bool { return d == 0 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", tc.when.UTC().Format(http.TimeFormat))
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer srv.Close()

			lim, _ := newTestLimiterOn(t, srv.URL)
			resp, err := lim.Client("alice").Get(context.Background(), "/")
			if err != nil {
				t.Fatal(err)
			}
			got, ok := resp.RetryAfter()
			if !ok {
				t.Fatal("RetryAfter did not parse an HTTP-date")
			}
			if !tc.check(got) {
				t.Errorf("RetryAfter = %v, outside the expected range", got)
			}
		})
	}
}

// TestRequestTimeoutBoundsTheRoundTrip, and excludes the wait for a token: a
// request held back by throttling has not started yet.
func TestRequestTimeoutBoundsTheRoundTrip(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	defer close(release)

	lim, err := pace.New(pace.Config{
		BaseURL:        srv.URL,
		Rate:           rate.PerMinute(600),
		Burst:          10,
		RequestTimeout: 150 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	start := time.Now()
	_, err = lim.Client("alice").Get(context.Background(), "/")
	if err == nil {
		t.Fatal("a request against a server that never answers reported success")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("the request took %v; RequestTimeout did not bound it", elapsed)
	}
}
