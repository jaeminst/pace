// testhelpers_test.go is the fixtures the request-path tests share.
//
// Most of them are duplicated from limiter/testhelpers_test.go, which is what
// Go's test packages force: an external test package cannot export to another
// one. The alternative — a shared pacetest package — is worse here, because the
// coverage command filters `test$` out of -coverpkg and would silently stop
// measuring it. Each copy is kept to the minimum this package actually calls.

package client_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
)

// build resolves cfg, applies the options, and returns a Pool that closes
// itself when the test ends.
func build(t *testing.T, cfg config.Config, opts ...func(*config.Config)) *client.Pool {
	t.Helper()
	for _, o := range opts {
		o(&cfg)
	}
	pool, err := client.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pool.Close() })
	return pool
}

// newTestLimiterOn builds a Pool pointed at an existing server, for tests that
// need a handler of their own. The rate is high and the burst is deep because
// these tests are about the request path rather than about throttling.
func newTestLimiterOn(t *testing.T, baseURL string, opts ...func(*config.Config)) (*client.Pool, string) {
	t.Helper()
	return build(t, config.Config{BaseURL: baseURL, Quota: bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 100}}, opts...), baseURL
}

// newEchoServer answers every request with 200 and the method it received, so a
// test can assert which verb went out without writing a handler.
func newEchoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Method", r.Method)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
}

// tokensOf drops the comma-ok for tests that only care about the count.
func tokensOf(c *client.Client) float64 {
	n, _ := c.Tokens()
	return n
}

// fakeClock is an injectable Clock whose Now never moves on its own. Tests use
// it where a refill must not happen between two assertions.
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

// failTransport is an http.RoundTripper that always returns an error, which is
// how a transport failure is reached without a server that can produce one.
type failTransport struct{ err error }

func (f failTransport) RoundTrip(*http.Request) (*http.Response, error) { return nil, f.err }

// errBodyTransport returns a 200 whose body errors on Read — the one failure
// that arrives after the status line, so nothing earlier stands in for it.
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
