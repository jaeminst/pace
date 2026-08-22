// example_test.go holds the one example godoc renders under this package's
// identifiers. LimitError is declared here, so its example belongs here — even
// though provoking a throttle means making a request, which is what
// github.com/jaeminst/pace/client is for.
//
// The rest of the examples moved to client/ and config/ with the types they
// name. An example attaches to the type it names, and one naming a type this
// package does not declare renders nowhere at all.

package limiter_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

// examplePool builds a Pool against srv, keeping the boilerplate out of the
// example itself.
func examplePool(srv *httptest.Server, tweak func(*config.Config)) *client.Pool {
	cfg := config.Config{BaseURL: srv.URL, QuotaFor: config.Fixed(bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10})}
	if tweak != nil {
		tweak(&cfg)
	}
	pool, err := client.New(cfg)
	must(err)
	return pool
}

// must panics rather than calling log.Fatal, which would skip the deferred
// cleanup the example relies on. Nothing here can fail in practice.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ExampleLimitError shows how to tell throttling apart from any other failure,
// and how long the caller would have had to wait.
func ExampleLimitError() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	// A frozen clock, because the assertion below is on an exact string. Delay
	// is measured at the point of failure, so against the wall clock a pause of
	// half a second anywhere between the two calls turns "10s" into "9s" — and
	// an Example compares stdout exactly, with no tolerance band.
	lim := examplePool(srv, func(c *config.Config) {
		c.QuotaFor = config.Fixed(bucket.Quota{Rate: bucket.PerMinute(6), Burst: 1})
		c.Clock = newFakeClock()
	})
	defer func() { _ = lim.Close() }()

	ctx := context.Background()
	alice := lim.Client("alice")
	_, err := alice.Get(ctx, "/")
	must(err)

	// The burst is spent and a refill is ten seconds away.
	deadlined, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()

	_, err = alice.Get(deadlined, "/")
	var le *limiter.LimitError
	switch {
	case errors.Is(err, limiter.ErrClosed):
		fmt.Println("limiter is shutting down")
	case errors.As(err, &le):
		fmt.Printf("throttled: %s would need about %v\n", le.UserID, le.Delay.Round(time.Second))
	}
	// Output:
	// throttled: alice would need about 10s
}

// ExamplePool_Stats reads the counters a dashboard would scrape.
