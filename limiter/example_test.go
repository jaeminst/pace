// example_test.go holds the examples godoc renders under this package's
// identifiers, which since v0.11.0 is all of them but one: the root declares
// Config and nothing else, so an example naming a Limiter, a Client or a
// Response belongs here. An example attaches to the type it names, and naming
// one the package does not have renders it nowhere at all.
package limiter_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/limiter"
)

// exampleLimiter builds a Limiter against srv, keeping the boilerplate out of
// the examples themselves.
func exampleLimiter(srv *httptest.Server, tweak func(*pace.Config)) *limiter.Limiter {
	cfg := pace.Config{BaseURL: srv.URL, Rate: limiter.PerMinute(60), Burst: 10}
	if tweak != nil {
		tweak(&cfg)
	}
	lim, err := pace.New(cfg)
	must(err)
	return lim
}

// must panics rather than calling log.Fatal, which would skip the deferred
// cleanup these examples rely on. Nothing here can fail in practice.

// must panics rather than calling log.Fatal, which would skip the deferred
// cleanup these examples rely on. Nothing here can fail in practice.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ExampleLimiter_Client shows the shape of the API: one Limiter owns the
// machinery, and each user gets a lightweight handle with its own quota.

// ExampleLimiter_Client shows the shape of the API: one Limiter owns the
// machinery, and each user gets a lightweight handle with its own quota.
func ExampleLimiter_Client() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim := exampleLimiter(srv, func(c *pace.Config) { c.Burst = 1; c.Rate = limiter.PerMinute(6) })
	defer func() { _ = lim.Close() }()

	ctx := context.Background()
	alice, bob := lim.Client("alice"), lim.Client("bob")

	// Alice spends her only token.
	_, err := alice.Get(ctx, "/")
	must(err)
	fmt.Println("alice can send again:", alice.Allow(context.Background()))
	fmt.Println("bob is unaffected:", bob.Allow(context.Background()))
	// Output:
	// alice can send again: false
	// bob is unaffected: true
}

// ExampleClient_Wait paces work that pace does not perform itself.

// ExampleClient_Wait paces work that pace does not perform itself.
func ExampleClient_Wait() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	lim := exampleLimiter(srv, nil)
	defer func() { _ = lim.Close() }()

	// Wait blocks until this user has a token, then consumes it. Use it when
	// the request is made by something other than pace.
	must(lim.Client("alice").Wait(context.Background()))
	fmt.Println("cleared to send")
	// Output:
	// cleared to send
}

// ExampleLimitError shows how to tell throttling apart from any other failure,
// and how long the caller would have had to wait.

// ExampleLimitError shows how to tell throttling apart from any other failure,
// and how long the caller would have had to wait.
func ExampleLimitError() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	// A frozen clock, because the assertion below is on an exact string. Delay
	// is measured at the point of failure, so against the wall clock a pause of
	// half a second anywhere between the two calls turns "10s" into "9s" — and
	// an Example compares stdout exactly, with no tolerance band.
	lim := exampleLimiter(srv, func(c *pace.Config) {
		c.Burst = 1
		c.Rate = limiter.PerMinute(6)
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

// ExampleLimiter_Stats reads the counters a dashboard would scrape.

// ExampleLimiter_Stats reads the counters a dashboard would scrape.
func ExampleLimiter_Stats() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	lim := exampleLimiter(srv, nil)
	defer func() { _ = lim.Close() }()

	ctx := context.Background()
	for _, user := range []string{"alice", "bob"} {
		_, err := lim.Client(user).Get(ctx, "/")
		must(err)
	}

	s := lim.Stats()
	fmt.Printf("users=%d requests=%d errors=%d\n", s.Users, s.Requests, s.Errors)
	// Output:
	// users=2 requests=2 errors=0
}

// ExampleLimiter_Shutdown drains in-flight requests before closing.

// ExampleLimiter_Shutdown drains in-flight requests before closing.
func ExampleLimiter_Shutdown() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	lim := exampleLimiter(srv, nil)

	_, err := lim.Client("alice").Get(context.Background(), "/")
	must(err)

	// On SIGTERM: give in-flight requests five seconds to finish. The store is
	// flushed whether or not the deadline is met.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := lim.Shutdown(ctx); err != nil {
		fmt.Println("shutdown forced:", err)
		return
	}
	fmt.Println("drained cleanly")
	// Output:
	// drained cleanly
}

// ExampleLimiter_ReloadQuotas changes a tier while the process is running.
// Rebuilding the Limiter would also work and would drop every user's accrued
// tokens on the floor; this keeps them.

// ExampleLimiter_ReloadQuotas changes a tier while the process is running.
// Rebuilding the Limiter would also work and would drop every user's accrued
// tokens on the floor; this keeps them.
func ExampleLimiter_ReloadQuotas() {
	tiers := map[string]limiter.Quota{"trial-42": {Rate: limiter.PerMinute(6), Burst: 1}}

	lim, err := pace.New(pace.Config{
		BaseURL:  "https://api.example.com",
		Rate:     limiter.PerMinute(60),
		Burst:    5,
		QuotaFor: func(userID string) limiter.Quota { return tiers[userID] },
	})
	must(err)
	defer lim.Close()

	user := lim.Client("trial-42")
	user.Allow(context.Background()) // brings the bucket into memory
	fmt.Println("before:", user.Quota().Burst)

	// The trial converted. Update whatever QuotaFor reads, then reload.
	tiers["trial-42"] = limiter.Quota{Rate: limiter.PerMinute(600), Burst: 50}
	lim.ReloadQuotas()

	fmt.Println("after:", user.Quota().Burst)
	// Output:
	// before: 1
	// after: 50
}

// ExampleClient_Reserve shows the middle ground between Allow, which refuses
// rather than waits, and Wait, which waits and cannot give the token back.

// ExampleClient_Reserve shows the middle ground between Allow, which refuses
// rather than waits, and Wait, which waits and cannot give the token back.
func ExampleClient_Reserve() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	lim := exampleLimiter(srv, func(c *pace.Config) { c.Burst = 1; c.Rate = limiter.PerMinute(6) })
	defer func() { _ = lim.Close() }()

	alice := lim.Client("alice")
	alice.Allow(context.Background()) // spend the burst

	const tolerable = time.Second
	r := alice.Reserve(context.Background())
	if !r.OK() || r.Delay() > tolerable {
		// Hand the token back: this request is not going to happen, and the
		// user should not be charged for it.
		r.Cancel()
		fmt.Printf("skipped: the wait would have been about %v\n", r.Delay().Round(time.Second))
		return
	}
	fmt.Println("proceeding after", r.Delay())
	// Output:
	// skipped: the wait would have been about 10s
}

func ExampleClient_Get() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	lim := exampleLimiter(srv, nil)
	defer func() { _ = lim.Close() }()

	resp, err := lim.Client("user-123").Get(context.Background(), "/items/42")
	must(err)
	fmt.Printf("status: %d\n", resp.StatusCode())
	// Output:
	// status: 200
}

func ExampleClient_Request() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	lim := exampleLimiter(srv, nil)
	defer func() { _ = lim.Close() }()

	// Building the request costs nothing; the rate-limit token is taken when
	// Post runs, so an abandoned builder does not burn the user's quota.
	resp, err := lim.Client("user-456").Request().
		SetHeader("X-Request-ID", "req-001").
		SetBody([]byte(`{"action":"create"}`)).
		Post(context.Background(), "/resources")
	must(err)
	fmt.Printf("status: %d, request-id: %s\n", resp.StatusCode(), resp.Header().Get("X-Request-ID"))
	// Output:
	// status: 201, request-id: req-001
}

// ExampleConfig_quotaFor grades users against a default. An unlisted user gets
// the zero Quota, which selects Config.Rate and Config.Burst — so a map is a
// complete implementation, with no "if missing" branch to write.

// ExampleResponse_RetryAfter reads upstream's own statement of its limit, which
// beats any guess a client could make. It is the number this library's readers
// care most about: you throttle outbound requests because upstream limits you,
// and this is upstream saying by how much.
//
// A non-2xx response is not an error in pace — the round-trip succeeded — so
// check [limiter.Response.OK] and then ask.
func ExampleResponse_RetryAfter() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	lim := exampleLimiter(srv, nil)
	defer lim.Close()

	resp, err := lim.Client("alice").Get(context.Background(), "/orders")
	must(err)
	if !resp.OK() {
		if after, ok := resp.RetryAfter(); ok {
			fmt.Printf("upstream asked us to wait %v\n", after)
		}
	}
	// Output:
	// upstream asked us to wait 30s
}
