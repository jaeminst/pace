// example_test.go holds the examples godoc renders under this package's
// identifiers: a Pool, a Client, a Request, a Response. An example attaches to
// the type it names, and naming one the package does not declare renders it
// nowhere at all — so ExampleLimitError lives in limiter/ and
// ExampleConfig_Quota in config/, beside the types they are about.
package client_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
)

// exampleLimiter builds a Limiter against srv, keeping the boilerplate out of
// the examples themselves.
func exampleLimiter(srv *httptest.Server, tweak func(*config.Config)) *client.Pool {
	cfg := config.Config{BaseURL: srv.URL, Rate: config.PerMinute(60), Burst: 10}
	if tweak != nil {
		tweak(&cfg)
	}
	lim, err := client.New(cfg)
	must(err)
	return lim
}

// must panics rather than calling log.Fatal, which would skip the deferred
// cleanup these examples rely on. Nothing here can fail in practice.
func must(err error) {
	if err != nil {
		panic(err)
	}
}

// ExamplePool_Client shows the shape of the API: one Limiter owns the
// machinery, and each user gets a lightweight handle with its own quota.
func ExamplePool_Client() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim := exampleLimiter(srv, func(c *config.Config) { c.Burst = 1; c.Rate = config.PerMinute(6) })
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

func ExamplePool_Stats() {
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

// ExamplePool_Shutdown drains in-flight requests before closing.
func ExamplePool_Shutdown() {
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

// ExamplePool_ReloadQuotas changes a tier while the process is running.
// Rebuilding the Pool would also work and would drop every user's accrued
// tokens on the floor; this keeps them.
//
// Note the atomic.Pointer. QuotaFor is called from request goroutines, so the
// table it reads cannot be a plain map that another goroutine writes — that is
// a data race. Replacing the whole map behind a pointer keeps the read a single
// load and the write a single store.
func ExamplePool_ReloadQuotas() {
	var tiers atomic.Pointer[map[string]config.Quota]
	tiers.Store(&map[string]config.Quota{"trial-42": {Rate: config.PerMinute(6), Burst: 1}})

	pool, err := client.New(config.Config{
		BaseURL:  "https://api.example.com",
		Rate:     config.PerMinute(60),
		Burst:    5,
		QuotaFor: func(userID string) config.Quota { return (*tiers.Load())[userID] },
	})
	must(err)
	defer pool.Close()

	user := pool.Client("trial-42")
	user.Allow(context.Background()) // brings the bucket into memory
	fmt.Println("before:", user.Quota().Burst)

	// The trial converted. Swap the table, then reload.
	tiers.Store(&map[string]config.Quota{"trial-42": {Rate: config.PerMinute(600), Burst: 50}})
	pool.ReloadQuotas()

	fmt.Println("after:", user.Quota().Burst)
	// Output:
	// before: 1
	// after: 50
}

// ExampleClient_Reserve shows the middle ground between Allow, which refuses
// rather than waits, and Wait, which waits and cannot give the token back.
func ExampleClient_Reserve() {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	lim := exampleLimiter(srv, func(c *config.Config) { c.Burst = 1; c.Rate = config.PerMinute(6) })
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
// beats any guess a pool could make. It is the number this library's readers
// care most about: you throttle outbound requests because upstream limits you,
// and this is upstream saying by how much.
//
// A non-2xx response is not an error in pace — the round-trip succeeded — so
// check [client.Response.OK] and then ask.
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
