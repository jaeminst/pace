// Package client is the rate-limited HTTP client: it creates clients, manages
// them, and performs the round-trips they ask for.
//
// A [Pool] is what you create once per upstream. It owns the engine and the
// HTTP machinery, and it mints a [Client] per user identity — a lightweight
// handle that shares the Pool's buckets and store.
//
//	pool, err := client.New(config.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    bucket.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer pool.Close()
//
//	resp, err := pool.Client("alice").Get(ctx, "/items/42")
//
// [New] is where the library is assembled: it resolves a
// [github.com/jaeminst/pace/config.Config], hands it to the engine, and keeps
// the four HTTP fields the engine has no use for. So this is the only package in
// pace that knows what HTTP is.
//
// The rate limiter underneath is [github.com/jaeminst/pace/limiter], reachable
// with [Pool.Limiter]. Reach for it to pace work this package does not perform
// on your behalf — a database write, an SDK call — without holding a Client
// whose base URL you never use.
//
// # Errors
//
// A non-2xx response is not an error. A 404 is a successful round-trip, and
// reporting it as a failure would mean returning a non-nil error alongside a
// non-nil response; check [Response.OK] or [Response.StatusCode] instead.
//
// Throttling is a [github.com/jaeminst/pace/limiter.LimitError] and shutdown is
// limiter.ErrClosed, because both are the engine's answers rather than this
// package's. [ErrBodyTooLarge] is declared here, because reading a body is
// something only this package does.
package client
