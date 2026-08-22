// Package limiter is the rate limiter, and only that.
//
// It paces work per user: a token bucket each, a sharded population with
// idle-user GC, an optional cross-replica quota, and a lifecycle that closes
// all of it down. It does not import net/http. There is no base URL here, no
// HTTP client, no request and no response — making requests is
// github.com/jaeminst/pace/client, and what a caller configures is
// github.com/jaeminst/pace/config.
//
// The seam is exactly the methods in api.go, and it is worth stating what that
// buys: this package can pace anything.
//
// # Every method takes a user ID
//
// A [Limiter] is the whole population rather than one member of it, so there is
// no handle bound to an identity here — that is client.Client, and it is what a
// caller normally holds. Ask the engine directly and you name the user each
// time:
//
//	if err := lim.Wait(ctx, "alice"); err != nil {
//	    return err
//	}
//	sendTheEmail()
//
// [Limiter.Enter] is the one method that is not a question about a user. It
// registers work against the shutdown barrier and hands back a context bounded
// by the Limiter's own lifetime, so that [Limiter.Close] and [Limiter.Shutdown]
// mean something for work the engine does not perform itself. A caller whose
// work must outlive its own return — a streamed body, say — passes the returned
// func on rather than deferring it.
//
// # Build one through the front door
//
// [New] takes a [github.com/jaeminst/pace/config.Spec]: a vtable, not a set of
// options — every field required, nothing defaulted, and New panics on a value
// it cannot work with. Both that type and the Config it is resolved from live in
// [github.com/jaeminst/pace/config], and
// [github.com/jaeminst/pace/client.New] is what does the resolving:
//
//	pool, err := client.New(config.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    config.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer pool.Close()
//
//	lim := pool.Limiter() // this package's Limiter
//
// Reach for limiter.New directly only when you are assembling the pieces
// yourself and have already decided every value it asks for.
// client.Pool.Limiter is the shorter road to the same object.
//
// # Errors
//
// [*LimitError] is throttling and [ErrClosed] is shutdown; the two used to be
// confused with each other, and a non-2xx response is neither. The whole of it
// is under "Errors" in github.com/jaeminst/pace, which is where a caller reads
// it, and stating it twice is how the two copies come to disagree.
//
// # The rest of the library
//
// Everything a caller supplies to a Limiter, or that it reports back, lives in
// a package of its own — github.com/jaeminst/pace lists them.
package limiter
