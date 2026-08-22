// Package limiter is the engine of pace: per-user outbound HTTP rate limiting.
//
// What the library does and why is documented at github.com/jaeminst/pace, the
// package that holds [github.com/jaeminst/pace.Config]. This one is everything
// else — the token buckets, the sharded user population and its GC, the
// shared-quota decision, the lifecycle, and the request path that consults all
// of it.
//
// Since v0.11.0 the root re-exports nothing, so every name a caller touches
// after New is here: [Limiter], [Client], [Request], [Response], [Limit],
// [Quota], [Reservation], [LimitError], [Inf], [PerMinute] and the rest of the
// vocabulary. Each is declared once, which is the point — an alias renders in
// godoc as a single line with no methods, so publishing these names twice
// documented them nowhere.
//
// # Build one through the front door
//
// [New] takes a [Spec] that is a vtable, not a set of options: every field is
// required, nothing is defaulted, and New panics on a value it cannot work
// with. That is deliberate — the configuration a caller writes, with its
// optional fields and its validation, is github.com/jaeminst/pace.Config, and
// github.com/jaeminst/pace.New is what resolves one into the other:
//
//	lim, err := pace.New(pace.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    limiter.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer lim.Close()
//
// Reach for limiter.New directly only when you are assembling the pieces
// yourself and have already decided every value it asks for.
//
// A [Limiter] owns the shared machinery — the buckets, the state store, the GC
// goroutine — and is what you create and close. A [Client] is a lightweight
// handle bound to one user identity. Derive as many as you need; they all share
// the Limiter's state:
//
//	alice := lim.Client("alice")
//	bob := lim.Client("bob")
//
//	resp, err := alice.Get(ctx, "/items/42")
//	resp, err = bob.Get(ctx, "/items/99")
//
// Because a Client has no lifecycle of its own, shutting the service down is
// unambiguously a Limiter operation: [Limiter.Close] or [Limiter.Shutdown].
//
// # Pacing work pace does not perform
//
// [Client.Wait] blocks for a token and [Client.Allow] takes one without
// blocking, neither of which sends anything. A Client whose BaseURL is never
// used is a perfectly good pacer for a database write or an SDK call.
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
