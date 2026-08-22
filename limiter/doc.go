// Package limiter is the engine of pace: per-user outbound HTTP rate limiting.
//
// What it does and why is documented at github.com/jaeminst/pace, the package a
// caller imports. This one is the machinery behind that.
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
//	    Rate:    PerMinute(60),
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
// # Errors
//
// [*LimitError] is throttling and [ErrClosed] is shutdown; the two used to be
// confused with each other, and a non-2xx response is neither. The whole of it
// is under "Errors" in github.com/jaeminst/pace, which is where a caller reads
// it, and stating it twice is how the two copies come to disagree.
//
// # The rest of the library
//
// This package holds the Limiter and the request path. Everything a caller
// supplies to it, or that it reports back, lives in a package of its own —
// github.com/jaeminst/pace lists them.
package limiter
