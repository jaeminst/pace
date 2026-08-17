// Package limiter is the engine of pace: per-user outbound HTTP rate limiting.
//
// Each user gets an independent token bucket, so one user's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the user count.
//
// A [Limiter] owns the shared machinery — the buckets, the state store, the
// durable queue, the GC goroutine — and is what you create and close:
//
//	lim, err := limiter.New(limiter.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    limit.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer lim.Close()
//
// A [Client] is a lightweight handle bound to one user identity. Derive as many
// as you need; they all share the Limiter's state:
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
// A non-2xx response is not an error. A 404 is a successful round-trip, and
// reporting it as a failure would mean returning a non-nil error alongside a
// non-nil response; check Response.OK or Response.StatusCode instead.
//
// Throttling returns a [*LimitError] carrying the user, the limit in force, and
// how long the wait would have been. [ErrClosed] means the Limiter itself is
// shutting down — a distinct condition, and one an earlier version confused
// with throttling.
//
// # The rest of the library
//
// This package holds the Limiter and the request path. Everything a caller
// supplies to it, or that it reports back, lives in a package of its own, so
// that each contract is documented where it is implemented rather than buried
// in a list of configuration fields:
//
//   - github.com/jaeminst/pace/limit — rates and quotas
//   - github.com/jaeminst/pace/store — the persistence contract
//   - github.com/jaeminst/pace/shared — the cross-replica quota backend
//   - github.com/jaeminst/pace/observe — hooks and counters
//   - github.com/jaeminst/pace/queue — the durable queue's configuration
//   - github.com/jaeminst/pace/transport — HTTP connection tuning
//
// The root package github.com/jaeminst/pace re-exports the handful of names in
// this one, so that the common case needs a single import.
package limiter
