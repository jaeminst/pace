// Package pace provides per-user outbound HTTP rate limiting.
//
// Each user gets an independent token bucket, so one user's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the user count.
//
// A [Limiter] owns the shared machinery — the buckets, the state store, the
// durable queue, the GC goroutine — and is what you create and close:
//
//	lim, err := pace.New(pace.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    pace.PerMinute(60),
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
// non-nil response; check [Response.OK] or [Response.StatusCode] instead.
//
// Throttling returns a [*LimitError] carrying the user, the limit in force, and
// how long the wait would have been. [ErrClosed] means the Limiter itself is
// shutting down — a distinct condition, and one an earlier version confused
// with throttling.
//
// # Compatibility
//
// From v1.0.0 the exported API follows semantic import versioning: no exported
// identifier changes meaning or signature within v1.
//
// Explicitly not covered by that promise:
//
//   - anything under internal/
//   - the SQLite schema and the on-disk format of the durable queue, which
//     migrate forward automatically but are not a stable interface
//   - new fields appearing in [Config], [Observer], [Stats], and the Info
//     structs; construct them with field names, not positionally
//   - the exact text of error messages
//   - benchmark numbers, which are machine-specific
package pace
