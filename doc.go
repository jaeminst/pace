// Package pace provides per-user outbound HTTP rate limiting.
//
// Each user gets an independent token bucket, so one user's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the user count.
//
//	import (
//	    "github.com/jaeminst/pace"
//	    "github.com/jaeminst/pace/limiter"
//	)
//
//	lim, err := pace.New(pace.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    limiter.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer lim.Close()
//
//	resp, err := lim.Client("alice").Get(ctx, "/items/42")
//
// # This package is the front door, and only that
//
// It holds [Config] — the fields a caller writes, their validation and their
// defaults — and [New], which resolves one into the vtable the engine takes.
// That is the whole of it. Three declarations and a function.
//
// Everything you touch after New is named in
// [github.com/jaeminst/pace/limiter]: the Limiter that New returns, the Client
// you bind a user identity to, the Request you build, the Response you read,
// and the rate vocabulary — Limit, Quota, PerMinute, Inf. So a caller imports
// two packages rather than one.
//
// That is a deliberate trade, and the thing bought is that **every name in this
// library is declared exactly once**. Go renders a type alias as a single line
// with no methods and no fields, so a re-exported Limiter documented nothing
// and sent the reader one package over anyway. An alias is for a type whose
// owner is elsewhere; it is not a way to publish a name in two places. See
// [ADR 0008].
//
// # The rest of the library
//
// Everything else you supply to a Limiter, or that it reports back, is a
// package of its own, so that a contract is documented where it is implemented
// rather than as one line in a list of configuration fields:
//
//   - [github.com/jaeminst/pace/limiter] — the Limiter, the request path, and
//     the rate vocabulary
//   - [github.com/jaeminst/pace/store] — the persistence contract, with
//     store/memory a reference implementation and store/storetest the contract
//     as a runnable test suite
//   - [github.com/jaeminst/pace/shared] — the cross-replica quota backend
//   - [github.com/jaeminst/pace/observe] — hooks and counters
//   - [github.com/jaeminst/pace/transport] — HTTP connection tuning
//
// Below those sit the pieces the engine is built from. They are public because
// they are worth reading, not because a caller is expected to assemble one:
//
//   - [github.com/jaeminst/pace/bucket] — the token bucket
//   - [github.com/jaeminst/pace/registry] — the sharded user population and its GC
//   - [github.com/jaeminst/pace/gate] — the shared-quota decision
//   - [github.com/jaeminst/pace/breaker] — the shared-quota circuit breaker
//   - [github.com/jaeminst/pace/urlx] — request URL construction
//
// limiter.Spec, and the Spec of registry and gate, are vtables rather than
// option structs: every field is required and each New panics on a value it
// cannot work with rather than defaulting it. [Config] here is the opposite —
// optional fields, validation, defaults — and [New] is the one place the two
// meet. So a vtable is something this package builds, not something a caller
// writes.
//
// # Errors
//
// A non-2xx response is not an error. A 404 is a successful round-trip, and
// reporting it as a failure would mean returning a non-nil error alongside a
// non-nil response; check Response.OK or Response.StatusCode instead.
//
// Throttling returns a [github.com/jaeminst/pace/limiter.LimitError] carrying
// the user, the limit in force, and how long the wait would have been.
// limiter.ErrClosed means the Limiter itself is shutting down — a distinct
// condition, and one an earlier version confused with throttling.
//
// # Compatibility
//
// From v1.0.0 the exported API follows semantic import versioning: no exported
// identifier changes meaning or signature within v1. Below v1.0.0 any release
// may break the API; the freeze begins at v1.0.0.
//
// Explicitly not covered by that promise:
//
//   - new fields appearing in any exported struct — construct them with field
//     names, not positionally. That is how the API is meant to grow.
//   - the exact text of error messages
//   - benchmark numbers, which are machine-specific
//
// [github.com/jaeminst/pace/store.State] is the one exception, and it is
// frozen. It is the wire format between pace and a third-party store: adding a
// field to it would compile everywhere and silently break every existing store,
// which would persist the fields it knew and hand back a zero for the new one.
// A break that compiles is worse than one that does not.
//
// [ADR 0008]: https://github.com/jaeminst/pace/blob/main/docs/adr/0008-the-root-re-exports-nothing.md
package pace
