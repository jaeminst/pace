// Package pace provides per-user outbound HTTP rate limiting.
//
// Each user gets an independent token bucket, so one user's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the user count.
//
//	lim, err := pace.New(pace.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    rate.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer lim.Close()
//
//	resp, err := lim.Client("alice").Get(ctx, "/items/42")
//
// # How the library is laid out
//
// This package is the front door: it holds [Config], it validates and defaults
// one, and [New] assembles the engine from the result. Everything else a caller
// supplies to a Limiter, or that a Limiter reports back, is a package of its
// own, so that a contract is documented where it is implemented rather than as
// one line in a list of configuration fields:
//
//   - [github.com/jaeminst/pace/limiter] — the Limiter and the request path
//   - [github.com/jaeminst/pace/rate] — rates and quotas
//   - [github.com/jaeminst/pace/store] — the persistence contract, with
//     store/memory a reference implementation and store/storetest the contract
//     as a runnable test suite
//   - [github.com/jaeminst/pace/shared] — the cross-replica quota backend
//   - [github.com/jaeminst/pace/observe] — hooks and counters
//   - [github.com/jaeminst/pace/response] — the response a request returns
//   - [github.com/jaeminst/pace/transport] — HTTP connection tuning
//
// Below those sit the pieces the Limiter is built from. They are public because
// they are worth reading, not because a caller is expected to assemble one:
//
//   - [github.com/jaeminst/pace/bucket] — the token bucket
//   - [github.com/jaeminst/pace/registry] — the sharded user population and its GC
//   - [github.com/jaeminst/pace/persist] — when and how that population is written to a store
//   - [github.com/jaeminst/pace/gate] — the shared-quota decision
//   - [github.com/jaeminst/pace/breaker] — the shared-quota circuit breaker
//   - [github.com/jaeminst/pace/urlx] — request URL construction
//
// The Config of limiter, registry, gate and persist is a vtable rather than an
// option struct: every field is required and each New panics on a value it
// cannot work with rather than defaulting it. [Config] here is the opposite —
// optional fields, validation, defaults — and [New] is the one place the two
// meet. So a vtable is something this package builds, not something a caller
// writes.
//
// Most names here are aliases, not defined types, so a value crosses the
// boundary without conversion: [errors.As] matches a [*LimitError] returned by
// the limiter, and a [github.com/jaeminst/pace/store.Store] you implement
// satisfies what the Limiter asks for. The methods and fields of each aliased
// type are documented in the package that declares it, which is where an alias
// sends you, because Go renders an alias as a single line. [Config], [Clock]
// and [ConfigError] are declared here rather than aliased, because validating
// and defaulting a configuration is this package's job.
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
package pace
