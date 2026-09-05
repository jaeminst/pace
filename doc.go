// Package pace provides per-key outbound HTTP rate limiting.
//
// Each key gets an independent token bucket, so one key's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the key count.
//
// # This package declares nothing
//
// That is deliberate, and it is why the Index below is empty. pace is three
// packages, and this path exists to say which one you want:
//
//	import (
//	    "github.com/jaeminst/pace/bucket"
//	    "github.com/jaeminst/pace/client"
//	    "github.com/jaeminst/pace/config"
//	)
//
//	pool, err := client.New(config.Config{
//	    BaseURL: "https://api.example.com",
//	    Quota:   bucket.NewQuota("60/m", 10),
//	})
//	if err != nil { log.Fatal(err) }
//	defer pool.Close()
//
//	resp, err := pool.Client("alice").Get(ctx, "/items/42")
//
// # The three
//
//   - [github.com/jaeminst/pace/config] — everything you configure: the Config
//     struct, its validation and its defaults.
//   - [github.com/jaeminst/pace/bucket] — the token bucket, and the vocabulary
//     for describing one: Quota, Limit, PerMinute, Inf. You write a rate in
//     these and a limiter reports one back in the same types.
//   - [github.com/jaeminst/pace/limiter] — the rate limiter, and only that:
//     token buckets, the sharded key population and its GC, the cross-replica
//     quota, the lifecycle. It does not import net/http.
//   - [github.com/jaeminst/pace/client] — creating and managing clients, and
//     the request path. A Pool owns a limiter and mints a Client per key.
//
// Each name is declared once, in the package whose job it is. There are no
// aliases anywhere in this module, and there is nothing here to re-export them
// from — which is the point. Go renders a type alias as a single line with no
// methods and no fields, so a convenience re-export documents nothing and sends
// the reader one package over anyway.
//
// # What you supply, and what you get back
//
// Everything a caller hands a limiter, or that a limiter reports, is a package
// of its own, so a contract is documented where it is implemented rather than
// as one line in a list of configuration fields:
//
//   - [github.com/jaeminst/pace/store] — the persistence contract, with
//     store/memory a reference implementation and store/storetest the contract
//     as a runnable test suite
//   - [github.com/jaeminst/pace/shared] — the cross-replica quota backend
//   - [github.com/jaeminst/pace/observe] — hooks and counters
//
// Below those sit the remaining pieces the engine is built from. They are public
// because they are worth reading, not because you are expected to assemble one:
// [github.com/jaeminst/pace/registry], [github.com/jaeminst/pace/gate],
// [github.com/jaeminst/pace/breaker] and [github.com/jaeminst/pace/urlx].
//
// The Spec of registry and gate are vtables rather than option structs: every
// field required, and a value they cannot work with panics where it is written
// rather than being defaulted. So a vtable is something the library builds, not
// something a caller writes. config.Config is the opposite and is the only
// configuration a caller ever fills in — limiter.New and client.New both take
// it directly.
//
// # Errors
//
// A non-2xx response is not an error. A 404 is a successful round-trip, and
// reporting it as a failure would mean returning a non-nil error alongside a
// non-nil response; check Response.OK or Response.StatusCode instead.
//
// Throttling returns a [github.com/jaeminst/pace/limiter.LimitError] carrying
// the key, the limit in force, and how long the wait would have been.
// limiter.ErrClosed means the limiter itself is shutting down — a distinct
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
package pace
