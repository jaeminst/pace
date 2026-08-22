// Package config is everything a caller of pace configures.
//
// [Config] is the struct they write: a base URL, a rate and a burst, and about
// a dozen optional fields with documented defaults. [Config.Resolve] checks one
// and fills the optional fields in, and it is what
// [github.com/jaeminst/pace/client.New] calls before assembling anything.
//
//	cfg := config.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    bucket.PerMinute(60),
//	    Burst:   10,
//	}
//
// # Where the rate vocabulary lives
//
// Not here. `Limit`, `Quota` and the constructors `PerSecond`, `PerMinute`,
// `PerHour` and `Every` belong to [github.com/jaeminst/pace/bucket], because a
// Quota is a rate and a ceiling — which is what a bucket *is* — and the type a
// caller writes has to be the same type a bucket reports, or there are two
// spellings of one pair.
//
// So a Config literal names two packages:
//
//	config.Config{BaseURL: "…", Rate: bucket.PerMinute(60), Burst: 10}
//
// That is a real cost and it was weighed. What it buys is that `Bucket.Quota()`
// returns one value instead of two loose numbers a caller reassembles, and that
// the caller-facing quota and the enforced quota cannot drift into different
// types. The alternative — vocabulary here, bucket reporting numbers — put two
// hand-written conversions on the path every throttle report takes.
//
// It costs nothing a third party pays. `bucket` imports nothing of pace's, so
// it stays reachable from every layer, and the contract packages implemented
// against from outside — store, shared, observe — still carry plain float64 and
// int per [ADR 0007].
//
// # One configuration type
//
// [Config] is the only one. A caller writes it, [Config.Resolve] validates and
// defaults it, and every package that needs configuring takes it directly —
// [github.com/jaeminst/pace/limiter.New] and
// [github.com/jaeminst/pace/client.New] both.
//
// There was a `Spec` here, a required-everything vtable the engine took, and
// resolving a Config into one was ten lines of `Field: cfg.Field`. Ten of its
// ten fields were the same field under the same name, so the type restated the
// Config and the method restated the type. Deleting both is what
// `limiter.New(cfg)` buys — see [ADR 0009].
//
// What the vtable did buy was a compiler guarantee: it carried no base URL and
// no transport, so the engine could not read one. That is a test now
// (limiter/httpfree_test.go), which is the honest trade — the property is worth
// keeping and it was not worth a second struct.
//
// [ADR 0007]: https://github.com/jaeminst/pace/blob/main/docs/adr/0007-contracts-carry-numbers-not-types.md
// [ADR 0009]: https://github.com/jaeminst/pace/blob/main/docs/adr/0009-config-limiter-client.md
package config
