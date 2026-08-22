// Package config is everything a caller of pace configures.
//
// [Config] is the struct they write: a base URL, a quota, and about a dozen
// optional fields with documented defaults. [Config.Resolve] checks the two
// required ones and fills the rest in, and it is what
// [github.com/jaeminst/pace/client.New] calls before assembling anything.
//
//	cfg := config.Config{
//	    BaseURL: "https://api.example.com",
//	    Quota:   bucket.NewQuota("60/m", 10),
//	}
//
// # Values here, behaviour in options
//
// Everything in [Config] is a value: something a caller writes down, and
// something [Config.Resolve] can check before a request is served. [Config.Quota]
// is the rate, and it is a field rather than a hook precisely so that a rate of
// zero is a construction error rather than one key silently throttled to a
// standstill three hours in.
//
// Behaviour goes in an [Option] instead — see [WithQuotaFor], which grades keys
// into tiers. An option cannot be checked by Resolve, because it produces its
// answer per key while running; keeping the two kinds apart is what lets Resolve
// be a real check rather than a check of the fields that happen to be values.
//
// The two do not compete. WithQuotaFor is handed [Config.Quota] as its default
// argument, so there is no precedence rule and no zero field that means
// "inherit" — the value being overridden is in the signature.
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
//	config.Config{BaseURL: "…", Quota: bucket.NewQuota("60/m", 10)}
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
