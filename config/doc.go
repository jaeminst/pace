// Package config is everything a caller of pace configures, and the vocabulary
// they configure it in.
//
// [Config] is the struct they write: a base URL, a rate and a burst, and about
// a dozen optional fields with documented defaults. [Config.Resolve] checks one
// and fills the optional fields in, and it is what
// [github.com/jaeminst/pace/client.New] calls before assembling anything.
//
//	cfg := config.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    config.PerMinute(60),
//	    Burst:   10,
//	}
//
// # Why the rate vocabulary lives here
//
// [Limit], [Quota] and the constructors [PerSecond], [PerMinute], [PerHour] and
// [Every] are here rather than with the engine because a rate is something a
// caller writes, and writing one should not mean naming a second package in the
// middle of a Config literal.
//
// It is worth saying what that does *not* cost, because a package everything
// shares is how pace/rate went wrong before it was deleted in v0.9.0. Only two
// packages import this one: the engine and the HTTP client, both of them pace's
// own. The contract packages a third party implements against — store, shared,
// observe — carry plain float64 and int and import nothing of pace's, which is
// the decision recorded in [ADR 0007]. The rule that follows: the vocabulary
// may live wherever it reads best, provided no package a third party implements
// against has to compile it.
//
// # Two configuration types, and the one line between them
//
// [Config] is what a caller writes: optional fields, validated and defaulted by
// [Config.Resolve]. [Spec] is what the rate limiter requires: every field, no
// defaults, [Spec.Validate] panicking on a value it cannot use. [Config.Spec]
// is the translation, and it is the whole of the difference between the two.
//
// Both live here on purpose. The engine imports this package for [Quota], and
// nothing in [Spec] names an engine type, so there is no cycle in either
// direction — which makes `func (Config) Spec() Spec` an ordinary method rather
// than the impossible one it looks like. (v0.12.0 claimed the opposite. What is
// actually forbidden is `func (Config) Spec() limiter.Spec`, a method naming a
// type in the package that imports this one; moving the type here is a
// different move, and a legal one. See
// [ADR 0009].)
//
// The names are the house rule: options are `Config`, vtables are `Spec`.
//
// [ADR 0007]: https://github.com/jaeminst/pace/blob/main/docs/adr/0007-contracts-carry-numbers-not-types.md
// [ADR 0009]: https://github.com/jaeminst/pace/blob/main/docs/adr/0009-config-limiter-client.md
package config
