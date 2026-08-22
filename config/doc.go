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
// # It holds no vtable
//
// github.com/jaeminst/pace/limiter.Spec is the engine's own required-everything
// vtable, and it is not here even though it is configuration. It cannot be:
// its Quota field returns a [Quota], so the engine imports this package, so
// this package cannot import the engine. A `func (Config) Spec() limiter.Spec`
// is the obvious API and the one Go forbids — client.New performs that
// translation instead.
//
// [ADR 0007]: https://github.com/jaeminst/pace/blob/main/docs/adr/0007-contracts-carry-numbers-not-types.md
package config
