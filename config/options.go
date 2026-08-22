// options.go is the part of a configuration that cannot be written down.
//
// [Config] is a struct of values: a caller fills it in, [Config.Resolve] checks
// it, and everything in it is knowable before the process serves a request. An
// option is the other kind of setting — behaviour, supplied as code, decided
// per key while running. Keeping the two apart is what lets Resolve be a real
// check rather than a check of the fields that happen to be values.
//
// Options live here rather than in client or limiter because both New
// functions take them, and a second copy of the type is a second place for the
// two to drift.

package config

import "github.com/jaeminst/pace/bucket"

// Options is the resolved form of the [Option] list a New was given. A caller
// does not build one; [Apply] does, and the two New functions read it.
type Options struct {
	// QuotaFor grades one key against the configured default. Nil — the usual
	// case — gives every key [Config.Quota].
	QuotaFor func(key string, def bucket.Quota) bucket.Quota
}

// Option is a setting passed to
// [github.com/jaeminst/pace/client.New] or
// [github.com/jaeminst/pace/limiter.New] rather than written in a [Config].
type Option func(*Options)

// Apply folds a list of options into one value.
func Apply(opts []Option) Options {
	var o Options
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

// WithQuotaFor grades keys into tiers: a free one and a paying one, or one
// customer with a negotiated ceiling.
//
// It is handed the key and the quota [Config.Quota] resolved to, and returns
// the quota that key gets. Taking the default as an argument is what keeps this
// from being a second place a rate is configured — there is no precedence rule
// to remember and no zero field that means "inherit", because the value being
// overridden is right there in the signature:
//
//	var tiers atomic.Pointer[map[string]bucket.Quota]  // replaced whole, never mutated
//
//	client.New(cfg, config.WithQuotaFor(
//		func(key string, def bucket.Quota) bucket.Quota {
//			if q, ok := (*tiers.Load())[key]; ok {
//				return q
//			}
//			return def
//		}))
//
// The Quota it returns is used as written. A rate that is zero, negative or
// NaN is one no bucket can refill from, so that key is throttled to a
// standstill and the Limiter logs it at warn level; a burst below one is raised
// to one. Unlike [Config.Quota] this cannot be checked by [Config.Resolve],
// because it produces its answer one key at a time long after Resolve has run.
//
// It is consulted when a key's bucket is created — its first request, or the
// first after an eviction — never on the hot path, and never while a shard lock
// is held. Keep it to a map lookup even so; it must not do I/O.
//
// **It must be safe for concurrent use.** It is called from request goroutines,
// one per key whose bucket is being created, and from whatever goroutine calls
// ReloadQuotas, possibly at the same instant. A plain map read here against a
// plain map write elsewhere is a data race, and it is the one this invites, so
// guard whatever it reads.
//
// To change a rate while the process runs, update whatever this reads and then
// call the Limiter's ReloadQuotas, or its ReloadQuota for a single key. A key
// with no bucket yet picks the new value up without a reload, because it is
// about to call this.
func WithQuotaFor(fn func(key string, def bucket.Quota) bucket.Quota) Option {
	return func(o *Options) { o.QuotaFor = fn }
}
