// Package shared is the contract for rate limiting across replicas.
//
// Implement [Quota] against a backend every process consults — Redis, a
// database, anything that can decrement a counter atomically — and supply it as
// github.com/jaeminst/pace.Config.Shared. The local bucket stays, as a
// shadow that can only refuse: it never grants a request the backend has not
// also granted, so it costs nothing in correctness and saves a round-trip for
// every request a replica can already tell is over its own share.
//
// Read [Quota] and [ErrorPolicy] before adopting this. Most callers who want
// "distributed rate limiting" are better served by setting a local rate to
// their share of the limit and handling 429s honestly; this trades an
// operational dependency on every outbound call path for accuracy that only
// matters when replicas are unevenly loaded.
//
// Run github.com/jaeminst/pace/shared/quotatest against an implementation
// before trusting it.
package shared
