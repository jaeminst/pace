// Package store is the persistence contract for per-key token state.
//
// A Limiter is in-memory by default. Implement [Store] to keep a key's tokens
// across restarts and idle-user eviction, and supply it as
// github.com/jaeminst/pace.Config.Store — Redis, Postgres, DynamoDB, or
// anything else that can hold two numbers under a key.
//
// pace ships no implementation. [github.com/jaeminst/pace/store/memory] is a
// reference one and [github.com/jaeminst/pace/store/storetest] is this contract
// as a runnable test suite, which is what to check a real backend against.
package store
