// Package store is the persistence contract for per-user token state.
//
// A Limiter is in-memory by default. Implement [Store] to keep a user's tokens
// across restarts and idle-user eviction, and supply it as
// github.com/jaeminst/pace/limiter.Config.Store — Redis, Postgres, DynamoDB, or
// anything else that can hold two numbers under a key.
//
// Two methods, both about persistence. A store that also needs tearing down
// implements io.Closer, which the Limiter discovers by type assertion, the same
// way [BatchStore] extends [Store]. Neither is required.
package store
