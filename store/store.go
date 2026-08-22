package store

import (
	"context"
	"time"
)

// State is the persisted snapshot of a single key's token bucket. It is the
// element type exchanged between a Limiter and a [Store].
type State struct {
	// Tokens is the bucket's token count at LastUsed. It may be fractional.
	Tokens float64
	// LastUsed is when the key last made a request.
	LastUsed time.Time
}

// KeyState pairs a key with its state, for stores that write in batches.
type KeyState struct {
	Key   string
	State State
}

// Store persists per-key token state across process restarts and GC
// evictions. Implement it to use any backend (Redis, Postgres, DynamoDB, …)
// and supply it via pace.Config.Store.
//
// Every method receives a context bounded by pace.Config.StoreTimeout, so a
// backend that talks over a network can honour cancellation rather than block
// the caller indefinitely.
//
// Two methods, both about persistence. A store that also needs tearing down
// implements [io.Closer], which Limiter.Close discovers by type assertion —
// the same way [BatchStore] extends this interface. Close used to be a member
// here, which forced every implementation to carry one whether it had resources
// or not; the README's own example wrote `func (r *RedisStore)
// Close() error { return nil }` because the interface demanded it.
//
// pace ships no implementation. github.com/jaeminst/pace/store/memory is a
// reference one — a map, correct and useless for persistence — and
// github.com/jaeminst/pace/store/storetest is the contract as an executable
// test suite, which is the thing to run a real backend against.
type Store interface {
	// Save persists state for key.
	Save(ctx context.Context, key string, state State) error
	// Load returns the saved state for key. Returning (State{}, false, nil)
	// when nothing is stored is valid and expected for a first-time key.
	Load(ctx context.Context, key string) (State, bool, error)
}

// BatchStore is an optional extension to [Store]. A store that
// implements it receives whole batches from the idle-user sweep and from the
// final flush on close, instead of one call per key.
//
// Implementing it matters: the sweep can evict thousands of keys at once, and
// a round-trip each turns a background task into a sustained load spike.
type BatchStore interface {
	Store
	// SaveBatch persists every entry, or reports an error. Partial success
	// should be reported as an error so the caller can log it.
	SaveBatch(ctx context.Context, states []KeyState) error
}
