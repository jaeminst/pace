package limiter

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/store"
)

// persistence is the persistence half of the key registry: what the
// registry's four persistence fields are wired to.
//
// It sits between github.com/jaeminst/pace/store, the contract a caller
// implements, and github.com/jaeminst/pace/registry, whose fields it fills. The
// split is the one the registry itself draws: the registry decides which keys
// exist and when they are evicted, and holds the shard locks while doing it;
// this decides what persisting one *means* — when state is written at all, how
// long a write may take, whether it goes out as a batch, and what happens when
// the store says no. That is why the [store.BatchStore] assertion is here
// rather than in the registry: it is a question about the caller's store, asked
// at the moment of the write. The registry still never sees a store.
//
// It holds no state of its own. Everything it decides is a function of these
// values and the argument in hand, which is what lets the Limiter rebuild one
// rather than reach inside it when the backing store changes.
//
// This was its own package once. Every one of the seven names it
// exported existed so that one caller could wire one value, which is not what a
// package boundary is for.
type persistence struct {
	// store is the caller's backend. A nil store disables persistence, which is
	// the default configuration and not an error: persists then
	// reports false and the registry never asks for anything else.
	store store.Store

	// shadowed reports that a shared quota is configured. It suppresses
	// persistence for the reason persists gives, and is separate from
	// store so that the two answers stay independent: a store may be configured
	// and still, correctly, never written to.
	shadowed bool

	// timeout bounds each store operation. Required when store is non-nil.
	timeout time.Duration

	// logger receives the failures this package swallows. Required when Store
	// is non-nil.
	logger *slog.Logger
}

// persists reports whether per-key token state should be written to and read
// from the store.
//
// A shared quota turns the local bucket into a shadow, and a shadow must never
// be persisted. The bucket no longer describes what this key has spent — it
// describes what this replica has spent, which is a fraction of it. Restoring
// replica A's snapshot into replica B would have B throttling itself for
// traffic it never sent, and the inequality that makes the shadow safe
// (shadowTokens >= sharedTokens) is exactly what that breaks.
//
// The authoritative count lives in the backend, which is the point of
// configuring one.
func (a *persistence) persists() bool { return a.store != nil && !a.shadowed }

// load reads a key's persisted state, if any. A store error is logged and
// treated as "no saved state": a fresh bucket is the safe fallback, and failing
// the request because persistence is unavailable would be worse than briefly
// granting a full burst.
func (a *persistence) load(ctx context.Context, key string) (registry.Snapshot, bool) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	st, found, err := a.store.Load(ctx, key)
	if err != nil {
		a.logger.Warn("pace: load key state", "key", key, "err", err)
		return registry.Snapshot{}, false
	}
	return registry.Snapshot{Key: key, Tokens: st.Tokens, LastUsed: st.LastUsed}, found
}

// save persists one key and reports whether it worked. It backs the eviction
// of a single key, whose contract is that state is written by the time it
// returns, so unlike flush it neither swallows the error nor detaches
// the context.
func (a *persistence) save(ctx context.Context, s registry.Snapshot) error {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()
	if err := a.store.Save(ctx, s.Key, store.State{Tokens: s.Tokens, LastUsed: s.LastUsed}); err != nil {
		return fmt.Errorf("pace: evict %q: %w", s.Key, err)
	}
	return nil
}

// chunk bounds each batch round-trip so one sweep cannot monopolise the store.
const chunk = 512

// flush persists snapshots with no lock held. Stores that can write a batch in
// one transaction do; the rest fall back to one call per key, still outside
// every lock.
//
// The batch capability is discovered per call rather than resolved once, so a
// store swapped in after construction is honoured.
//
// It runs on context.Background rather than the owner's context: the final
// flush happens after the owner has been cancelled, and inheriting a cancelled
// context would discard exactly the state a clean shutdown exists to save.
// The timeout below is what bounds it instead.
func (a *persistence) flush(snaps []registry.Snapshot) {
	if !a.persists() || len(snaps) == 0 {
		return
	}
	if bs, ok := a.store.(store.BatchStore); ok {
		batch := make([]store.KeyState, 0, min(chunk, len(snaps)))
		for start := 0; start < len(snaps); start += chunk {
			batch = batch[:0]
			for _, sn := range snaps[start:min(start+chunk, len(snaps))] {
				batch = append(batch, store.KeyState{
					Key:   sn.Key,
					State: store.State{Tokens: sn.Tokens, LastUsed: sn.LastUsed},
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
			err := bs.SaveBatch(ctx, batch)
			cancel()
			if err != nil {
				a.logger.Warn("pace: flush state", "count", len(batch), "err", err)
			}
		}
		return
	}
	for _, sn := range snaps {
		ctx, cancel := context.WithTimeout(context.Background(), a.timeout)
		err := a.store.Save(ctx, sn.Key, store.State{Tokens: sn.Tokens, LastUsed: sn.LastUsed})
		cancel()
		if err != nil {
			a.logger.Warn("pace: flush state", "key", sn.Key, "err", err)
		}
	}
}
