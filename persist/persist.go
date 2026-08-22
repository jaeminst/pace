// Package persist is the persistence half of the user registry.
//
// It sits between github.com/jaeminst/pace/store, the contract a caller
// implements, and github.com/jaeminst/pace/registry, whose four persistence
// fields it fills. The split is the one the registry itself draws: the registry
// decides which users exist and when they are evicted, and holds the shard
// locks while doing it; this decides what persisting one *means* — when state
// is written at all, how long a write may take, whether it goes out as a batch,
// and what happens when the store says no.
//
// That is why the [store.BatchStore] assertion lives here rather than in the
// registry: it is a question about the caller's store, asked at the moment of
// the write.
package persist

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/store"
)

// Config is what an Adapter needs from its owner. It is plain values only, so
// this package never imports the one that builds it.
type Config struct {
	// Store is the caller's backend. A nil Store disables persistence, which is
	// the default configuration and not an error: [Adapter.Persists] then
	// reports false and the registry never asks for anything else.
	Store store.Store

	// Shadowed reports that a shared quota is configured. It suppresses
	// persistence for the reason [Adapter.Persists] gives, and is separate from
	// Store so that the two answers stay independent: a store may be configured
	// and still, correctly, never written to.
	Shadowed bool

	// Timeout bounds each store operation. Required when Store is non-nil.
	Timeout time.Duration

	// Logger receives the failures this package swallows. Required when Store
	// is non-nil.
	Logger *slog.Logger
}

// Adapter fills the persistence fields of [registry.Config].
//
// It holds no state of its own. Everything it decides is a function of Config
// and the argument in hand, which is what lets an owner rebuild one rather than
// reach inside it when the backing store changes.
type Adapter struct{ cfg Config }

// New builds an Adapter. It panics on a Config it cannot work with, naming the
// field, in the manner of [registry.New]: this is a vtable for one caller, so a
// zero field is a nil dereference on the first request rather than a default.
func New(cfg Config) *Adapter {
	if cfg.Store != nil {
		switch {
		case cfg.Timeout <= 0:
			panic("persist: Timeout must be positive when Store is set")
		case cfg.Logger == nil:
			panic("persist: Logger is required when Store is set")
		}
	}
	return &Adapter{cfg: cfg}
}

// Persists reports whether per-user token state should be written to and read
// from the store.
//
// A shared quota turns the local bucket into a shadow, and a shadow must never
// be persisted. The bucket no longer describes what this user has spent — it
// describes what this replica has spent, which is a fraction of it. Restoring
// replica A's snapshot into replica B would have B throttling itself for
// traffic it never sent, and the inequality that makes the shadow safe
// (shadowTokens >= sharedTokens) is exactly what that breaks.
//
// The authoritative count lives in the backend, which is the point of
// configuring one.
func (a *Adapter) Persists() bool { return a.cfg.Store != nil && !a.cfg.Shadowed }

// Load reads a user's persisted state, if any. A store error is logged and
// treated as "no saved state": a fresh bucket is the safe fallback, and failing
// the request because persistence is unavailable would be worse than briefly
// granting a full burst.
func (a *Adapter) Load(ctx context.Context, userID string) (registry.Snapshot, bool) {
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	st, found, err := a.cfg.Store.Load(ctx, userID)
	if err != nil {
		a.cfg.Logger.Warn("pace: load user state", "user", userID, "err", err)
		return registry.Snapshot{}, false
	}
	return registry.Snapshot{UserID: userID, Tokens: st.Tokens, LastUsed: st.LastUsed}, found
}

// Save persists one user and reports whether it worked. It backs the eviction
// of a single user, whose contract is that state is written by the time it
// returns, so unlike [Adapter.Flush] it neither swallows the error nor detaches
// the context.
func (a *Adapter) Save(ctx context.Context, s registry.Snapshot) error {
	ctx, cancel := context.WithTimeout(ctx, a.cfg.Timeout)
	defer cancel()
	if err := a.cfg.Store.Save(ctx, s.UserID, store.State{Tokens: s.Tokens, LastUsed: s.LastUsed}); err != nil {
		return fmt.Errorf("pace: evict %q: %w", s.UserID, err)
	}
	return nil
}

// chunk bounds each batch round-trip so one sweep cannot monopolise the store.
const chunk = 512

// Flush persists snapshots with no lock held. Stores that can write a batch in
// one transaction do; the rest fall back to one call per user, still outside
// every lock.
//
// The batch capability is discovered per call rather than resolved once, so a
// store swapped in after construction is honoured.
//
// It runs on context.Background rather than the owner's context: the final
// flush happens after the owner has been cancelled, and inheriting a cancelled
// context would discard exactly the state a clean shutdown exists to save.
// Config.Timeout is what bounds it instead.
func (a *Adapter) Flush(snaps []registry.Snapshot) {
	if !a.Persists() || len(snaps) == 0 {
		return
	}
	if bs, ok := a.cfg.Store.(store.BatchStore); ok {
		batch := make([]store.UserState, 0, min(chunk, len(snaps)))
		for start := 0; start < len(snaps); start += chunk {
			batch = batch[:0]
			for _, sn := range snaps[start:min(start+chunk, len(snaps))] {
				batch = append(batch, store.UserState{
					UserID: sn.UserID,
					State:  store.State{Tokens: sn.Tokens, LastUsed: sn.LastUsed},
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Timeout)
			err := bs.SaveBatch(ctx, batch)
			cancel()
			if err != nil {
				a.cfg.Logger.Warn("pace: flush state", "count", len(batch), "err", err)
			}
		}
		return
	}
	for _, sn := range snaps {
		ctx, cancel := context.WithTimeout(context.Background(), a.cfg.Timeout)
		err := a.cfg.Store.Save(ctx, sn.UserID, store.State{Tokens: sn.Tokens, LastUsed: sn.LastUsed})
		cancel()
		if err != nil {
			a.cfg.Logger.Warn("pace: flush state", "user", sn.UserID, "err", err)
		}
	}
}
