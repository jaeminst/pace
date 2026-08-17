package sqlite

import (
	"context"
	"time"

	"github.com/jaeminst/pace/store"
)

// StateStore views a [Store] as a [store.Store], the persistence contract a
// Limiter reads and writes per-user token state through.
//
// It lives here rather than with the Limiter because it is a statement about
// this backend, not about the caller's configuration: it is the one adapter
// pace ships, and the translation it performs — [store.State]'s time.Time to
// the Unix nanoseconds this package stores — is a fact about the schema.
//
// Earlier versions met user-supplied stores at a private interface with a
// wrapper bridging them, which made the batteries-included path a special case.
// Being just another store.Store means per-user state takes one code path
// whichever backend is configured.
type StateStore struct{ s *Store }

var (
	_ store.Store      = StateStore{}
	_ store.BatchStore = StateStore{}
)

// NewStateStore adapts s. The returned value shares the underlying database, so
// closing either closes both.
func NewStateStore(s *Store) StateStore { return StateStore{s: s} }

// Save implements [store.Store].
func (a StateStore) Save(ctx context.Context, userID string, st store.State) error {
	return a.s.Save(ctx, userID, st.Tokens, st.LastUsed.UnixNano())
}

// SaveBatch implements [store.BatchStore], writing every row in one
// transaction.
func (a StateStore) SaveBatch(ctx context.Context, states []store.UserState) error {
	rows := make([]UserState, len(states))
	for i, u := range states {
		rows[i] = UserState{
			UserID:   u.UserID,
			Tokens:   u.State.Tokens,
			LastUsed: u.State.LastUsed.UnixNano(),
		}
	}
	return a.s.SaveBatch(ctx, rows)
}

// Load implements [store.Store]. A user with no saved row reports found as
// false and no error.
func (a StateStore) Load(ctx context.Context, userID string) (store.State, bool, error) {
	ss, found, err := a.s.Load(ctx, userID)
	if err != nil || !found {
		return store.State{}, found, err
	}
	return store.State{Tokens: ss.Tokens, LastUsed: time.Unix(0, ss.LastUsed)}, true, nil
}

// Close closes the underlying database, since the two share it.
func (a StateStore) Close() error { return a.s.Close() }
