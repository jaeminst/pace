// Package memory is an in-memory [store.Store].
//
// It is a reference implementation and a test double, not persistence. Nothing
// it holds survives the process, so a Limiter backed by one restores nothing on
// restart — which is the single thing a real store is for. Use it to exercise
// code that takes a store, and read it as the shortest correct answer to "what
// does implementing the contract involve".
//
// It is a [store.BatchStore] as well, because the batch path is a real branch
// in the flush and deserves to be reachable without a database.
//
// It has no Close, deliberately. Close is optional — pace discovers it by type
// assertion — and a map has nothing to release. A Close that emptied the map
// would also be the wrong shape: closing a store releases a handle, it does not
// destroy what the store holds.
package memory

import (
	"context"
	"sync"

	"github.com/jaeminst/pace/store"
)

// Store keeps per-user token state in a map. The zero value is not usable; call
// [New].
type Store struct {
	mu    sync.RWMutex
	state map[string]store.State
}

var (
	_ store.Store      = (*Store)(nil)
	_ store.BatchStore = (*Store)(nil)
)

// New returns an empty Store, safe for concurrent use.
func New() *Store { return &Store{state: make(map[string]store.State)} }

// Save records one user's state, replacing anything held for that key.
func (s *Store) Save(ctx context.Context, userID string, st store.State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state[userID] = st
	return nil
}

// SaveBatch records every state in one pass. It is all-or-nothing only in the
// sense that a cancelled context writes nothing: there is no failure a map
// write can produce partway through.
func (s *Store) SaveBatch(ctx context.Context, states []store.UserState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range states {
		s.state[u.UserID] = u.State
	}
	return nil
}

// Load returns a user's saved state. A user who was never saved reports found
// as false and no error — not finding someone is not a failure.
func (s *Store) Load(ctx context.Context, userID string) (store.State, bool, error) {
	if err := ctx.Err(); err != nil {
		return store.State{}, false, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	st, ok := s.state[userID]
	return st, ok, nil
}

// Len reports how many users have saved state. It is here for tests that assert
// a flush happened at all, which otherwise have to reach for Load in a loop.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.state)
}
