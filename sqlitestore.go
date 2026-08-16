package pace

import (
	"context"
	"time"

	"github.com/jaeminst/pace/internal/store"
)

// sqliteStateStore adapts the built-in SQLite backend to the public
// [StateStore] interface.
//
// This is the only adapter in the package. Previously the built-in backend and
// user-supplied stores met at a private interface with a wrapper bridging them,
// which meant the batteries-included path was a special case. Making SQLite
// just another StateStore means per-user state takes one code path whichever
// backend is configured.
//
// It is used only when [Config.Store] is unset. With both fields set, the
// SQLite file serves the durable queue and this adapter is not built, so
// user_state stays empty and the caller's Store owns every read and write.
type sqliteStateStore struct{ s *store.Store }

var (
	_ StateStore      = sqliteStateStore{}
	_ BatchStateStore = sqliteStateStore{}
)

func (a sqliteStateStore) Save(ctx context.Context, userID string, st State) error {
	return a.s.Save(ctx, userID, st.Tokens, st.LastUsed.UnixNano())
}

func (a sqliteStateStore) SaveBatch(ctx context.Context, states []UserState) error {
	rows := make([]store.UserState, len(states))
	for i, u := range states {
		rows[i] = store.UserState{
			UserID:   u.UserID,
			Tokens:   u.State.Tokens,
			LastUsed: u.State.LastUsed.UnixNano(),
		}
	}
	return a.s.SaveBatch(ctx, rows)
}

func (a sqliteStateStore) Load(ctx context.Context, userID string) (State, bool, error) {
	ss, found, err := a.s.Load(ctx, userID)
	if err != nil || !found {
		return State{}, found, err
	}
	return State{Tokens: ss.Tokens, LastUsed: time.Unix(0, ss.LastUsed)}, true, nil
}

func (a sqliteStateStore) Close() error { return a.s.Close() }
