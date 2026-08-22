package limiter

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/store"
)

func discard() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// fake records what it was asked to write and can be told to fail.
type fake struct {
	mu     sync.Mutex
	saved  []string
	batch  [][]store.UserState
	loaded store.State
	found  bool
	err    error
}

func (f *fake) Save(_ context.Context, userID string, _ store.State) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.saved = append(f.saved, userID)
	return nil
}

func (f *fake) Load(_ context.Context, _ string) (store.State, bool, error) {
	if f.err != nil {
		return store.State{}, false, f.err
	}
	return f.loaded, f.found, nil
}

// batching is a fake that also implements store.BatchStore.
type batching struct{ fake }

func (b *batching) SaveBatch(_ context.Context, states []store.UserState) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	b.batch = append(b.batch, states)
	return nil
}

// TestNoStoreIsTheDefault: a nil store is the default configuration, not a
// mistake. The zero persistence therefore has to be usable — it is what a
// Limiter with no Config.Store holds — and must answer "no" rather than panic.
func TestNoStoreIsTheDefault(t *testing.T) {
	t.Parallel()
	if a := (&persistence{}); a.persists() {
		t.Error("persists() = true with no store")
	}
}

func TestPersists(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		store    store.Store
		shadowed bool
		want     bool
	}{
		{"store only", &fake{}, false, true},
		{"no store", nil, false, false},
		// A shared quota makes the local bucket a shadow of the fleet's
		// consumption, and persisting a shadow would restore one replica's
		// fraction as if it were the whole.
		{"store but shadowed", &fake{}, true, false},
		{"neither", nil, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			a := &persistence{store: tt.store, shadowed: tt.shadowed, timeout: time.Second, logger: discard()}
			if got := a.persists(); got != tt.want {
				t.Errorf("persists() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A store that cannot be read must not fail the request: a fresh bucket is the
// safe fallback, so the error is logged and reported as "nothing saved".
func TestLoadTreatsAnErrorAsNoSavedState(t *testing.T) {
	t.Parallel()
	a := &persistence{store: &fake{err: errors.New("boom"), found: true}, timeout: time.Second, logger: discard()}
	snap, found := a.load(context.Background(), "alice")
	if found {
		t.Error("found = true after a store error")
	}
	if snap != (registry.Snapshot{}) {
		t.Errorf("snapshot = %+v, want zero", snap)
	}
}

func TestLoadCarriesTheSavedState(t *testing.T) {
	t.Parallel()
	at := time.Unix(1700000000, 0)
	f := &fake{loaded: store.State{Tokens: 3.5, LastUsed: at}, found: true}
	a := &persistence{store: f, timeout: time.Second, logger: discard()}
	snap, found := a.load(context.Background(), "alice")
	if !found {
		t.Fatal("found = false")
	}
	want := registry.Snapshot{UserID: "alice", Tokens: 3.5, LastUsed: at}
	if snap != want {
		t.Errorf("snapshot = %+v, want %+v", snap, want)
	}
}

// Save backs a single eviction, whose contract is that the state is written by
// the time it returns — so unlike Flush it reports the failure.
func TestSaveReportsTheError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	a := &persistence{store: &fake{err: sentinel}, timeout: time.Second, logger: discard()}
	err := a.save(context.Background(), registry.Snapshot{UserID: "alice"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap %v", err, sentinel)
	}
	if got, want := err.Error(), `pace: evict "alice": boom`; got != want {
		t.Errorf("err = %q, want %q", got, want)
	}
}

func TestFlushIsANoOpWhenThereIsNothingToDo(t *testing.T) {
	t.Parallel()
	snaps := []registry.Snapshot{{UserID: "alice"}}
	tests := []struct {
		name  string
		cfg   persistence
		snaps []registry.Snapshot
	}{
		{"no store", persistence{}, snaps},
		{"shadowed", persistence{store: &fake{}, shadowed: true, timeout: time.Second, logger: discard()}, snaps},
		{"no snapshots", persistence{store: &fake{}, timeout: time.Second, logger: discard()}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			(&tt.cfg).flush(tt.snaps)
			if f, ok := tt.cfg.store.(*fake); ok && len(f.saved) != 0 {
				t.Errorf("wrote %v, want nothing", f.saved)
			}
		})
	}
}

// A store that cannot batch still gets every user, one call at a time.
func TestFlushFallsBackToOneCallPerUser(t *testing.T) {
	t.Parallel()
	f := &fake{}
	(&persistence{store: f, timeout: time.Second, logger: discard()}).flush([]registry.Snapshot{
		{UserID: "alice"}, {UserID: "bob"},
	})
	if got, want := len(f.saved), 2; got != want {
		t.Fatalf("saved %d users, want %d", got, want)
	}
}

// One sweep must not become one unbounded round-trip, so a batch store is fed
// in chunks.
func TestFlushChunksABatchStore(t *testing.T) {
	t.Parallel()
	snaps := make([]registry.Snapshot, chunk+1)
	for i := range snaps {
		snaps[i] = registry.Snapshot{UserID: strconv.Itoa(i)}
	}
	b := &batching{}
	(&persistence{store: b, timeout: time.Second, logger: discard()}).flush(snaps)

	if got, want := len(b.batch), 2; got != want {
		t.Fatalf("made %d round-trips, want %d", got, want)
	}
	if got, want := len(b.batch[0]), chunk; got != want {
		t.Errorf("first batch held %d, want %d", got, want)
	}
	if got, want := len(b.batch[1]), 1; got != want {
		t.Errorf("second batch held %d, want %d", got, want)
	}
	if len(b.saved) != 0 {
		t.Errorf("also wrote %d users one at a time", len(b.saved))
	}
}

// Flush swallows what Save reports: it runs during shutdown, where there is
// nobody left to hand the error to.
func TestFlushSwallowsTheError(t *testing.T) {
	t.Parallel()
	for _, s := range []store.Store{&fake{err: errors.New("boom")}, &batching{fake{err: errors.New("boom")}}} {
		(&persistence{store: s, timeout: time.Second, logger: discard()}).flush([]registry.Snapshot{{UserID: "alice"}})
	}
}
