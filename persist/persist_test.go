package persist

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

func TestNewPanicsOnConfigItCannotUse(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{"no timeout", Config{Store: &fake{}, Logger: discard()}, "persist: Timeout must be positive when Store is set"},
		{"negative timeout", Config{Store: &fake{}, Timeout: -time.Second, Logger: discard()}, "persist: Timeout must be positive when Store is set"},
		{"no logger", Config{Store: &fake{}, Timeout: time.Second}, "persist: Logger is required when Store is set"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			defer func() {
				got, ok := recover().(string)
				if !ok || got != tt.want {
					t.Errorf("panic = %v, want %q", got, tt.want)
				}
			}()
			New(tt.cfg)
			t.Error("New did not panic")
		})
	}
}

// A nil Store is the default configuration, not a mistake, so it must not
// panic even though Timeout and Logger are then zero.
func TestNewAcceptsNoStore(t *testing.T) {
	t.Parallel()
	if a := New(Config{}); a.Persists() {
		t.Error("Persists() = true with no store")
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
			a := New(Config{Store: tt.store, Shadowed: tt.shadowed, Timeout: time.Second, Logger: discard()})
			if got := a.Persists(); got != tt.want {
				t.Errorf("Persists() = %v, want %v", got, tt.want)
			}
		})
	}
}

// A store that cannot be read must not fail the request: a fresh bucket is the
// safe fallback, so the error is logged and reported as "nothing saved".
func TestLoadTreatsAnErrorAsNoSavedState(t *testing.T) {
	t.Parallel()
	a := New(Config{Store: &fake{err: errors.New("boom"), found: true}, Timeout: time.Second, Logger: discard()})
	snap, found := a.Load(context.Background(), "alice")
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
	a := New(Config{Store: f, Timeout: time.Second, Logger: discard()})
	snap, found := a.Load(context.Background(), "alice")
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
	a := New(Config{Store: &fake{err: sentinel}, Timeout: time.Second, Logger: discard()})
	err := a.Save(context.Background(), registry.Snapshot{UserID: "alice"})
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
		cfg   Config
		snaps []registry.Snapshot
	}{
		{"no store", Config{}, snaps},
		{"shadowed", Config{Store: &fake{}, Shadowed: true, Timeout: time.Second, Logger: discard()}, snaps},
		{"no snapshots", Config{Store: &fake{}, Timeout: time.Second, Logger: discard()}, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			New(tt.cfg).Flush(tt.snaps)
			if f, ok := tt.cfg.Store.(*fake); ok && len(f.saved) != 0 {
				t.Errorf("wrote %v, want nothing", f.saved)
			}
		})
	}
}

// A store that cannot batch still gets every user, one call at a time.
func TestFlushFallsBackToOneCallPerUser(t *testing.T) {
	t.Parallel()
	f := &fake{}
	New(Config{Store: f, Timeout: time.Second, Logger: discard()}).Flush([]registry.Snapshot{
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
	New(Config{Store: b, Timeout: time.Second, Logger: discard()}).Flush(snaps)

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
		New(Config{Store: s, Timeout: time.Second, Logger: discard()}).
			Flush([]registry.Snapshot{{UserID: "alice"}})
	}
}
