// store demonstrates implementing store.Store, the persistence contract, and
// what it buys: a user's token count surviving a restart.
//
// pace ships no backend. The one below is a JSON file, written whole on every
// save — the shortest thing that is actually durable, and small enough to read
// in one sitting. A real one is Redis, Postgres, DynamoDB, or whatever already
// holds your state; the interface is the same two methods either way.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/store"
)

// fileStore keeps every user's state in one JSON file.
//
// Two properties are what the contract asks for and both are easy to get wrong:
// a user who was never saved reports found as false and *no error*, and
// LastUsed round-trips exactly — pace restores a bucket from it, so a store
// that truncated the timestamp would hand tokens back at the wrong moment.
type fileStore struct {
	path string
	mu   sync.Mutex
	all  map[string]store.State
}

func openFileStore(path string) (*fileStore, error) {
	s := &fileStore{path: path, all: map[string]store.State{}}
	b, err := os.ReadFile(path) //nolint:gosec // the path is this program's own, not input
	if os.IsNotExist(err) {
		return s, nil // a first run is not a failure
	}
	if err != nil {
		return nil, err
	}
	return s, json.Unmarshal(b, &s.all)
}

func (s *fileStore) Load(_ context.Context, userID string) (store.State, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.all[userID]
	return st, ok, nil
}

func (s *fileStore) Save(_ context.Context, userID string, st store.State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.all[userID] = st
	b, err := json.Marshal(s.all)
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, b, 0o600)
}

// Close is optional: pace discovers it by type assertion. Implement it as a
// no-op if pace should not own the backend's lifetime.
func (s *fileStore) Close() error { return nil }

// main keeps no logic of its own so that run's defers are never skipped by a
// log.Fatal — the mistake gocritic's exitAfterDefer names.
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	path := filepath.Join(os.TempDir(), "pace-demo-state.json")
	defer os.Remove(path) //nolint:errcheck // best-effort cleanup of a demo temp file

	newLimiter := func() (*pace.Limiter, error) {
		st, err := openFileStore(path)
		if err != nil {
			return nil, err
		}
		return pace.New(pace.Config{
			BaseURL: srv.URL,
			Rate:    rate.PerMinute(6), // one token every 10s
			Burst:   1,
			Store:   st,
		})
	}

	ctx := context.Background()

	// --- First Limiter instance ---
	lim1, err := newLimiter()
	if err != nil {
		return err
	}
	if _, err := lim1.Client("alice").Get(ctx, "/"); err != nil {
		return err
	}
	fmt.Println("lim1: alice consumed her token")

	// Close reports whether the final flush reached the store. With persistence
	// configured that error is worth reading: losing it means losing the token
	// accounting this example is about.
	if err := lim1.Close(); err != nil {
		return err
	}
	fmt.Printf("lim1: state saved to %s\n", path)

	// --- Second Limiter instance, simulating a restart ---
	lim2, err := newLimiter()
	if err != nil {
		return err
	}
	defer func() { _ = lim2.Close() }()

	// Alice is still throttled: her token count came back from the file.
	timed, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	if _, err := lim2.Client("alice").Get(timed, "/"); err != nil {
		fmt.Printf("lim2: alice still throttled after restart → %v\n", err)
	} else {
		fmt.Println("lim2: alice was NOT throttled (unexpected)")
	}
	return nil
}
