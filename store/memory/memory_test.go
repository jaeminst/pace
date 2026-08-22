package memory_test

import (
	"context"
	"testing"
	"time"

	"github.com/jaeminst/pace/store"
	"github.com/jaeminst/pace/store/memory"
	"github.com/jaeminst/pace/store/storetest"
)

// The reference implementation has to pass the executable contract, or one of
// the two is wrong and there is nothing to say which.
func TestMemoryStoreSatisfiesTheContract(t *testing.T) {
	storetest.Suite(t, func(*testing.T) store.Store { return memory.New() })
}

// Len is the one thing here that is not part of the contract, so the suite does
// not reach it.
func TestLenCountsSavedUsers(t *testing.T) {
	s := memory.New()
	if got := s.Len(); got != 0 {
		t.Fatalf("Len() = %d on a fresh store, want 0", got)
	}

	ctx := context.Background()
	at := time.Unix(1700000000, 0)
	for _, id := range []string{"alice", "bob", "alice"} {
		if err := s.Save(ctx, id, store.State{Tokens: 1, LastUsed: at}); err != nil {
			t.Fatal(err)
		}
	}
	if got := s.Len(); got != 2 {
		t.Errorf("Len() = %d after saving two distinct users twice over, want 2", got)
	}
}
