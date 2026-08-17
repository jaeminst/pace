package sqlite

import (
	"context"
	"testing"
	"time"
)

// TestReadsDoNotBlockOnAnOpenWrite is the point of holding two handles, checked
// without timing anything.
//
// The writer pool is capped at one connection, so an open write transaction
// occupies it entirely. If reads shared that pool they would have nowhere to go
// and would block until the transaction finished — which is exactly what
// happened when a single pool served both. With a separate reader and WAL, the
// read proceeds against the last committed snapshot.
func walStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestReadsDoNotBlockOnAnOpenWrite(t *testing.T) {
	s := walStore(t)
	ctx := context.Background()

	if err := s.Save(ctx, "alice", 2.5, 100); err != nil {
		t.Fatal(err)
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO user_state (user_id, tokens, last_used) VALUES ('bob', 1, 1)`); err != nil {
		t.Fatal(err)
	}

	// The writer's only connection is now held by the transaction above.
	done := make(chan error, 1)
	go func() {
		_, _, err := s.Load(ctx, "alice")
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Load during an open write transaction = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Load blocked on the writer's connection")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// TestReaderSeesCommittedWrites guards the other half: snapshot isolation must
// not mean stale reads after a commit.
func TestReaderSeesCommittedWrites(t *testing.T) {
	s := walStore(t)
	ctx := context.Background()

	for i, tokens := range []float64{1, 2, 3} {
		if err := s.Save(ctx, "alice", tokens, int64(i)); err != nil {
			t.Fatal(err)
		}
		st, found, err := s.Load(ctx, "alice")
		if err != nil {
			t.Fatal(err)
		}
		if !found || st.Tokens != tokens {
			t.Fatalf("after writing %v the reader saw (%+v, %v)", tokens, st, found)
		}
	}
}

func TestJournalModeIsWAL(t *testing.T) {
	s := walStore(t)
	var mode string
	if err := s.wdb.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Errorf("writer journal_mode = %q, want %q", mode, "wal")
	}
	if err := s.rdb.QueryRow(`PRAGMA journal_mode`).Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Errorf("reader journal_mode = %q, want %q", mode, "wal")
	}
}
