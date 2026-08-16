package store

import (
	"context"
	"net/http"
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
func TestReadsDoNotBlockOnAnOpenWrite(t *testing.T) {
	s := newQueueStore(t)
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
	s := newQueueStore(t)
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

// TestQueueReadsSeeCommittedJobs covers the same for the queue tables, which
// are read on the request path by Get and by the poller.
func TestQueueReadsSeeCommittedJobs(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()

	enqueue(t, s, "job-1", http.MethodGet)
	if jobs, err := s.Due(ctx, 1<<62, 10); err != nil || len(jobs) != 1 {
		t.Fatalf("Due after Enqueue = (%d jobs, %v), want 1", len(jobs), err)
	}
	if ok, err := s.Claim(ctx, "job-1", "w", 1, 1<<62); err != nil || !ok {
		t.Fatalf("Claim = (%v, %v)", ok, err)
	}
	// The claim is committed, so the job is no longer due.
	if jobs, err := s.Due(ctx, 1, 10); err != nil || len(jobs) != 0 {
		t.Fatalf("Due after Claim = (%d jobs, %v), want 0", len(jobs), err)
	}
}

func TestJournalModeIsWAL(t *testing.T) {
	s := newQueueStore(t)
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
