package pace_test

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	_ "modernc.org/sqlite" // direct DB access for planting queue rows

	"github.com/jaeminst/pace"
)

// durableDo runs a durable request end to end.
//
// It used to exist to fold Durable's second return value into the result.
// Durable is chainable now, so this is a one-liner kept for the call sites that
// read better without the builder spelled out.
func durableDo(ctx context.Context, c *pace.Client, id, method, path string) (*pace.Response, error) {
	return c.Durable(id).Do(ctx, method, path)
}

// migrateDB creates the schema at path by opening and closing a Limiter, so
// tests that plant rows directly do not duplicate the schema definition.
//
// It skips an existing file. That is not just an optimisation: a Limiter with a
// queue configured drains it on startup, so migrating again after seeding would
// try to deliver the very jobs the test is about to set up.
func migrateDB(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err == nil {
		return
	}
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(60),
		DBPath:  path,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(lim)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
}

func openRawDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// seedQueuedJob plants a job that was persisted but never dispatched — the
// unambiguous case, where replaying is simply correct.
func seedQueuedJob(t *testing.T, path, id, userID, method, reqPath string) {
	t.Helper()
	migrateDB(t, path)
	db := openRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO pending_jobs (id, user_id, method, path, headers, body, created_at, state, attempts, next_attempt_at)
		VALUES (?, ?, ?, ?, '{}', NULL, ?, 'queued', 0, 0)
	`, id, userID, method, reqPath, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
}

// strandSendingJob plants a job left mid-flight: its intent to send was
// committed, then the process died before the outcome was recorded. The lease
// is already expired, as it would be after a restart.
func strandSendingJob(t *testing.T, path, id, userID, method, reqPath string) {
	t.Helper()
	migrateDB(t, path)
	db := openRawDB(t, path)
	if _, err := db.Exec(`
		INSERT INTO pending_jobs (id, user_id, method, path, headers, body, created_at, state, attempts, next_attempt_at, lease_until, owner)
		VALUES (?, ?, ?, ?, '{}', NULL, ?, 'sending', 1, 0, 1, 'dead-process')
	`, id, userID, method, reqPath, time.Now().UnixNano()); err != nil {
		t.Fatal(err)
	}
}

// tokensOf reports a user's token count, for assertions that do not care
// whether the user has in-memory state.
func tokensOf(c *pace.Client) float64 {
	n, _ := c.Tokens()
	return n
}

// evict is Evict for tests that only care whether the user was present.
func evict(t *testing.T, c *pace.Client) bool {
	t.Helper()
	present, err := c.Evict(context.Background())
	if err != nil {
		t.Fatalf("Evict: %v", err)
	}
	return present
}
