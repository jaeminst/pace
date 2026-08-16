package pace_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite" // direct DB access for planting queue rows

	"github.com/jaeminst/pace"
)

// durableDo runs a durable request end to end, folding Durable's setup error
// into the result. Durable reports configuration mistakes (no queue, empty ID)
// separately from execution errors; most tests care about one error value, so
// this keeps them reading as a single call. It is safe to use from a goroutine,
// unlike a helper that would call t.Fatal.
func durableDo(ctx context.Context, c *pace.Client, id, method, path string) (*pace.Response, error) {
	req, err := c.Durable(id)
	if err != nil {
		return nil, err
	}
	return req.Do(ctx, method, path)
}

// migrateDB creates the schema at path by opening and closing a Limiter, so
// tests that plant rows directly do not duplicate the schema definition.
func migrateDB(t *testing.T, path string) {
	t.Helper()
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
