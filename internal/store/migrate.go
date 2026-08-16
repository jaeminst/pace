package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
)

// schemaVersion is the version this binary writes and understands. It must
// equal len(migrations).
const schemaVersion = 2

// migration is one ordered, transactional schema step. Steps are never edited
// once released; a change to the schema is always a new step, so that a
// database written by an older binary reaches the current shape by the same
// path a fresh one does.
type migration struct {
	version int
	apply   func(ctx context.Context, tx *sql.Tx) error
}

var migrations = []migration{
	{version: 1, apply: migrateV1},
	{version: 2, apply: migrateV2},
}

// migrate brings the database up to schemaVersion.
//
// It refuses to open a database stamped newer than this binary understands. A
// rolled-back deploy would otherwise write through columns it does not know
// about, silently corrupting state that the newer binary still expects to read.
func (s *Store) migrate(ctx context.Context) error {
	var current int
	if err := s.wdb.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&current); err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}
	if current > schemaVersion {
		return fmt.Errorf(
			"database schema version %d is newer than this build understands (%d); upgrade pace or use a different DBPath",
			current, schemaVersion,
		)
	}
	for _, m := range migrations {
		if m.version <= current {
			continue
		}
		if err := s.applyMigration(ctx, m); err != nil {
			return fmt.Errorf("migrate to schema version %d: %w", m.version, err)
		}
	}
	return nil
}

func (s *Store) applyMigration(ctx context.Context, m migration) error {
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after a successful Commit is a no-op
	if err := m.apply(ctx, tx); err != nil {
		return err
	}
	// PRAGMA does not accept bind parameters, hence the formatted constant.
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`PRAGMA user_version = %d`, m.version)); err != nil {
		return err
	}
	return tx.Commit()
}

// migrateV1 is the schema as shipped in v0.1.0. Existing databases already have
// these tables and a user_version of 0, so the IF NOT EXISTS clauses let them
// join the same migration path as a fresh database.
func migrateV1(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS user_state (
			user_id   TEXT    PRIMARY KEY,
			tokens    REAL    NOT NULL,
			last_used INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS pending_jobs (
			id         TEXT    PRIMARY KEY,
			user_id    TEXT    NOT NULL,
			method     TEXT    NOT NULL,
			path       TEXT    NOT NULL,
			headers    TEXT    NOT NULL,
			body       BLOB,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS job_results (
			id           TEXT    PRIMARY KEY,
			status_code  INTEGER NOT NULL,
			status       TEXT    NOT NULL,
			headers      TEXT    NOT NULL,
			body         BLOB,
			completed_at INTEGER NOT NULL
		)`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// migrateV2 turns pending_jobs from a replay log into a queue with a state
// machine: a job is queued, then claimed for sending, then either completed or
// dead. It also converts stored headers from map[string]string to the
// http.Header shape, which the old format could not represent.
func migrateV2(ctx context.Context, tx *sql.Tx) error {
	stmts := []string{
		`ALTER TABLE pending_jobs ADD COLUMN state           TEXT    NOT NULL DEFAULT 'queued'`,
		`ALTER TABLE pending_jobs ADD COLUMN attempts        INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE pending_jobs ADD COLUMN next_attempt_at INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE pending_jobs ADD COLUMN lease_until     INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE pending_jobs ADD COLUMN owner           TEXT    NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_jobs ADD COLUMN last_error      TEXT    NOT NULL DEFAULT ''`,
		`ALTER TABLE pending_jobs ADD COLUMN updated_at      INTEGER NOT NULL DEFAULT 0`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_claim   ON pending_jobs(state, next_attempt_at)`,
		`CREATE INDEX IF NOT EXISTS idx_results_time ON job_results(completed_at)`,
		`CREATE TABLE IF NOT EXISTS dead_jobs (
			id       TEXT    PRIMARY KEY,
			user_id  TEXT    NOT NULL,
			method   TEXT    NOT NULL,
			path     TEXT    NOT NULL,
			headers  TEXT    NOT NULL,
			body     BLOB,
			attempts INTEGER NOT NULL,
			reason   TEXT    NOT NULL,
			died_at  INTEGER NOT NULL
		)`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return convertHeadersToCanonical(ctx, tx)
}

// convertHeadersToCanonical rewrites v1 header JSON, which was a
// map[string]string, into the http.Header shape. Leaving the old rows in place
// would make every job carried across the upgrade fail to decode.
func convertHeadersToCanonical(ctx context.Context, tx *sql.Tx) error {
	for _, table := range []string{"pending_jobs", "job_results"} {
		// The read is its own function so that the rows can be closed by defer.
		// Deferring inside the loop would hold both tables' cursors open until
		// the migration finished, and closing by hand on every error path is
		// how a cursor gets leaked by the next edit.
		converted, err := legacyHeaders(ctx, tx, table)
		if err != nil {
			return err
		}
		for id, headers := range converted {
			if _, err := tx.ExecContext(ctx,
				`UPDATE `+table+` SET headers = ? WHERE id = ?`, headers, id); err != nil { //nolint:gosec // table is one of two literals above
				return err
			}
		}
	}
	return nil
}

// legacyHeaders returns the rows of table whose headers are still in the v1
// map[string]string shape, keyed by ID and already re-encoded.
func legacyHeaders(ctx context.Context, tx *sql.Tx, table string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, headers FROM `+table) //nolint:gosec // table is one of two literals in the caller
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // rows.Err below reports anything that matters

	converted := map[string]string{}
	for rows.Next() {
		var id, raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return nil, err
		}
		// Already in the new shape, or empty: leave it alone.
		var canonical http.Header
		if json.Unmarshal([]byte(raw), &canonical) == nil {
			continue
		}
		var legacy map[string]string
		if err := json.Unmarshal([]byte(raw), &legacy); err != nil {
			// Unreadable either way; a job we cannot decode is one we cannot
			// replay, and failing the whole migration over it would strand the
			// database.
			continue
		}
		h := make(http.Header, len(legacy))
		for k, v := range legacy {
			h.Set(k, v)
		}
		encoded, err := json.Marshal(h)
		if err != nil {
			return nil, err
		}
		converted[id] = string(encoded)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return converted, rows.Close()
}
