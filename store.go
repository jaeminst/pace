package pace

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

// savedState holds the persisted snapshot of a single user+endpoint bucket.
type savedState struct {
	tokens   float64
	lastUsed int64 // unix nanoseconds
}

// store persists per-user bucket states to a SQLite database so that token
// counts survive process restarts and idle-user GC evictions.
type store struct {
	db *sql.DB
}

func openStore(path string) (*store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS user_state (
			user_id   TEXT    NOT NULL,
			endpoint  TEXT    NOT NULL,
			tokens    REAL    NOT NULL,
			last_used INTEGER NOT NULL,
			PRIMARY KEY (user_id, endpoint)
		)
	`); err != nil {
		db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &store{db: db}, nil
}

// save persists the current token counts and lastUsed timestamp for a user.
// It is called on GC eviction and on [Manager.Close].
func (s *store) save(userID string, buckets map[string]*bucket, lastUsed int64) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // superseded by Commit result
	stmt, err := tx.Prepare(`
		INSERT OR REPLACE INTO user_state (user_id, endpoint, tokens, last_used)
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for name, b := range buckets {
		if _, err := stmt.Exec(userID, name, b.tokens(), lastUsed); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// load returns saved states for all endpoints of a user.
// Returns an empty map (not an error) when the user has no saved state.
func (s *store) load(userID string) (map[string]savedState, error) {
	rows, err := s.db.Query(`
		SELECT endpoint, tokens, last_used
		FROM user_state
		WHERE user_id = ?
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]savedState)
	for rows.Next() {
		var ep string
		var ss savedState
		if err := rows.Scan(&ep, &ss.tokens, &ss.lastUsed); err != nil {
			return nil, err
		}
		result[ep] = ss
	}
	return result, rows.Err()
}

func (s *store) close() error {
	return s.db.Close()
}
