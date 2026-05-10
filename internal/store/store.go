// Package store persists per-user rate-limit state to a SQLite database.
package store

import (
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

// SavedState holds the persisted snapshot of a single user+endpoint bucket.
type SavedState struct {
	Tokens   float64
	LastUsed int64 // unix nanoseconds
}

// Store persists per-user bucket states to a SQLite database so that token
// counts survive process restarts and idle-user GC evictions.
type Store struct {
	db *sql.DB
}

// OpenStore opens (or creates) the SQLite database at path and ensures the
// schema is initialised.
func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// SQLite is single-writer; a single connection avoids SQLITE_BUSY contention.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS user_state (
			user_id   TEXT    NOT NULL,
			endpoint  TEXT    NOT NULL,
			tokens    REAL    NOT NULL,
			last_used INTEGER NOT NULL,
			PRIMARY KEY (user_id, endpoint)
		)
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &Store{db: db}, nil
}

// Save persists the current token counts and lastUsed timestamp for a user.
// It is called on GC eviction and on Manager.Close.
func (s *Store) Save(userID string, tokens map[string]float64, lastUsed int64) error {
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
	defer stmt.Close() //nolint:errcheck
	for name, t := range tokens {
		if _, err := stmt.Exec(userID, name, t, lastUsed); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Load returns saved states for all endpoints of a user.
// Returns an empty map (not an error) when the user has no saved state.
func (s *Store) Load(userID string) (map[string]SavedState, error) {
	rows, err := s.db.Query(`
		SELECT endpoint, tokens, last_used
		FROM user_state
		WHERE user_id = ?
	`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	result := make(map[string]SavedState)
	for rows.Next() {
		var ep string
		var ss SavedState
		if err := rows.Scan(&ep, &ss.Tokens, &ss.LastUsed); err != nil {
			return nil, err
		}
		result[ep] = ss
	}
	return result, rows.Err()
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}
