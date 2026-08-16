// Package store persists per-user rate-limit state to a SQLite database.
package store

import (
	"database/sql"
	"errors"
	"fmt"

	_ "modernc.org/sqlite" // register "sqlite" driver
)

// SavedState holds the persisted snapshot of a single user's bucket.
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
			user_id   TEXT    PRIMARY KEY,
			tokens    REAL    NOT NULL,
			last_used INTEGER NOT NULL
		)
	`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	return &Store{db: db}, nil
}

// Save persists the current token count and lastUsed timestamp for a user.
// It is called on GC eviction and when the Limiter closes.
func (s *Store) Save(userID string, tokens float64, lastUsed int64) error {
	_, err := s.db.Exec(`
		INSERT OR REPLACE INTO user_state (user_id, tokens, last_used)
		VALUES (?, ?, ?)
	`, userID, tokens, lastUsed)
	return err
}

// Load returns the saved state for a user.
// Returns (zero, false, nil) when the user has no saved state.
func (s *Store) Load(userID string) (SavedState, bool, error) {
	row := s.db.QueryRow(`
		SELECT tokens, last_used FROM user_state WHERE user_id = ?
	`, userID)
	var ss SavedState
	if err := row.Scan(&ss.Tokens, &ss.LastUsed); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return SavedState{}, false, nil
		}
		return SavedState{}, false, err
	}
	return ss, true, nil
}

// Close closes the underlying database connection.
func (s *Store) Close() error {
	return s.db.Close()
}
