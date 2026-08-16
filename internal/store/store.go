// Package store persists per-user rate-limit state to a SQLite database.
package store

import (
	"context"
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

// UserState is one user's persisted bucket state as a batch element.
type UserState struct {
	UserID   string
	Tokens   float64
	LastUsed int64
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
func (s *Store) Save(ctx context.Context, userID string, tokens float64, lastUsed int64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT OR REPLACE INTO user_state (user_id, tokens, last_used)
		VALUES (?, ?, ?)
	`, userID, tokens, lastUsed)
	return err
}

// SaveBatch persists many users in one transaction. The GC sweep evicts users
// in bulk, and a transaction per user turns that into one fsync each — the
// difference between milliseconds and seconds for a few thousand users.
func (s *Store) SaveBatch(ctx context.Context, states []UserState) error {
	if len(states) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after a successful Commit is a no-op
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO user_state (user_id, tokens, last_used)
		VALUES (?, ?, ?)
		ON CONFLICT(user_id) DO UPDATE SET
			tokens = excluded.tokens,
			last_used = excluded.last_used
	`)
	if err != nil {
		return err
	}
	defer stmt.Close() //nolint:errcheck // a prepared-statement close cannot report anything the exec did not
	for i := range states {
		if _, err := stmt.ExecContext(ctx, states[i].UserID, states[i].Tokens, states[i].LastUsed); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Load returns the saved state for a user.
// Returns (zero, false, nil) when the user has no saved state.
func (s *Store) Load(ctx context.Context, userID string) (SavedState, bool, error) {
	row := s.db.QueryRowContext(ctx, `
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
