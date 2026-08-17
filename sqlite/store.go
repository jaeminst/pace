// Package sqlite is the database behind limiter.Config.DBPath: the file, its
// connections, its schema, and per-user token state.
//
// One file holds two things — user_state, and the durable queue's tables — so
// this package owns one migration chain over both. It does not own the queue's
// SQL: Enqueue, Claim, Kill and the rest are in
// github.com/jaeminst/pace/runner, next to the poller that calls them, because
// what those statements guarantee is queue behaviour rather than storage. What
// stays here is what the queue borrows to run them: [Store.Exec], [Store.Query],
// [Store.QueryRow] and [Store.Tx], which route to the right pool.
//
// That leaves a coupling the two packages keep by hand. A column added here for
// a query there — pending_jobs.lease_until, or the (died_at, id) index Dead
// orders by — has no compiler to check it.
//
// It is also not the same thing as github.com/jaeminst/pace/store, which is the
// persistence *contract* a caller implements; this is one implementation of it.
//
// It is public because it is worth reading, not because a caller is expected to
// open one: the Limiter opens and closes the handle, and a second writer on the
// same file is not something the design accounts for. The methods here are
// covered by the compatibility promise; the schema they read is not, and
// migrates forward on open.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"runtime"

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
//
// It holds two handles to the same file. SQLite allows one writer, so wdb is
// capped at a single connection; rdb exists because capping the *whole* pool at
// one connection also puts every read behind whatever write is committing, and
// reads are on the request path — a user lookup should not queue behind the GC
// sweep. In WAL mode readers see a consistent snapshot without blocking on the
// writer, which is what makes the split worth having.
type Store struct {
	wdb *sql.DB
	rdb *sql.DB
}

// OpenStore opens (or creates) the SQLite database at path and migrates it to
// the current schema.
//
// Both the user-state table and the durable-queue tables are created here. The
// queue tables cost two empty tables for callers who never enable the queue,
// which is cheaper than a second schema path that only some databases have
// taken.
func OpenStore(path string) (*Store, error) {
	wdb, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		return nil, err
	}
	// SQLite is single-writer; one connection avoids SQLITE_BUSY between our
	// own writes. busy_timeout in the DSN covers a second process on the file.
	wdb.SetMaxOpenConns(1)
	wdb.SetMaxIdleConns(1)
	wdb.SetConnMaxLifetime(0)

	s := &Store{wdb: wdb}
	// Migrations run on the writer, and must complete before any reader opens:
	// a read-only connection cannot create the database.
	if err := s.migrate(context.Background()); err != nil {
		_ = wdb.Close()
		return nil, err
	}

	rdb, err := sql.Open("sqlite", dsn(path))
	if err != nil {
		_ = wdb.Close()
		return nil, err
	}
	rdb.SetMaxOpenConns(max(2, min(4, runtime.NumCPU())))
	s.rdb = rdb
	return s, nil
}

// Exec runs a statement on the writer.
//
// It exists so that the durable queue's SQL can live with the queue rather than
// here, without handing out the *sql.DB. SQLite takes one writer and this pool
// is capped at one connection to match; routing every write through here is
// what keeps that true no matter who is calling.
func (s *Store) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.wdb.ExecContext(ctx, query, args...)
}

// Query runs a read on the reader pool, which in WAL mode proceeds against a
// consistent snapshot without waiting for the writer.
func (s *Store) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.rdb.QueryContext(ctx, query, args...)
}

// QueryRow is Query for a statement that returns at most one row.
func (s *Store) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.rdb.QueryRowContext(ctx, query, args...)
}

// Tx runs fn inside a write transaction, rolling back if it returns an error.
//
// The queue needs this for Complete, which deletes the pending row and inserts
// the result as one step, and for Kill, which moves a row between tables.
func (s *Store) Tx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after a successful Commit is a no-op
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// dsn builds the connection string.
//
// journal_mode=WAL is what lets readers proceed while the writer commits.
// synchronous=NORMAL is the matching durability choice: with WAL it still
// survives a process crash, losing at most recent commits to a power failure —
// acceptable for token accounting, where the cost is a bucket that refills a
// little early. busy_timeout matters when a second process shares the file, and
// for the WAL checkpointer.
//
// WAL keeps -wal and -shm sidecars next to the database, and is unsafe on a
// network filesystem. Both are documented on Config.DBPath.
func dsn(path string) string {
	return path +
		"?_pragma=journal_mode(WAL)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_pragma=foreign_keys(1)"
}

// Save persists the current token count and lastUsed timestamp for a user.
// It is called on GC eviction and when the Limiter closes.
func (s *Store) Save(ctx context.Context, userID string, tokens float64, lastUsed int64) error {
	_, err := s.wdb.ExecContext(ctx, `
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
	tx, err := s.wdb.BeginTx(ctx, nil)
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
	row := s.rdb.QueryRowContext(ctx, `
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

// Close closes both handles. The reader is closed first so that no read can
// outlive the writer's file lock.
func (s *Store) Close() error {
	var rerr error
	if s.rdb != nil {
		rerr = s.rdb.Close()
	}
	werr := s.wdb.Close()
	return errors.Join(rerr, werr)
}
