package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"
)

// headerJSON reads a row's headers column directly. The migration tests assert
// on what a migration wrote, and the queue's decoder that used to read this for
// them now lives in another package — reading the column is the more direct
// assertion anyway.
func headerJSON(t *testing.T, s *Store, table, id string) http.Header {
	t.Helper()
	var raw string
	if err := s.QueryRow(context.Background(),
		`SELECT headers FROM `+table+` WHERE id = ?`, id).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var h http.Header
	if err := json.Unmarshal([]byte(raw), &h); err != nil {
		t.Fatalf("headers for %s.%s are not http.Header JSON: %v", table, id, err)
	}
	return h
}

// insertJob plants a pending row without going through the queue's Enqueue.
func insertJob(t *testing.T, s *Store, id string, h http.Header) {
	t.Helper()
	raw, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Exec(context.Background(), `
		INSERT INTO pending_jobs (id, user_id, method, path, headers, body, created_at, state, attempts, next_attempt_at)
		VALUES (?, 'u', 'GET', '/', ?, NULL, 1, 'queued', 0, 0)
	`, id, string(raw)); err != nil {
		t.Fatal(err)
	}
}

func tempDB(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "pace.db")
}

func userVersion(t *testing.T, path string) int {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func TestOpenStoreMigratesFreshDatabase(t *testing.T) {
	path := tempDB(t)
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if got := userVersion(t, path); got != schemaVersion {
		t.Errorf("user_version = %d, want %d", got, schemaVersion)
	}
}

func TestMigrationsAreIdempotent(t *testing.T) {
	path := tempDB(t)
	for i := range 3 {
		s, err := OpenStore(path)
		if err != nil {
			t.Fatalf("open #%d: %v", i, err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got := userVersion(t, path); got != schemaVersion {
		t.Errorf("user_version = %d, want %d", got, schemaVersion)
	}
}

func TestMigrateRefusesNewerSchema(t *testing.T) {
	path := tempDB(t)
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a rolled-back deploy: the file was written by a newer binary.
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`PRAGMA user_version = 99`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenStore(path); err == nil {
		t.Fatal("OpenStore accepted a database newer than this build understands")
	}
}

// writeV1Database builds a database exactly as v0.1.0 left it: the original
// tables, user_version still 0, and headers stored as map[string]string.
func writeV1Database(t *testing.T, path string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	stmts := []string{
		`CREATE TABLE user_state (
			user_id   TEXT    PRIMARY KEY,
			tokens    REAL    NOT NULL,
			last_used INTEGER NOT NULL
		)`,
		`CREATE TABLE pending_jobs (
			id         TEXT    PRIMARY KEY,
			user_id    TEXT    NOT NULL,
			method     TEXT    NOT NULL,
			path       TEXT    NOT NULL,
			headers    TEXT    NOT NULL,
			body       BLOB,
			created_at INTEGER NOT NULL
		)`,
		`CREATE TABLE job_results (
			id           TEXT    PRIMARY KEY,
			status_code  INTEGER NOT NULL,
			status       TEXT    NOT NULL,
			headers      TEXT    NOT NULL,
			body         BLOB,
			completed_at INTEGER NOT NULL
		)`,
	}
	for _, q := range stmts {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(
		`INSERT INTO user_state VALUES (?, ?, ?)`, "alice", 2.5, time.Now().UnixNano(),
	); err != nil {
		t.Fatal(err)
	}
	// v1 encoded headers as map[string]string.
	if _, err := db.Exec(
		`INSERT INTO pending_jobs VALUES (?, ?, ?, ?, ?, ?, ?)`,
		"job-1", "alice", "POST", "/things", `{"X-Custom":"v1"}`, []byte("body"), time.Now().UnixNano(),
	); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(
		`INSERT INTO job_results VALUES (?, ?, ?, ?, ?, ?)`,
		"job-0", 200, "200 OK", `{"Content-Type":"application/json"}`, []byte("{}"), time.Now().UnixNano(),
	); err != nil {
		t.Fatal(err)
	}
}

// TestUpgradeFromV1Database is the migration path that runs in production. A
// v0.1.0 file has user_version 0 and tables that already exist, so it must
// reach the current schema by the same steps a fresh database takes.
func TestUpgradeFromV1Database(t *testing.T) {
	path := tempDB(t)
	writeV1Database(t, path)

	s, err := OpenStore(path)
	if err != nil {
		t.Fatalf("upgrading a v0.1.0 database: %v", err)
	}
	defer s.Close()

	ctx := context.Background()

	// Existing user state survives.
	st, found, err := s.Load(ctx, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !found || st.Tokens != 2.5 {
		t.Errorf("Load(alice) = (%+v, %v), want tokens 2.5", st, found)
	}

	// The pending job survives and its headers now decode as http.Header,
	// which the v1 encoding could not represent.
	if got := headerJSON(t, s, "pending_jobs", "job-1").Get("X-Custom"); got != "v1" {
		t.Errorf("migrated header X-Custom = %q, want %q", got, "v1")
	}

	// The cached result survives too.
	if got := headerJSON(t, s, "job_results", "job-0").Get("Content-Type"); got != "application/json" {
		t.Errorf("migrated result header = %q, want application/json", got)
	}

	if got := userVersion(t, path); got != schemaVersion {
		t.Errorf("user_version = %d, want %d", got, schemaVersion)
	}
}

func TestMultiValueHeadersRoundTrip(t *testing.T) {
	// The reason headers moved to http.Header: map[string]string could not
	// express a header that legitimately repeats.
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	h := http.Header{}
	h.Add("Accept", "application/json")
	h.Add("Accept", "text/plain")
	h.Set("X-Single", "one")

	insertJob(t, s, "job-multi", h)

	stored := headerJSON(t, s, "pending_jobs", "job-multi")
	got := stored["Accept"]
	if len(got) != 2 || got[0] != "application/json" || got[1] != "text/plain" {
		t.Errorf("Accept round-tripped as %q, want both values", got)
	}
	if v := stored.Get("X-Single"); v != "one" {
		t.Errorf("X-Single = %q, want %q", v, "one")
	}
	_ = ctx
}

func TestSchemaVersionMatchesMigrationCount(t *testing.T) {
	if len(migrations) != schemaVersion {
		t.Fatalf("schemaVersion = %d but there are %d migrations", schemaVersion, len(migrations))
	}
	for i, m := range migrations {
		if m.version != i+1 {
			t.Errorf("migrations[%d].version = %d, want %d (steps must be ordered and contiguous)", i, m.version, i+1)
		}
	}
}

func TestConvertHeadersLeavesCanonicalRowsAlone(t *testing.T) {
	// Running the header conversion twice must not double-wrap values.
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	h := http.Header{}
	h.Set("X-Custom", "value")
	insertJob(t, s, "j", h)

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := convertHeadersToCanonical(ctx, tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	var raw string
	if err := s.wdb.QueryRowContext(ctx, `SELECT headers FROM pending_jobs WHERE id = 'j'`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var decoded http.Header
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		t.Fatalf("re-running the conversion made the row undecodable: %v", err)
	}
	if got := decoded.Get("X-Custom"); got != "value" {
		t.Errorf("X-Custom = %q, want %q", got, "value")
	}
}

func TestMigrationFailureLeavesVersionUnchanged(t *testing.T) {
	// A migration step that fails must not stamp its version, or the next run
	// would skip it and leave the schema half-applied.
	path := tempDB(t)
	s, err := OpenStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	failing := migration{
		version: schemaVersion + 1,
		apply: func(context.Context, *sql.Tx) error {
			return errFailedMigration
		},
	}
	if err := s.applyMigration(context.Background(), failing); err == nil {
		t.Fatal("applyMigration reported success for a step that failed")
	}
	if got := userVersion(t, path); got != schemaVersion {
		t.Errorf("user_version = %d after a failed migration, want %d", got, schemaVersion)
	}
}

var errFailedMigration = errors.New("migration failed on purpose")

func TestConvertHeadersSkipsUndecodableRows(t *testing.T) {
	// A row nobody can decode is a job nobody can replay. Failing the whole
	// migration over it would strand the database instead.
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx := context.Background()
	insertJob(t, s, "ok", http.Header{})
	if _, err := s.wdb.ExecContext(ctx, `UPDATE pending_jobs SET headers = 'garbage' WHERE id = 'ok'`); err != nil {
		t.Fatal(err)
	}

	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := convertHeadersToCanonical(ctx, tx); err != nil {
		t.Errorf("convertHeadersToCanonical = %v, want nil (undecodable rows are skipped)", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
