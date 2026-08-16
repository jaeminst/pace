package store

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
	jobs, err := s.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Pending returned %d jobs, want 1", len(jobs))
	}
	if got := jobs[0].Headers.Get("X-Custom"); got != "v1" {
		t.Errorf("migrated header X-Custom = %q, want %q", got, "v1")
	}

	// The cached result survives too.
	res, ok, err := s.Get(ctx, "job-0")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("cached result for job-0 was lost in the migration")
	}
	if got := res.Headers.Get("Content-Type"); got != "application/json" {
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

	if err := s.Enqueue(ctx, Job{
		ID: "job-multi", UserID: "alice", Method: "GET", Path: "/", Headers: h,
	}, 1); err != nil {
		t.Fatal(err)
	}

	jobs, err := s.Pending(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Pending returned %d jobs, want 1", len(jobs))
	}
	got := jobs[0].Headers["Accept"]
	if len(got) != 2 || got[0] != "application/json" || got[1] != "text/plain" {
		t.Errorf("Accept round-tripped as %q, want both values", got)
	}
	if v := jobs[0].Headers.Get("X-Single"); v != "one" {
		t.Errorf("X-Single = %q, want %q", v, "one")
	}
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
	if err := s.Enqueue(ctx, Job{ID: "j", UserID: "u", Method: "GET", Path: "/", Headers: h}, 1); err != nil {
		t.Fatal(err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
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
	if err := s.db.QueryRowContext(ctx, `SELECT headers FROM pending_jobs WHERE id = 'j'`).Scan(&raw); err != nil {
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
	if err := s.Enqueue(ctx, Job{ID: "ok", UserID: "u", Method: "GET", Path: "/", Headers: http.Header{}}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, `UPDATE pending_jobs SET headers = 'garbage' WHERE id = 'ok'`); err != nil {
		t.Fatal(err)
	}

	tx, err := s.db.BeginTx(ctx, nil)
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
