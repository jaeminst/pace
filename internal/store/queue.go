package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

// Job is a durable HTTP request that has been persisted but not yet executed.
type Job struct {
	ID      string
	UserID  string
	Method  string
	Path    string
	Headers map[string]string
	Body    []byte
}

// Result is the persisted outcome of a completed job.
type Result struct {
	StatusCode int
	Status     string
	Headers    http.Header
	Body       []byte
}

// Setup creates the pending_jobs and job_results tables if they do not exist.
func (s *Store) Setup() error {
	if _, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS pending_jobs (
			id         TEXT    PRIMARY KEY,
			user_id    TEXT    NOT NULL,
			method     TEXT    NOT NULL,
			path       TEXT    NOT NULL,
			headers    TEXT    NOT NULL,
			body       BLOB,
			created_at INTEGER NOT NULL
		)
	`); err != nil {
		return err
	}
	_, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS job_results (
			id           TEXT    PRIMARY KEY,
			status_code  INTEGER NOT NULL,
			status       TEXT    NOT NULL,
			headers      TEXT    NOT NULL,
			body         BLOB,
			completed_at INTEGER NOT NULL
		)
	`)
	return err
}

// Enqueue persists job as pending. Uses INSERT OR IGNORE so duplicate IDs
// are silently skipped (idempotent).
func (s *Store) Enqueue(job Job) error {
	h, err := json.Marshal(job.Headers)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO pending_jobs (id, user_id, method, path, headers, body, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.UserID, job.Method, job.Path, string(h), job.Body, time.Now().UnixNano())
	return err
}

// Complete atomically moves a job from pending to completed.
func (s *Store) Complete(id string, result Result) error {
	h, err := json.Marshal(result.Headers)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after a successful Commit is a no-op
	if _, err := tx.Exec(`
		INSERT OR REPLACE INTO job_results (id, status_code, status, headers, body, completed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, id, result.StatusCode, result.Status, string(h), result.Body, time.Now().UnixNano()); err != nil {
		return err
	}
	if _, err := tx.Exec(`DELETE FROM pending_jobs WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// Get returns the cached result for a completed job.
// Returns (nil, false, nil) when no result exists yet.
func (s *Store) Get(id string) (*Result, bool, error) {
	row := s.db.QueryRow(`
		SELECT status_code, status, headers, body FROM job_results WHERE id = ?
	`, id)
	var r Result
	var headersJSON string
	if err := row.Scan(&r.StatusCode, &r.Status, &headersJSON, &r.Body); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := json.Unmarshal([]byte(headersJSON), &r.Headers); err != nil {
		return nil, false, err
	}
	return &r, true, nil
}

// Pending returns all jobs that have not yet completed, oldest first.
func (s *Store) Pending() ([]Job, error) {
	rows, err := s.db.Query(`
		SELECT id, user_id, method, path, headers, body
		FROM pending_jobs
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // the deferred close cannot report anything rows.Err() has not already surfaced
	var jobs []Job
	for rows.Next() {
		var j Job
		var headersJSON string
		if err := rows.Scan(&j.ID, &j.UserID, &j.Method, &j.Path, &headersJSON, &j.Body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(headersJSON), &j.Headers); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
