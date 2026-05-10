package store

import (
	"database/sql"
	"encoding/json"
	"net/http"
	"time"
)

// PendingJob is a durable HTTP request that has been persisted but not yet executed.
type PendingJob struct {
	ID       string
	UserID   string
	Endpoint string
	Method   string
	Path     string
	Headers  map[string]string
	Body     []byte
}

// JobResult is the persisted outcome of a completed job.
type JobResult struct {
	StatusCode int
	Status     string
	Headers    http.Header
	Body       []byte
}

// InitQueueSchema creates the pending_jobs and job_results tables if they do not exist.
func (s *Store) InitQueueSchema() error {
	if _, err := s.db.Exec(`
		CREATE TABLE IF NOT EXISTS pending_jobs (
			id         TEXT    PRIMARY KEY,
			user_id    TEXT    NOT NULL,
			endpoint   TEXT    NOT NULL,
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

// EnqueueJob persists job as pending. Uses INSERT OR IGNORE so duplicate IDs
// are silently skipped (idempotent).
func (s *Store) EnqueueJob(job PendingJob) error {
	h, err := json.Marshal(job.Headers)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
		INSERT OR IGNORE INTO pending_jobs (id, user_id, endpoint, method, path, headers, body, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.UserID, job.Endpoint, job.Method, job.Path, string(h), job.Body, time.Now().UnixNano())
	return err
}

// CompleteJob atomically moves a job from pending to completed.
func (s *Store) CompleteJob(id string, result JobResult) error {
	h, err := json.Marshal(result.Headers)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck
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

// LoadResult returns the cached result for a completed job.
// Returns (nil, false, nil) when no result exists yet.
func (s *Store) LoadResult(id string) (*JobResult, bool, error) {
	row := s.db.QueryRow(`
		SELECT status_code, status, headers, body FROM job_results WHERE id = ?
	`, id)
	var r JobResult
	var headersJSON string
	if err := row.Scan(&r.StatusCode, &r.Status, &headersJSON, &r.Body); err != nil {
		if err == sql.ErrNoRows {
			return nil, false, nil
		}
		return nil, false, err
	}
	if err := json.Unmarshal([]byte(headersJSON), &r.Headers); err != nil {
		return nil, false, err
	}
	return &r, true, nil
}

// PendingJobs returns all jobs that have not yet completed, oldest first.
func (s *Store) PendingJobs() ([]PendingJob, error) {
	rows, err := s.db.Query(`
		SELECT id, user_id, endpoint, method, path, headers, body
		FROM pending_jobs
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck
	var jobs []PendingJob
	for rows.Next() {
		var j PendingJob
		var headersJSON string
		if err := rows.Scan(&j.ID, &j.UserID, &j.Endpoint, &j.Method, &j.Path, &headersJSON, &j.Body); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(headersJSON), &j.Headers); err != nil {
			return nil, err
		}
		jobs = append(jobs, j)
	}
	return jobs, rows.Err()
}
