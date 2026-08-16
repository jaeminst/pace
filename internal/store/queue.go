package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
)

// JobState is where a durable job sits in the queue's state machine.
type JobState string

const (
	// StateQueued means the job is persisted and nobody is sending it.
	StateQueued JobState = "queued"
	// StateSending means a worker committed its intent to send before
	// dispatching. A job found in this state after a crash is ambiguous: the
	// server may or may not have seen it.
	StateSending JobState = "sending"
)

// Job is a durable HTTP request that has been persisted but not yet completed.
type Job struct {
	ID       string
	UserID   string
	Method   string
	Path     string
	Headers  http.Header
	Body     []byte
	State    JobState
	Attempts int
	// Reason is set only for jobs read back from the dead-letter table.
	Reason string
}

// Result is the persisted outcome of a completed job.
type Result struct {
	StatusCode int
	Status     string
	Headers    http.Header
	Body       []byte
}

// Enqueue persists job as pending, and is idempotent: an ID that is already
// pending, or that has already completed, is left alone.
//
// OR IGNORE covers the first case, since id is the primary key. The second
// needs the NOT EXISTS, because Complete deletes the pending row — so once a
// job finishes there is nothing left for OR IGNORE to conflict with, and a
// plain insert would resurrect a completed job as a fresh queued one. Two
// workers racing for the same job hit exactly that: the loser reads the result
// cache just before the winner writes it, finds nothing, and would otherwise
// re-enqueue and send a second copy of a request that has already been
// delivered.
//
// now is supplied by the caller rather than read here, so that every timestamp
// in the database comes from one clock — the injected one — and tests can drive
// expiry without waiting for it.
func (s *Store) Enqueue(ctx context.Context, job Job, now int64) error {
	h, err := json.Marshal(job.Headers)
	if err != nil {
		return err
	}
	_, err = s.wdb.ExecContext(ctx, `
		INSERT OR IGNORE INTO pending_jobs (id, user_id, method, path, headers, body, created_at)
		SELECT ?, ?, ?, ?, ?, ?, ?
		 WHERE NOT EXISTS (SELECT 1 FROM job_results WHERE id = ?)
	`, job.ID, job.UserID, job.Method, job.Path, string(h), job.Body, now, job.ID)
	return err
}

// Complete atomically moves a job from pending to completed. now is supplied by
// the caller; see Enqueue.
func (s *Store) Complete(ctx context.Context, id string, result Result, now int64) error {
	h, err := json.Marshal(result.Headers)
	if err != nil {
		return err
	}
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after a successful Commit is a no-op
	if _, err := tx.ExecContext(ctx, `
		INSERT OR REPLACE INTO job_results (id, status_code, status, headers, body, completed_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, id, result.StatusCode, result.Status, string(h), result.Body, now); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_jobs WHERE id = ?`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// Claim marks a job as being sent by owner, before the request is dispatched.
// It reports whether the claim succeeded; false means another worker — possibly
// in another process — already owns it, or the job is not yet due.
//
// The whole state transition is one conditional UPDATE, so two workers racing
// for the same job cannot both win. INSERT OR IGNORE deduplicates the row, not
// the send; this is what deduplicates the send.
//
// Committing state='sending' before dispatch is what makes a crash detectable.
// A job found in that state afterwards is one whose outcome is unknown, which
// is a fact worth recording rather than papering over.
func (s *Store) Claim(ctx context.Context, id, owner string, now, leaseUntil int64) (bool, error) {
	res, err := s.wdb.ExecContext(ctx, `
		UPDATE pending_jobs
		   SET state = 'sending',
		       attempts = attempts + 1,
		       owner = ?,
		       lease_until = ?,
		       updated_at = ?
		 WHERE id = ?
		   AND next_attempt_at <= ?
		   AND (state = 'queued' OR (state = 'sending' AND lease_until < ?))
	`, owner, leaseUntil, now, id, now, now)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Release returns a claimed job to the queue, to be retried no earlier than
// nextAttemptAt. Use it only when the request is known not to have been
// delivered; a job whose outcome is unknown must stay in StateSending.
//
// It reports whether the release happened. False means the caller no longer
// owned the job — its lease expired and another worker reclaimed it. Releasing
// anyway would hand a job that is being sent right now back to the queue, and
// the next worker to claim it would send a second copy. That is the failure
// Claim's conditional UPDATE exists to prevent, so Release is conditional on
// the same ownership.
func (s *Store) Release(ctx context.Context, id, owner string, now, nextAttemptAt int64, lastErr string) (bool, error) {
	res, err := s.wdb.ExecContext(ctx, `
		UPDATE pending_jobs
		   SET state = 'queued',
		       owner = '',
		       lease_until = 0,
		       next_attempt_at = ?,
		       last_error = ?,
		       updated_at = ?
		 WHERE id = ?
		   AND owner = ?
		   AND state = 'sending'
	`, nextAttemptAt, lastErr, now, id, owner)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return n == 1, nil
}

// Kill moves a job out of the queue and into dead_jobs, atomically. A dead job
// is never retried; it is kept so an operator can see what was abandoned and
// why.
func (s *Store) Kill(ctx context.Context, id, reason string, now int64) (Job, bool, error) {
	tx, err := s.wdb.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback() //nolint:errcheck // rollback after a successful Commit is a no-op

	row := tx.QueryRowContext(ctx, `
		SELECT id, user_id, method, path, headers, body, state, attempts
		FROM pending_jobs WHERE id = ?
	`, id)
	job, err := scanJob(row.Scan)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Job{}, false, nil
		}
		return Job{}, false, err
	}

	headers, err := json.Marshal(job.Headers)
	if err != nil {
		return Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT OR REPLACE INTO dead_jobs (id, user_id, method, path, headers, body, attempts, reason, died_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.UserID, job.Method, job.Path, string(headers), job.Body, job.Attempts, reason, now); err != nil {
		return Job{}, false, err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM pending_jobs WHERE id = ?`, id); err != nil {
		return Job{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, false, err
	}
	return job, true, nil
}

// Dead returns abandoned jobs, most recent first.
func (s *Store) Dead(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT id, user_id, method, path, headers, body, 'dead', attempts, reason
		FROM dead_jobs
		ORDER BY died_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // the deferred close cannot report anything rows.Err() has not already surfaced
	var jobs []Job
	for rows.Next() {
		var reason string
		job, err := scanJob(rows.Scan, &reason)
		if err != nil {
			return nil, err
		}
		job.Reason = reason
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// scanJob decodes one job row from either a Row or Rows scanner. extra receives
// any trailing columns the caller selected beyond the common set.
func scanJob(scan func(dest ...any) error, extra ...any) (Job, error) {
	var j Job
	var headersJSON string
	var state string
	dest := append([]any{&j.ID, &j.UserID, &j.Method, &j.Path, &headersJSON, &j.Body, &state, &j.Attempts}, extra...)
	if err := scan(dest...); err != nil {
		return Job{}, err
	}
	j.State = JobState(state)
	if err := json.Unmarshal([]byte(headersJSON), &j.Headers); err != nil {
		return Job{}, err
	}
	return j, nil
}

// Get returns the cached result for a completed job.
// Returns (nil, false, nil) when no result exists yet.
func (s *Store) Get(ctx context.Context, id string) (*Result, bool, error) {
	row := s.rdb.QueryRowContext(ctx, `
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

// Pending returns all jobs that have not yet completed, oldest first, each
// carrying the state it was left in.
func (s *Store) Pending(ctx context.Context) ([]Job, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT id, user_id, method, path, headers, body, state, attempts
		FROM pending_jobs
		ORDER BY created_at ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // the deferred close cannot report anything rows.Err() has not already surfaced
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows.Scan)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// Due returns jobs that are eligible to run now: queued and past their next
// attempt time, or claimed by a worker whose lease has expired.
func (s *Store) Due(ctx context.Context, now int64, limit int) ([]Job, error) {
	rows, err := s.rdb.QueryContext(ctx, `
		SELECT id, user_id, method, path, headers, body, state, attempts
		FROM pending_jobs
		WHERE next_attempt_at <= ?
		  AND (state = 'queued' OR (state = 'sending' AND lease_until < ?))
		ORDER BY next_attempt_at ASC
		LIMIT ?
	`, now, now, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck // the deferred close cannot report anything rows.Err() has not already surfaced
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows.Scan)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// ClaimN is Claim, additionally reporting the attempt number the claim
// produced. Workers need it to compute backoff and to know when a job has
// exhausted its allowance.
func (s *Store) ClaimN(ctx context.Context, id, owner string, now, leaseUntil int64) (bool, int, error) {
	ok, err := s.Claim(ctx, id, owner, now, leaseUntil)
	if err != nil || !ok {
		return false, 0, err
	}
	var attempts int
	if err := s.wdb.QueryRowContext(ctx,
		`SELECT attempts FROM pending_jobs WHERE id = ?`, id).Scan(&attempts); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return true, 0, nil // completed underneath us; harmless
		}
		return false, 0, err
	}
	return true, attempts, nil
}

// PurgeResults deletes cached results older than cutoff, in bounded chunks, and
// reports how many rows went.
//
// The chunking is the point: a first run against a table that has been growing
// for months would otherwise hold the single writer for as long as the delete
// takes, stalling every other queue operation behind it.
func (s *Store) PurgeResults(ctx context.Context, cutoff int64, chunk int) (int64, error) {
	var total int64
	for {
		res, err := s.wdb.ExecContext(ctx, `
			DELETE FROM job_results
			WHERE id IN (SELECT id FROM job_results WHERE completed_at < ? LIMIT ?)
		`, cutoff, chunk)
		if err != nil {
			return total, err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return total, err
		}
		total += n
		if n < int64(chunk) {
			return total, nil
		}
		// Stop between chunks if the caller gave up. Releasing the writer is
		// the chunking's job — each DELETE is its own transaction, so other
		// queue operations interleave — and this only decides whether to start
		// another one.
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}
	}
}
