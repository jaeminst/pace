package runner

import (
	"context"
	"fmt"

	"github.com/jaeminst/pace/sqlite"
)

// Attempt is the minimum a retry decision needs about a durable job.
type Attempt struct {
	ID       string
	Method   string
	Attempts int
	// Delivered is true when the server answered. A delivered request is not
	// ambiguous: repeating it is a choice, not a gamble.
	Delivered bool
}

// ScheduleRetry decides what happens to a durable job that did not complete.
//
// The decision turns on one question: do we know whether the server saw it? A
// delivered request is unambiguous, so the only limit is the attempt allowance.
// A transport error is ambiguous, and repeating it is safe only under the same
// rules that govern a job found stranded after a crash — an idempotent method,
// or an idempotency key the server can collapse on.
func (q *Queue) ScheduleRetry(a Attempt, cause error) {
	switch {
	case a.Attempts >= q.cfg.MaxAttempts:
		q.kill(sqlite.Job{ID: a.ID, Method: a.Method},
			fmt.Sprintf("gave up after %d attempts: %v", a.Attempts, cause))
		return
	case !a.Delivered && !q.cfg.RepeatSafe(a.Method):
		q.kill(sqlite.Job{ID: a.ID, Method: a.Method},
			fmt.Sprintf("outcome unknown and the request is not safe to repeat: %v", cause))
		return
	}

	delay := q.cfg.Backoff(a.Attempts)
	next := q.cfg.Now().Add(delay).UnixNano()

	// Not q.ctx. See Release for why this one runs on a detached context.
	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.StoreTimeout)
	defer cancel()
	released, err := q.cfg.Store.Release(ctx, a.ID, q.cfg.Owner, q.cfg.Now().UnixNano(), next, cause.Error())
	switch {
	case err != nil:
		q.cfg.Logger.Warn("pace: durable: schedule retry", "job", a.ID, "err", err)
		return
	case !released:
		// The lease expired and another worker reclaimed the job. It owns the
		// retry decision now; scheduling one here would race with the send it
		// is already making.
		q.cfg.Logger.Warn("pace: durable: retry skipped, job no longer owned here",
			"job", a.ID, "err", cause)
		return
	}
	q.cfg.Logger.Debug("pace: durable: retry scheduled",
		"job", a.ID, "attempt", a.Attempts, "in", delay)
	q.cfg.OnRetry(a.ID, a.Method, a.Attempts, delay, cause)
}

// Release returns a durable job to the queue after a failure that provably
// happened before dispatch.
//
// It deliberately does not take the request's context, and must not be changed
// to. The most common reason to be here is that acquiring a token failed
// because that context was cancelled, and reusing it would make this write fail
// too — leaving the job in StateSending, where a restart would classify as
// ambiguous a request we know for certain never left the process. StoreTimeout
// bounds it instead.
func (q *Queue) Release(id string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.StoreTimeout)
	defer cancel()
	now := q.cfg.Now().UnixNano()
	released, err := q.cfg.Store.Release(ctx, id, q.cfg.Owner, now, now, cause.Error())
	switch {
	case err != nil:
		q.cfg.Logger.Warn("pace: durable: release", "job", id, "err", err)
	case !released:
		q.cfg.Logger.Warn("pace: durable: release skipped, job no longer owned here",
			"job", id, "err", cause)
	}
}

// kill moves a job to the dead-letter table and reports it.
//
// Unexported: nothing outside this package abandons a job directly. Both
// callers — the startup recovery and the retry decision — live here.
func (q *Queue) kill(j sqlite.Job, reason string) {
	// Detached for the same reason Release is: this is the record of a job
	// nobody will retry, and losing it to a cancelled context would leave the
	// row stranded in StateSending instead.
	ctx, cancel := context.WithTimeout(context.Background(), q.cfg.StoreTimeout)
	defer cancel()

	killed, ok, err := q.cfg.Store.Kill(ctx, j.ID, reason, q.cfg.Now().UnixNano())
	if err != nil {
		q.cfg.Logger.Error("pace: durable: dead-letter", "job", j.ID, "err", err)
		return
	}
	if !ok {
		return // already gone; another worker completed or killed it
	}
	q.cfg.Logger.Warn("pace: durable: job abandoned",
		"job", killed.ID, "attempts", killed.Attempts, "reason", reason)
	q.cfg.OnDead(killed, reason)
}
