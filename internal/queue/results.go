package queue

import (
	"context"
	"errors"
	"time"

	"github.com/jaeminst/pace/internal/store"
)

const (
	// completeAttempts is how many times recording a result is retried. The
	// response is already in hand, so a transient write failure is worth a few
	// retries rather than an immediate loss.
	completeAttempts = 3
	// completeRetryDelay is the first backoff between those attempts; it
	// doubles each time.
	completeRetryDelay = 10 * time.Millisecond
)

// Complete records a job's result, retrying briefly. The response is already in
// hand at this point, so giving up immediately would throw away work that
// cannot be redone without asking the server again.
//
// It takes store.Result rather than the owner's response type on purpose: that
// type's fields are unexported, so reading one here would mean exporting them —
// and store.Result is already the vocabulary both packages share.
func (q *Queue) Complete(ctx context.Context, id string, result store.Result) error {
	var err error
	for attempt := range completeAttempts {
		if attempt > 0 {
			timer := time.NewTimer(completeRetryDelay << (attempt - 1))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return errors.Join(err, ctx.Err())
			}
		}
		if err = q.cfg.Store.Complete(ctx, id, result, q.cfg.Now().UnixNano()); err == nil {
			return nil
		}
	}
	return err
}

// resultPurgeChunk bounds one DELETE so a large purge cannot hold the SQLite
// writer for the whole operation.
const resultPurgeChunk = 1000

// PurgeResults drops cached durable results past their TTL.
//
// It is driven by the owner's existing GC tick rather than a goroutine of its
// own: the idle-user sweep already runs on that schedule, and both are
// background housekeeping against the same store.
func (q *Queue) PurgeResults() {
	if q.cfg.ResultTTL < 0 {
		return
	}
	cutoff := q.cfg.Now().Add(-q.cfg.ResultTTL).UnixNano()

	ctx, cancel := context.WithTimeout(q.ctx, q.cfg.StoreTimeout)
	defer cancel()

	n, err := q.cfg.Store.PurgeResults(ctx, cutoff, resultPurgeChunk)
	if err != nil {
		if q.ctx.Err() == nil {
			q.cfg.Logger.Warn("pace: durable: purge results", "err", err)
		}
		return
	}
	if n > 0 {
		q.cfg.Logger.Debug("pace: durable: purged cached results", "count", n)
	}
}
