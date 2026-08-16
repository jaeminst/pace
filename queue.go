package pace

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jaeminst/pace/internal/queue"
	"github.com/jaeminst/pace/internal/store"
)

// future represents an in-flight Durable execution.
type future struct {
	done chan struct{} // closed when the job finishes
	resp *Response
	err  error
}

func await(ctx context.Context, f *future) (*Response, error) {
	select {
	case <-f.done:
		return f.resp, f.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func toResponse(r *store.Result, clock Clock) *Response {
	return &Response{
		statusCode: r.StatusCode,
		status:     r.Status,
		body:       r.Body,
		header:     r.Headers,
		clock:      clock,
	}
}

// DeadJobs returns durable jobs that were abandoned rather than retried, most
// recent first, up to limit (zero or negative means 100).
//
// Dead jobs are the ones a human has to decide about. Without a way to read
// them back, they would be visible only to a [QueueConfig.OnDeadLetter] callback
// that happened to be registered at the moment they were abandoned.
func (l *Limiter) DeadJobs(ctx context.Context, limit int) ([]DeadJob, error) {
	if l.sqliteStore == nil {
		return nil, ErrNoQueue
	}
	if !l.enter() {
		return nil, ErrClosed
	}
	defer l.leave()

	if limit <= 0 {
		limit = 100
	}
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()

	jobs, err := l.sqliteStore.Dead(ctx, limit)
	if err != nil {
		return nil, fmt.Errorf("pace: read dead jobs: %w", err)
	}
	out := make([]DeadJob, len(jobs))
	for i, j := range jobs {
		out[i] = DeadJob{
			ID:       j.ID,
			UserID:   j.UserID,
			Method:   j.Method,
			Path:     j.Path,
			Headers:  j.Headers,
			Body:     j.Body,
			Attempts: j.Attempts,
			Reason:   j.Reason,
		}
	}
	return out, nil
}

// newQueue builds the background queue and wires it to this Limiter.
//
// Every value is snapshotted here, which is safe only because l.cfg is never
// written after withDefaults. The three funcs are not snapshots: runJob is the
// dispatcher that breaks the import cycle, and fireAfterPoll is passed as a
// method value so a hook installed after the queue started still runs — which
// is how the test suite waits for quiet polls.
func (l *Limiter) newQueue(sqlite *store.Store) *queue.Queue {
	qc := l.cfg.Queue
	return queue.New(l.ctx, queue.Config{
		Store:        sqlite,
		Owner:        l.owner,
		Logger:       l.cfg.Logger,
		Now:          l.cfg.Clock.Now,
		StoreTimeout: l.cfg.StoreTimeout,
		PollInterval: qc.PollInterval,
		Workers:      qc.Workers,
		ResultTTL:    qc.ResultTTL,
		MaxAttempts:  qc.Retry.MaxAttempts,
		Backoff:      qc.Retry.backoff,
		RepeatSafe: func(method string) bool {
			return qc.AmbiguousPolicy.resolve(method, qc.IdempotencyHeader)
		},
		Dispatch:  l.runJob,
		OnDead:    l.onJobDead,
		OnRetry:   l.onJobRetrying,
		AfterPoll: l.fireAfterPoll,
	})
}

// onJobDead turns the queue's report of an abandoned job into the two public
// notifications, in the order they have always fired: the observer first, then
// the caller's dead-letter hook.
func (l *Limiter) onJobDead(j store.Job, reason string) {
	l.observeJob(JobInfo{
		ID: j.ID, UserID: j.UserID, Method: j.Method,
		Phase: JobDead, Attempt: j.Attempts, Reason: reason,
	})
	if l.cfg.Queue.OnDeadLetter != nil {
		l.cfg.Queue.OnDeadLetter(l.ctx, DeadJob{
			ID:       j.ID,
			UserID:   j.UserID,
			Method:   j.Method,
			Path:     j.Path,
			Headers:  j.Headers,
			Body:     j.Body,
			Attempts: j.Attempts,
			Reason:   reason,
		})
	}
}

// onJobRetrying turns the queue's report of a scheduled retry into a JobInfo.
func (l *Limiter) onJobRetrying(id, method string, attempt int, retryIn time.Duration, cause error) {
	l.observeJob(JobInfo{
		ID: id, Method: method, Phase: JobRetrying,
		Attempt: attempt, RetryIn: retryIn, Err: cause,
	})
}

// purgeResults is the GC tick's view of the queue's result purge. The Limiter
// runs a GC tick whether or not a queue exists; the queue does not.
func (l *Limiter) purgeResults() {
	if l.queue == nil {
		return
	}
	l.queue.PurgeResults()
}

// runJob executes one queued job. Failures are recorded by doDurable itself,
// so anything surfacing here is either a lost race for the claim — normal — or
// worth a log line.
func (l *Limiter) runJob(ctx context.Context, j store.Job) {
	req := newRequest(l, j.UserID)
	req.durable, req.durableID = true, j.ID
	req.body = j.Body
	req.headers = j.Headers.Clone()
	if _, err := req.do(ctx, j.Method, j.Path); err != nil &&
		!errors.Is(err, ErrJobClaimed) && l.ctx.Err() == nil {
		l.cfg.Logger.Debug("pace: durable: attempt failed", "job", j.ID, "err", err)
	}
}

// joinOrLead registers the caller as the one that will run job id, or returns
// the execution already under way for it. The second return value is true only
// for the caller that must do the work.
func (l *Limiter) joinOrLead(id string) (*future, bool) {
	l.inflightMu.Lock()
	defer l.inflightMu.Unlock()
	if f, exists := l.inflight[id]; exists {
		return f, false
	}
	f := &future{done: make(chan struct{})}
	l.inflight[id] = f
	return f, true
}

// finishInflight publishes the leader's result to everyone waiting on it.
func (l *Limiter) finishInflight(id string, f *future) {
	l.inflightMu.Lock()
	delete(l.inflight, id)
	l.inflightMu.Unlock()
	close(f.done)
}

// doDurable executes the request against the durable queue.
//
// The order is what gives the queue its properties: record the job, claim it
// exclusively, commit the intent to send, dispatch, then record the outcome.
// Committing before dispatch is what makes a crash detectable — a job found
// mid-flight afterwards is one whose outcome is genuinely unknown, which is a
// fact worth storing rather than a case to guess at.
//
// Concurrent callers in this process share one execution (singleflight); the
// claim is what stops a second process, or a replay goroutine, from sending the
// same request again.
func (r *Request) doDurable(ctx context.Context, method, path string) (*Response, error) {
	l, id := r.lim, r.durableID

	f, leading := l.joinOrLead(id)
	if !leading {
		return await(ctx, f)
	}
	defer l.finishInflight(id, f)

	// A result recorded by an earlier run, possibly in an earlier process,
	// means the request was already delivered.
	if result, ok, err := l.sqliteStore.Get(ctx, id); err != nil {
		f.err = fmt.Errorf("pace: durable: %w", err)
		return nil, f.err
	} else if ok {
		f.resp = toResponse(result, l.cfg.Clock)
		return f.resp, nil
	}

	f.resp, f.err = r.sendDurable(ctx, method, path)
	return f.resp, f.err
}

// sendDurable performs one attempt at a durable job, from recording it to
// recording its outcome.
func (r *Request) sendDurable(ctx context.Context, method, path string) (*Response, error) {
	l, id := r.lim, r.durableID

	l.fireDurableBeforeEnqueue()
	if err := l.sqliteStore.Enqueue(ctx, store.Job{
		ID:      id,
		UserID:  r.userID,
		Method:  method,
		Path:    path,
		Headers: r.headers,
		Body:    r.body,
	}, l.cfg.Clock.Now().UnixNano()); err != nil {
		return nil, fmt.Errorf("pace: durable: enqueue: %w", err)
	}

	// Claim before dispatching. The row was deduplicated by INSERT OR IGNORE,
	// but that deduplicates the *row*, not the *send*: without this, a replay
	// worker and a live caller could both decide they were the leader and put
	// the same request on the wire twice. The claim is one conditional UPDATE,
	// so exactly one of them wins.
	now := l.cfg.Clock.Now()
	claimed, attempt, err := l.sqliteStore.ClaimN(ctx, id, l.owner, now.UnixNano(), now.Add(l.cfg.Queue.JobLease).UnixNano())
	if err != nil {
		return nil, fmt.Errorf("pace: durable: claim: %w", err)
	}
	if !claimed {
		// Losing the claim has two causes and they need different answers.
		// Another worker may still be sending, in which case there is nothing
		// to report but the contention — or it may have already finished, in
		// which case the result is now in the cache and this caller should get
		// the response rather than an error. The first read of the cache
		// happened before the claim; this one happens after, which is what
		// makes the difference visible.
		if result, ok, gerr := l.sqliteStore.Get(ctx, id); gerr == nil && ok {
			return toResponse(result, l.cfg.Clock), nil
		}
		return nil, fmt.Errorf("pace: durable %q: %w", id, ErrJobClaimed)
	}
	l.observeJob(JobInfo{ID: id, UserID: r.userID, Method: method, Phase: JobClaimed, Attempt: attempt})

	httpReq, err := r.build(ctx, method, path)
	if err != nil {
		l.queue.Release(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see queue.Release
		return nil, err
	}
	if l.cfg.Queue.IdempotencyHeader != "" {
		httpReq.Header.Set(l.cfg.Queue.IdempotencyHeader, id)
	}
	if err := l.acquire(ctx, r.userID); err != nil {
		// Nothing was dispatched, so the job is unambiguously still pending.
		l.queue.Release(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see queue.Release
		return nil, err
	}

	timed, cancel := l.withRequestTimeout(ctx)
	defer cancel()
	var started time.Time
	if l.observesRequests() {
		started = l.cfg.Clock.Now()
	}
	resp, err := r.roundTrip(r.lim.timed(timed, httpReq))
	l.countRequest(err)
	if l.observesRequests() {
		l.cfg.Observer.RequestFinished(ctx, RequestInfo{
			UserID:  r.userID,
			Method:  method,
			Path:    path,
			Status:  statusOf(resp),
			Latency: l.cfg.Clock.Now().Sub(started),
			Durable: true,
			Err:     err,
		})
	}
	if err != nil {
		// No response means no way to know whether bytes reached the server.
		// scheduleRetry applies the same ambiguity rules the startup path uses
		// rather than assuming it was not delivered — the wrong assumption
		// sends a payment twice.
		l.queue.ScheduleRetry(queue.Attempt{ID: id, Method: method, Attempts: attempt}, err) //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
		return nil, err
	}

	// A response, of any status, means the request was delivered — which is
	// what the queue promises. Whether that response is worth repeating is the
	// caller's judgement, not pace's.
	if l.cfg.Queue.RetryOn != nil && l.cfg.Queue.RetryOn(ctx, RetryDecision{
		Response: resp,
		Method:   method,
		Path:     path,
		Attempt:  attempt,
	}) {
		l.queue.ScheduleRetry( //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
			queue.Attempt{ID: id, Method: method, Attempts: attempt, Delivered: true},
			fmt.Errorf("pace: durable: response %d rejected by RetryOn", resp.statusCode))
		return resp, nil
	}

	result := store.Result{
		StatusCode: resp.statusCode,
		Status:     resp.status,
		Headers:    resp.header,
		Body:       resp.body,
	}
	if cerr := l.queue.Complete(ctx, id, result); cerr != nil {
		// The response is in hand but could not be recorded. Log at Error, not
		// Warn: this is lost data, and the job is now ambiguous.
		l.cfg.Logger.Error("pace: durable: record result", "job", id, "err", cerr)
	} else {
		l.observeJob(JobInfo{ID: id, UserID: r.userID, Method: method, Phase: JobCompleted, Attempt: attempt})
	}
	return resp, nil
}
