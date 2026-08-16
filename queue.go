package pace

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jaeminst/pace/internal/store"
)

// future represents an in-flight Durable execution.
type future struct {
	done chan struct{} // closed when the job finishes
	resp *Response
	err  error
}

const (
	// completeAttempts is how many times recording a result is retried. The
	// response is already in hand, so a transient write failure is worth a few
	// retries rather than an immediate loss.
	completeAttempts = 3
	// completeRetryDelay is the first backoff between those attempts; it
	// doubles each time.
	completeRetryDelay = 10 * time.Millisecond
)

// queueBatchFactor is how many due jobs a single poll fetches per worker. A
// small multiple keeps the workers fed without loading the whole backlog.
const queueBatchFactor = 4

// replay recovers the queue at startup.
//
// It first decides the fate of jobs left mid-flight by a previous process, then
// drains whatever is due through the same bounded path the background poller
// uses. It deliberately does not spawn a goroutine per pending job: a large
// backlog would otherwise become an equally large burst of goroutines, each
// holding a request and a body buffer.
func (l *Limiter) replay() {
	defer l.replayWg.Done()
	l.recoverStranded()
	l.runDueJobs()
}

// recoverStranded classifies jobs whose intent to send was committed but whose
// outcome was never recorded.
//
// The process that owned them is gone, so the server may or may not have acted.
// Jobs that are unsafe to repeat are parked here; the rest are simply left for
// the poller, which treats an expired lease as eligible.
func (l *Limiter) recoverStranded() {
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.StoreTimeout)
	defer cancel()

	jobs, err := l.sqliteStore.Pending(ctx)
	if err != nil {
		if l.ctx.Err() == nil {
			l.cfg.Logger.Warn("pace: replay: load pending", "err", err)
		}
		return
	}
	for _, j := range jobs {
		if j.State != store.StateSending {
			continue
		}
		if l.cfg.Queue.AmbiguousPolicy.resolve(j.Method, l.cfg.Queue.IdempotencyHeader) {
			continue
		}
		l.killJob(j, "outcome unknown after restart and the request is not safe to repeat")
	}
}

// killJob moves a job to the dead-letter table and reports it.
func (l *Limiter) killJob(j store.Job, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
	defer cancel()

	killed, ok, err := l.sqliteStore.Kill(ctx, j.ID, reason, l.cfg.Clock.Now().UnixNano())
	if err != nil {
		l.cfg.Logger.Error("pace: durable: dead-letter", "job", j.ID, "err", err)
		return
	}
	if !ok {
		return // already gone; another worker completed or killed it
	}
	l.cfg.Logger.Warn("pace: durable: job abandoned", "job", killed.ID, "attempts", killed.Attempts, "reason", reason)
	l.observeJob(JobInfo{
		ID: killed.ID, UserID: killed.UserID, Method: killed.Method,
		Phase: JobDead, Attempt: killed.Attempts, Reason: reason,
	})
	if l.cfg.Queue.OnDeadLetter != nil {
		l.cfg.Queue.OnDeadLetter(l.ctx, DeadJob{
			ID:       killed.ID,
			UserID:   killed.UserID,
			Method:   killed.Method,
			Path:     killed.Path,
			Headers:  killed.Headers,
			Body:     killed.Body,
			Attempts: killed.Attempts,
			Reason:   reason,
		})
	}
}

// releaseJob returns a durable job to the queue after a failure that provably
// happened before dispatch.
//
// It deliberately does not take the request's context. The most common reason
// to be here is that acquiring a token failed because that context was
// cancelled, and reusing it would make this write fail too — leaving the job in
// StateSending, where a restart would classify as ambiguous a request we know
// for certain never left the process. StoreTimeout bounds it instead.
func (l *Limiter) releaseJob(id string, cause error) {
	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
	defer cancel()
	now := l.cfg.Clock.Now().UnixNano()
	released, err := l.sqliteStore.Release(ctx, id, l.owner, now, now, cause.Error())
	switch {
	case err != nil:
		l.cfg.Logger.Warn("pace: durable: release", "job", id, "err", err)
	case !released:
		l.cfg.Logger.Warn("pace: durable: release skipped, job no longer owned here",
			"job", id, "err", cause)
	}
}

// completeJob records a job's result, retrying briefly. The response is already
// in hand at this point, so giving up immediately would throw away work that
// cannot be redone without asking the server again.
func (l *Limiter) completeJob(ctx context.Context, id string, resp *Response) error {
	result := store.Result{
		StatusCode: resp.statusCode,
		Status:     resp.status,
		Headers:    resp.header,
		Body:       resp.body,
	}
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
		if err = l.sqliteStore.Complete(ctx, id, result, l.cfg.Clock.Now().UnixNano()); err == nil {
			return nil
		}
	}
	return err
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

// job is the minimum a retry decision needs about a durable job.
type job struct {
	id       string
	method   string
	attempts int
	// delivered is true when the server answered. A delivered request is not
	// ambiguous: repeating it is a choice, not a gamble.
	delivered bool
}

// scheduleRetry decides what happens to a durable job that did not complete.
//
// The decision turns on one question: do we know whether the server saw it?
// A delivered request is unambiguous, so the only limit is the attempt
// allowance. A transport error is ambiguous, and repeating it is safe only
// under the same rules that govern a job found stranded after a crash — an
// idempotent method, or an idempotency key the server can collapse on.
func (l *Limiter) scheduleRetry(j job, cause error) {
	switch {
	case j.attempts >= l.cfg.Queue.Retry.MaxAttempts:
		l.killJob(store.Job{ID: j.id, Method: j.method},
			fmt.Sprintf("gave up after %d attempts: %v", j.attempts, cause))
		return
	case !j.delivered && !l.cfg.Queue.AmbiguousPolicy.resolve(j.method, l.cfg.Queue.IdempotencyHeader):
		l.killJob(store.Job{ID: j.id, Method: j.method},
			fmt.Sprintf("outcome unknown and the request is not safe to repeat: %v", cause))
		return
	}

	delay := l.cfg.Queue.Retry.backoff(j.attempts)
	next := l.cfg.Clock.Now().Add(delay).UnixNano()

	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
	defer cancel()
	released, err := l.sqliteStore.Release(ctx, j.id, l.owner, l.cfg.Clock.Now().UnixNano(), next, cause.Error())
	switch {
	case err != nil:
		l.cfg.Logger.Warn("pace: durable: schedule retry", "job", j.id, "err", err)
		return
	case !released:
		// The lease expired and another worker reclaimed the job. It owns the
		// retry decision now; scheduling one here would race with the send it
		// is already making.
		l.cfg.Logger.Warn("pace: durable: retry skipped, job no longer owned here",
			"job", j.id, "err", cause)
		return
	}
	l.cfg.Logger.Debug("pace: durable: retry scheduled",
		"job", j.id, "attempt", j.attempts, "in", delay)
	l.observeJob(JobInfo{
		ID: j.id, Method: j.method, Phase: JobRetrying,
		Attempt: j.attempts, RetryIn: delay, Err: cause,
	})
}

// pollQueue drives background retries. One goroutine looks for jobs that have
// become due and hands them to a bounded set of workers.
//
// The previous implementation spawned one goroutine per pending job at startup
// and never looked again: a fifty-thousand-job backlog became fifty thousand
// goroutines, each holding a request and a body buffer, and nothing retried
// afterwards until the next restart.
func (l *Limiter) pollQueue() {
	defer l.workerWg.Done()

	ticker := time.NewTicker(l.cfg.Queue.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
		}
		l.runDueJobs()
		l.fireAfterPoll()
	}
}

// runDueJobs claims and executes whatever is due, never running more than
// QueueWorkers at a time.
func (l *Limiter) runDueJobs() {
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.StoreTimeout)
	jobs, err := l.sqliteStore.Due(ctx, l.cfg.Clock.Now().UnixNano(), l.cfg.Queue.Workers*queueBatchFactor)
	cancel()
	if err != nil {
		if l.ctx.Err() == nil {
			l.cfg.Logger.Warn("pace: durable: poll", "err", err)
		}
		return
	}

	var wg sync.WaitGroup
	for _, j := range jobs {
		select {
		case l.queueSlots <- struct{}{}:
		case <-l.ctx.Done():
			wg.Wait()
			return
		}
		wg.Go(func() {
			defer func() { <-l.queueSlots }()
			l.runJob(j)
		})
	}
	wg.Wait()
}

// runJob executes one queued job. Failures are recorded by doDurable itself,
// so anything surfacing here is either a lost race for the claim — normal — or
// worth a log line.
func (l *Limiter) runJob(j store.Job) {
	req := newRequest(l, j.UserID)
	req.durable, req.durableID = true, j.ID
	req.body = j.Body
	req.headers = j.Headers.Clone()
	if _, err := req.do(l.ctx, j.Method, j.Path); err != nil &&
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

// resultPurgeChunk bounds one DELETE so a large purge cannot hold the SQLite
// writer for the whole operation.
const resultPurgeChunk = 1000

// purgeResults drops cached durable results past their TTL.
//
// It rides the existing GC tick rather than adding a goroutine: the idle-user
// sweep already runs on that schedule, and both are background housekeeping
// against the same store.
func (l *Limiter) purgeResults() {
	if l.sqliteStore == nil || l.cfg.Queue.ResultTTL < 0 {
		return
	}
	cutoff := l.cfg.Clock.Now().Add(-l.cfg.Queue.ResultTTL).UnixNano()

	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.StoreTimeout)
	defer cancel()

	n, err := l.sqliteStore.PurgeResults(ctx, cutoff, resultPurgeChunk)
	if err != nil {
		if l.ctx.Err() == nil {
			l.cfg.Logger.Warn("pace: durable: purge results", "err", err)
		}
		return
	}
	if n > 0 {
		l.cfg.Logger.Debug("pace: durable: purged cached results", "count", n)
	}
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
		l.releaseJob(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see releaseJob
		return nil, err
	}
	if l.cfg.Queue.IdempotencyHeader != "" {
		httpReq.Header.Set(l.cfg.Queue.IdempotencyHeader, id)
	}
	if err := l.acquire(ctx, r.userID); err != nil {
		// Nothing was dispatched, so the job is unambiguously still pending.
		l.releaseJob(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see releaseJob
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
		l.scheduleRetry(job{id: id, method: method, attempts: attempt}, err) //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
		return nil, err
	}

	// A response, of any status, means the request was delivered — which is
	// what the queue promises. Whether that response is worth repeating is the
	// caller's judgement, not pace's.
	if l.cfg.Queue.RetryOn != nil && l.cfg.Queue.RetryOn(resp) {
		l.scheduleRetry( //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
			job{id: id, method: method, attempts: attempt, delivered: true},
			fmt.Errorf("pace: durable: response %d rejected by RetryOn", resp.statusCode))
		return resp, nil
	}

	if cerr := l.completeJob(ctx, id, resp); cerr != nil {
		// The response is in hand but could not be recorded. Log at Error, not
		// Warn: this is lost data, and the job is now ambiguous.
		l.cfg.Logger.Error("pace: durable: record result", "job", id, "err", cerr)
	} else {
		l.observeJob(JobInfo{ID: id, UserID: r.userID, Method: method, Phase: JobCompleted, Attempt: attempt})
	}
	return resp, nil
}
