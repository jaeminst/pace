package pace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
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

// Limiter throttles outbound HTTP requests on a per-user basis toward a single
// base URL. It owns every resource involved: the idle-user GC goroutine, the
// state store, and the durable queue.
//
// Create one with [New], derive a per-user handle with [Limiter.Client], and
// release resources with [Limiter.Close] or [Limiter.Shutdown]. A Limiter is
// safe for concurrent use by multiple goroutines.
type Limiter struct {
	cfg        Config // validated and defaulted; the single source of configuration
	httpClient *http.Client
	shards     []shard // len is a power of two; shardMask is len-1
	shardMask  uint32
	ctx        context.Context
	cancel     context.CancelFunc
	store      StateStore // nil when no persistence is configured
	owner      string     // identifies this process when claiming durable jobs
	closeOnce  sync.Once
	closeErr   error // recorded by the first close; returned by every later one
	gcWg       sync.WaitGroup
	// shutdown tracking
	shutdownMu   sync.RWMutex
	shuttingDown bool
	activeWg     sync.WaitGroup
	// durable queue (non-nil only when opened via DBPath)
	sqliteStore *store.Store
	inflightMu  sync.Mutex
	inflight    map[string]*future
	replayWg    sync.WaitGroup
	// _testHookGetOrCreate is called in userFor's cold path before the write lock.
	_testHookGetOrCreate func()
	// _testHookDurableBeforeEnqueue is called in doDurable before Enqueue; nil in production.
	_testHookDurableBeforeEnqueue func()
}

// validate reports the first invalid field in cfg.
func (cfg *Config) validate() error {
	if cfg.BaseURL == "" {
		return &ConfigError{Field: "BaseURL", Err: errors.New("required")}
	}
	if cfg.Rate <= 0 {
		return &ConfigError{Field: "Rate", Value: cfg.Rate, Err: errors.New("must be greater than zero")}
	}
	if cfg.Store != nil && cfg.DBPath != "" {
		return &ConfigError{Field: "Store", Err: errors.New("mutually exclusive with Config.DBPath")}
	}
	if cfg.Shards > maxShards {
		return &ConfigError{
			Field: "Shards",
			Value: cfg.Shards,
			Err:   fmt.Errorf("must not exceed %d", maxShards),
		}
	}
	return nil
}

// withDefaults returns a copy of cfg with every optional field resolved, so
// nothing downstream has to re-check for zero values.
func (cfg Config) withDefaults() Config {
	if cfg.Burst <= 0 {
		cfg.Burst = 1
	}
	cfg.Shards = roundUpPowerOfTwo(cfg.Shards)
	if cfg.IdleExpiry <= 0 {
		cfg.IdleExpiry = 10 * time.Minute
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = time.Minute
	}
	if cfg.StoreTimeout <= 0 {
		cfg.StoreTimeout = 5 * time.Second
	}
	if cfg.JobLease <= 0 {
		cfg.JobLease = 5 * time.Minute
	}
	switch cfg.IdempotencyHeader {
	case "":
		cfg.IdempotencyHeader = "Idempotency-Key"
	case noIdempotencyHeader:
		cfg.IdempotencyHeader = ""
	}
	if cfg.Clock == nil {
		cfg.Clock = stdClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Transport == nil {
		cfg.Transport = http.DefaultTransport
	}
	return cfg
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

// noIdempotencyHeader is the sentinel a caller sets Config.IdempotencyHeader
// to in order to send no header at all. An empty string cannot mean that,
// because the zero value has to select the default.
const noIdempotencyHeader = "-"

// newOwnerID returns a value that identifies this Limiter when it claims
// durable jobs. It only has to be distinct from other processes sharing the
// same database file, so that an expired lease can be told apart from a claim
// this process still holds.
func newOwnerID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail in practice; if it ever does, a constant
		// still leaves claims correct, only lease attribution ambiguous.
		return "pace-unknown-owner"
	}
	return hex.EncodeToString(b[:])
}

// maxShards bounds Config.Shards. Far beyond any useful striping, but it makes
// the shard count provably small enough to mask with a uint32 and stops
// roundUpPowerOfTwo from overflowing.
const maxShards = 1 << 20

// roundUpPowerOfTwo returns the smallest power of two at least n, defaulting to
// numShards for non-positive input. shardIndex masks rather than divides, which
// requires the count to be a power of two.
func roundUpPowerOfTwo(n int) int {
	if n <= 0 {
		return numShards
	}
	if n > maxShards {
		n = maxShards
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

// openStore returns the StateStore implied by cfg, or nil if no persistence is
// requested. The built-in SQLite backend is adapted to the same public
// interface a caller would implement, so there is one code path, not two.
func openStore(cfg Config) (StateStore, *store.Store, error) {
	switch {
	case cfg.Store != nil:
		return cfg.Store, nil, nil
	case cfg.DBPath != "":
		s, err := store.OpenStore(cfg.DBPath)
		if err != nil {
			return nil, nil, fmt.Errorf("pace: open store: %w", err)
		}
		return sqliteStateStore{s: s}, s, nil
	}
	return nil, nil, nil
}

// New creates a Limiter from cfg. It starts a background GC goroutine and opens
// the configured store (SQLite or custom). Call [Limiter.Close] or
// [Limiter.Shutdown] when the Limiter is no longer needed.
//
// Bind a user identity with [Limiter.Client].
func New(cfg Config) (*Limiter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	ctx, cancel := context.WithCancel(context.Background())
	st, sqlite, err := openStore(cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	l := &Limiter{
		cfg:        cfg,
		httpClient: &http.Client{Transport: cfg.Transport},
		ctx:        ctx,
		cancel:     cancel,
		store:      st,
		inflight:   make(map[string]*future),
		shards:     newShards(cfg.Shards),
		owner:      newOwnerID(),
	}
	// Safe: validate rejects anything above maxShards (2^20).
	l.shardMask = uint32(len(l.shards) - 1) //nolint:gosec // shard count is bounded by maxShards
	l.gcWg.Add(1)
	go l.gcLoop()

	// Wire up the durable queue when the SQLite backend is active. The schema
	// is created by OpenStore's migration, so there is nothing to set up here.
	if sqlite != nil {
		l.sqliteStore = sqlite
		l.replayWg.Add(1)
		go l.replay()
	}

	return l, nil
}

// Client returns a handle bound to userID. It is lightweight and safe for
// concurrent use; every Client derived from one Limiter shares that Limiter's
// rate-limiter state, store, and durable queue.
func (l *Limiter) Client(userID string) *Client {
	return &Client{userID: userID, lim: l}
}

// Close stops the background GC goroutine and flushes all in-memory user state
// to the configured store. Close is idempotent; it reports the store's close
// error, if any.
func (l *Limiter) Close() error { return l.close() }

// Shutdown stops the Limiter gracefully. It prevents new requests and waits
// until ctx expires (or all in-flight requests finish) before cleaning up.
// If ctx expires first, remaining waiters are force-cancelled and Shutdown
// returns ctx.Err(). The store is always flushed and closed on return.
// Shutdown is idempotent via the underlying Close call.
func (l *Limiter) Shutdown(ctx context.Context) error {
	// Stop accepting new requests.
	l.shutdownMu.Lock()
	l.shuttingDown = true
	l.shutdownMu.Unlock()

	// Wait for active requests to finish, honouring the caller's deadline.
	waitDone := make(chan struct{})
	go func() {
		l.activeWg.Wait()
		close(waitDone)
	}()

	var shutdownErr error
	select {
	case <-waitDone:
	case <-ctx.Done():
		shutdownErr = ctx.Err()
		l.cancel() // force-cancel remaining waiters
		<-waitDone
	}

	// finish deliberately does not receive ctx. Shutdown documents that the
	// store is always flushed on return, and by this point ctx may already be
	// expired — that is the branch above. Inheriting it would discard the flush
	// at exactly the moment it matters. StoreTimeout bounds it instead; see
	// TestFinalFlushSurvivesLimiterCancellation.
	closeErr := l.finish() //nolint:contextcheck // the final flush must outlive the caller's shutdown deadline
	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}

// withLifetime derives a context cancelled when either ctx or the Limiter's
// lifetime ends, so shutting the Limiter down aborts work already in progress.
//
// context.AfterFunc rather than a goroutine: nothing is spawned in the common
// case where the Limiter outlives the request. Deriving this once per request
// and reusing it — rather than re-merging inside every operation — keeps the
// hot path to a single context allocation.
func (l *Limiter) withLifetime(ctx context.Context) (context.Context, func()) {
	merged, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	return merged, func() {
		stop()
		cancel()
	}
}

// acquire blocks until userID has a token or ctx is done. ctx must already be
// merged with the Limiter's lifetime via withLifetime. Callers are responsible
// for the activeWg registration around it.
func (l *Limiter) acquire(ctx context.Context, userID string) error {
	now := l.cfg.Clock.Now()
	u := l.userFor(ctx, userID)
	u.lastUsed.Store(now.UnixNano())
	if l.cfg.OnThrottle != nil && !u.bucket.HasTokenAt(now) {
		l.cfg.OnThrottle(userID)
	}
	if err := u.bucket.Wait(ctx); err != nil {
		// Ask the Limiter's own context whether it shut down, rather than
		// inferring it from the caller's context still being live. The
		// limiter reports "would exceed context deadline" without waiting,
		// so ctx.Err() is legitimately nil in that case too — treating that
		// as ErrClosed told callers the Limiter was closed when it was not.
		if l.ctx.Err() != nil {
			return ErrClosed
		}
		return &LimitError{
			UserID: userID,
			Limit:  l.cfg.Rate,
			Burst:  l.cfg.Burst,
			Err:    err,
		}
	}
	return nil
}

// allow consumes a token for userID if one is immediately available.
func (l *Limiter) allow(userID string) bool {
	l.shutdownMu.RLock()
	shuttingDown := l.shuttingDown
	l.shutdownMu.RUnlock()
	if shuttingDown {
		return false
	}
	now := l.cfg.Clock.Now()
	// Allow never blocks, so it gets its own bounded context rather than
	// inheriting one it was not given.
	ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
	defer cancel()
	u := l.userFor(ctx, userID)
	u.lastUsed.Store(now.UnixNano())
	if !u.bucket.AllowAt(now) {
		if l.cfg.OnThrottle != nil {
			l.cfg.OnThrottle(userID)
		}
		return false
	}
	return true
}

// tokens returns the approximate number of available tokens for userID.
// Returns -1 if the user has no in-memory state (not yet seen, or already GC'd).
func (l *Limiter) tokens(userID string) float64 {
	sh := l.shardFor(userID)
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if !ok {
		return -1
	}
	return u.bucket.TokensAt(l.cfg.Clock.Now())
}

// evictUser removes userID from the in-memory shard immediately. If a store is
// configured, the current token state is saved before removal. Returns false
// if the user had no in-memory state.
func (l *Limiter) evictUser(userID string) bool {
	sh := l.shardFor(userID)
	sh.mu.Lock()
	u, ok := sh.users[userID]
	if ok {
		l.evict(sh, userID, u, l.cfg.Clock.Now())
	}
	sh.mu.Unlock()
	return ok
}

// close marks the Limiter as shutting down and then tears it down. It is
// idempotent; repeated calls return the error recorded by the first.
func (l *Limiter) close() error {
	l.shutdownMu.Lock()
	l.shuttingDown = true
	l.shutdownMu.Unlock()
	return l.finish()
}

// finish is the single teardown sequence, shared by Close and Shutdown so the
// ordering exists in exactly one place.
//
// The invariant it establishes: once the store is closed, nothing may touch it.
// Store I/O has four producers — the GC sweep, new-user loads, the durable
// queue, and replay — so every one of them must be drained first. gcWg in
// particular used to be started and never waited on, leaving a sweep free to
// Save into a store that Close had already shut.
//
// Waiting on activeWg cannot deadlock: shuttingDown is set before finish runs,
// and it is checked under the same mutex that guards activeWg.Add, so no new
// registration can appear after this point.
func (l *Limiter) finish() error {
	// sync.Once establishes happens-before for closeErr, so later callers read
	// what the first writer stored.
	l.closeOnce.Do(func() {
		l.cancel()
		l.gcWg.Wait()
		l.replayWg.Wait()
		l.activeWg.Wait()
		if l.store != nil {
			l.saveAll()
			if cerr := l.store.Close(); cerr != nil {
				l.cfg.Logger.Warn("pace: close store", "err", cerr)
				l.closeErr = fmt.Errorf("pace: close store: %w", cerr)
			}
		}
	})
	return l.closeErr
}

// replay decides what to do with jobs left behind by a previous run.
//
// A job in StateQueued was persisted but never dispatched, so re-sending it is
// unambiguously correct. A job in StateSending had its intent to send committed
// before dispatch, which means the process died without learning the outcome —
// the server may or may not have acted. That window cannot be closed from this
// side of the wire, so the job's fate is decided by AmbiguousPolicy rather than
// guessed at.
//
// The previous implementation replayed everything indiscriminately, which sends
// a non-idempotent request a second time whenever a crash lands in that window.
func (l *Limiter) replay() {
	defer l.replayWg.Done()
	jobs, err := l.sqliteStore.Pending(l.ctx)
	if err != nil {
		l.cfg.Logger.Warn("pace: replay: load pending", "err", err)
		return
	}
	for _, j := range jobs {
		if j.State == store.StateSending && !l.cfg.AmbiguousPolicy.resolve(j.Method, l.cfg.IdempotencyHeader) {
			l.killJob(j, "outcome unknown after restart and the request is not safe to repeat")
			continue
		}
		l.replayWg.Go(func() {
			req := newRequest(l, j.UserID)
			req.durable, req.durableID = true, j.ID
			req.body = j.Body
			req.headers = j.Headers.Clone()
			// l.ctx, not context.Background(): a replayed job must be
			// cancellable when the Limiter shuts down.
			if _, err := req.do(l.ctx, j.Method, j.Path); err != nil && !errors.Is(err, ErrJobClaimed) {
				l.cfg.Logger.Warn("pace: replay: execute", "job", j.ID, "err", err)
			}
		})
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
	if l.cfg.OnDeadLetter != nil {
		l.cfg.OnDeadLetter(DeadJob{
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
	next := l.cfg.Clock.Now().UnixNano()
	if err := l.sqliteStore.Release(ctx, id, next, cause.Error()); err != nil {
		l.cfg.Logger.Warn("pace: durable: release", "job", id, "err", err)
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
		if err = l.sqliteStore.Complete(ctx, id, result); err == nil {
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

func toResponse(r *store.Result) *Response {
	return &Response{
		statusCode: r.StatusCode,
		status:     r.Status,
		body:       r.Body,
		header:     r.Headers,
	}
}

// DeadJobs returns durable jobs that were abandoned rather than retried, most
// recent first, up to limit (zero or negative means 100).
//
// Dead jobs are the ones a human has to decide about. Without a way to read
// them back, they would be visible only to a [Config.OnDeadLetter] callback
// that happened to be registered at the moment they were abandoned.
func (l *Limiter) DeadJobs(ctx context.Context, limit int) ([]DeadJob, error) {
	if l.sqliteStore == nil {
		return nil, ErrNoQueue
	}
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
