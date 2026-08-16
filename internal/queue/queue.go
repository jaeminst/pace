// Package queue runs the durable request queue: the startup replay, the
// background poller, and the decisions about a job that did not complete.
//
// It knows nothing about HTTP. Sending is the owner's job and arrives back here
// as a [Dispatcher] — which is the one thing in this package that exists to
// break an import cycle rather than to do work.
//
// White-box tests for this package must not import the parent, or the cycle
// comes back. Everything the queue needs is a plain value or a function field
// on [Config], so a fake is a struct literal.
package queue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jaeminst/pace/internal/store"
)

// Dispatcher runs one durable job: it performs the request the job describes
// and records the outcome.
//
// It exists to break an import cycle, and it is the only thing here that does.
// Running a job means building a request, paying for a rate-limit token and
// reading a bounded response body — the whole of the owner's request path — and
// the owner imports this package, so that call has to arrive from outside it.
//
// It is a function rather than an interface because there is exactly one
// implementation needing exactly one method. An interface would have to declare
// that method exported, and the owner's Limiter satisfying it would put a
// Dispatch method on a public API where it means nothing to a caller.
//
// It reports no error. Every outcome an attempt can have is already written
// down by the dispatcher itself, through [Queue.Complete], [Queue.ScheduleRetry]
// or [Queue.Release]; an error returned here would be a second, weaker account
// of something already recorded.
type Dispatcher func(ctx context.Context, job store.Job)

// Config is everything the queue needs from its owner. Every field is required:
// the queue is constructed at exactly one call site, so nothing here is
// defaulted or nil-checked.
//
// It is values and functions rather than an interface for the reason the
// parent's Observer is a struct of functions rather than an interface: there is
// one implementation, there is no optional extension to discover by type
// assertion, and most of these are method values on types that already exist
// rather than closures written to satisfy a shape.
//
// The values are snapshotted at construction. That is safe only because the
// owner's config is immutable after its own defaulting runs; if that ever stops
// being true, these have to become funcs too.
type Config struct {
	// Store is the SQLite handle holding the queue tables. The queue does not
	// own it: the owner opens it, the owner closes it, and the owner reads the
	// same tables on the live durable path and to list dead jobs.
	Store *store.Store

	// Owner identifies this process when it releases a claim. It must be the
	// same string the live path claims with, or a release is refused.
	Owner  string
	Logger *slog.Logger

	// Now is the owner's clock, so every timestamp the queue writes comes from
	// the source the rest of the package reports against.
	Now func() time.Time

	// StoreTimeout bounds each database call.
	StoreTimeout time.Duration

	// PollInterval, Workers and ResultTTL come from the owner's queue config,
	// already defaulted.
	PollInterval time.Duration
	Workers      int
	ResultTTL    time.Duration

	// MaxAttempts is the attempt ceiling and Backoff is the schedule. The retry
	// policy type itself stays with the owner, where it is exported and
	// documented; the queue needs the two answers it produces, not the type.
	MaxAttempts int
	Backoff     func(attempt int) time.Duration

	// RepeatSafe reports whether a request whose outcome is unknown may be sent
	// again — the owner's ambiguity policy resolved against the method and the
	// configured idempotency header. The enum and its rules stay with the owner
	// for the same reason the retry policy does.
	RepeatSafe func(method string) bool

	// Dispatch runs one job; see [Dispatcher].
	Dispatch Dispatcher

	// OnDead reports a job moved to the dead-letter table. j is the row as it
	// was killed, so it carries everything the owner's public DeadJob needs.
	// The queue logs before calling this.
	OnDead func(j store.Job, reason string)

	// OnRetry reports a scheduled retry. Its arguments are positional rather
	// than a struct because there is one call site and no store.Job in hand
	// there — only the attempt that just failed.
	OnRetry func(id, method string, attempt int, retryIn time.Duration, cause error)

	// AfterPoll fires at the end of each poll, once everything due has
	// finished. Pass a method value that reads the hook at call time, not the
	// hook itself: a test may install one after the queue has started.
	AfterPoll func()
}

// Queue owns the background half of the durable queue.
type Queue struct {
	cfg Config
	ctx context.Context
	// slots bounds durable-job concurrency across every path that runs jobs.
	// One channel for the whole Queue: a semaphore each for the startup drain
	// and the poller would let them run Workers jobs apiece.
	slots    chan struct{}
	workerWg sync.WaitGroup
	replayWg sync.WaitGroup
}

// New builds a queue bound to ctx, which must be the owner's lifetime context.
// It starts nothing.
func New(ctx context.Context, cfg Config) *Queue {
	return &Queue{
		cfg:   cfg,
		ctx:   ctx,
		slots: make(chan struct{}, cfg.Workers),
	}
}

// Start launches the startup replay and the background poller.
//
// It is separate from New on purpose. The first thing a replayed job does is
// call Config.Dispatch, which calls straight back into this Queue — so starting
// the goroutines inside New would race the owner's assignment of the Queue to
// the field its dispatcher reads. Replay runs immediately, with no poll
// interval to hide behind, so the race is real rather than theoretical.
func (q *Queue) Start() {
	q.replayWg.Add(1)
	go q.replay()
	q.workerWg.Add(1)
	go q.poll()
}

// Wait blocks until the poller and the replay have exited. The owner must
// cancel ctx first, and must call this before closing the store.
func (q *Queue) Wait() {
	q.workerWg.Wait()
	q.replayWg.Wait()
}

// WaitReplay blocks until the startup replay has exited, with the poller still
// running. It is a test seam: it is how a suite waits for a recovered backlog
// to drain without waiting for shutdown.
func (q *Queue) WaitReplay() { q.replayWg.Wait() }
