package limiter

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/gate"
	"github.com/jaeminst/pace/persist"
	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/store"
)

// Limiter throttles outbound HTTP requests on a per-user basis toward a single
// base URL. It owns every resource involved: the idle-user GC goroutine and
// the state store.
//
// Create one with [New], derive a per-user handle with [Limiter.Client], and
// release resources with [Limiter.Close] or [Limiter.Shutdown]. A Limiter is
// safe for concurrent use by multiple goroutines.
type Limiter struct {
	cfg        Spec // resolved by the front door; the single source of configuration
	httpClient *http.Client
	ctx        context.Context
	cancel     context.CancelFunc
	// reg owns the user population: the sharded map, each user's bucket,
	// their persistence and their eviction. newRegistry below is the wiring.
	reg   *registry.Registry
	store store.Store // nil when no persistence is configured
	// state is the persistence policy over that store, and what the registry
	// actually calls. It is rebuilt, never mutated, when store changes.
	state        *persist.Adapter
	stats        counters
	closeOnce    sync.Once
	closeErr     error // recorded by the first close; returned by every later one
	gcWg         sync.WaitGroup
	shutdownMu   sync.RWMutex
	shuttingDown bool
	activeWg     sync.WaitGroup
	// gate is the shared-quota decision, nil when no backend is configured. It
	// owns the circuit breaker and the three counters describing backend calls,
	// because nothing else writes them.
	gate *gate.Gate
	// hooks is nil in production; see hooks.go.
	hooks atomic.Pointer[hooks]
}

// New builds an engine from an already-resolved [Spec] and starts its GC
// goroutine. Call [Limiter.Close] or [Limiter.Shutdown] when it is no longer
// needed.
//
// It panics on a Spec it cannot work with rather than returning an error:
// this is a vtable, its owner has already validated what a caller supplied, and
// anything wrong here is a wiring bug. Callers configure a Limiter through
// github.com/jaeminst/pace.New, which is what does return an error.
//
// Bind a user identity with [Limiter.Client].
func New(spec Spec) *Limiter {
	spec.validate()

	ctx, cancel := context.WithCancel(context.Background())
	l := &Limiter{
		cfg:        spec,
		httpClient: spec.HTTPClient,
		ctx:        ctx,
		cancel:     cancel,
		store:      spec.Store,
	}
	l.state = l.newState()
	l.reg = l.newRegistry()
	l.gate = l.newGate()
	l.gcWg.Add(1)
	go l.gcLoop()

	return l
}

// newState builds the persistence half of the registry.
//
// It is rebuilt rather than mutated when the backing store changes, which is
// what lets [persist.Adapter] hold no state of its own; l.store stays the one
// place the store lives, because Close reads it too.
func (l *Limiter) newState() *persist.Adapter {
	return persist.New(persist.Config{
		Store:    l.store,
		Shadowed: l.cfg.Shared.Quota != nil,
		Timeout:  l.cfg.StoreTimeout,
		Logger:   l.cfg.Logger,
	})
}

// newRegistry wires the user population to this Limiter.
//
// Everything the registry needs arrives as a value or a function, so it never
// imports this package. The split is not arbitrary: the registry decides which
// users exist and when they are evicted, and holds the shard locks while doing
// it; everything below decides what persisting or reporting one *means*, which
// is where [persist.Adapter], [Observer] and [Quota] live.
func (l *Limiter) newRegistry() *registry.Registry {
	return registry.New(registry.Config{
		Shards:     l.cfg.Shards,
		IdleExpiry: l.cfg.IdleExpiry,
		Now:        l.cfg.Now,
		QuotaFor: func(userID string) (float64, int) {
			q := l.cfg.Quota(userID)
			return float64(q.Rate), q.Burst
		},
		// Method values on the adapter, so a store swapped in after
		// construction is honoured: newState rebuilds it and the registry keeps
		// calling through l.state.
		Persists: func() bool { return l.state.Persists() },
		Load: func(ctx context.Context, userID string) (registry.Snapshot, bool) {
			return l.state.Load(ctx, userID)
		},
		Save: func(ctx context.Context, s registry.Snapshot) error {
			return l.state.Save(ctx, s)
		},
		Flush:    func(snaps []registry.Snapshot) { l.state.Flush(snaps) },
		Observes: l.observesEvictions,
		OnEvict:  l.onEvict,
		// Method values, not the hooks themselves: New starts the GC goroutine
		// before a test can install one.
		OnGetOrCreate: l.fireGetOrCreate,
		AfterSweep:    l.fireAfterSweep,
	})
}

// newGate wires the shared-quota decision to this Limiter.
//
// It is nil when no backend is configured, and that nil is the enabled test:
// building one would put a circuit breaker and three counters on every Limiter
// that never consults a backend.
//
// It lives here rather than in a file of its own because it is a constructor
// and nothing else. The two translations that used to keep it company — the
// gate's error into a LimitError, its throttle report into a ThrottleInfo —
// belong with the types they produce, and are in errors.go and observer.go.
func (l *Limiter) newGate() *gate.Gate {
	if l.cfg.Shared.Quota == nil {
		return nil
	}
	return gate.New(l.ctx, gate.Config{
		Quota:     l.cfg.Shared.Quota,
		Namespace: l.cfg.Shared.Namespace,
		Timeout:   l.cfg.Shared.Timeout,
		OnError:   l.cfg.Shared.OnError,
		Logger:    l.cfg.Logger,
		Now:       l.cfg.Now,
		Closed:    ErrClosed,
		Throttled: l.reportBucketTokens,
		// A method value, not the hook itself: a test may install one after the
		// Limiter has started.
		BeforeWait: l.fireBeforeWait,
		// gate requires this one to be non-nil and nothing here needs it. A
		// hook nothing can install is worse than none: it reads as a seam and
		// is a no-op. Add a setter in export_test.go if a test ever wants the
		// window before a backend call.
		BeforeQuotaTake: func() {},
	})
}

// sharedEnabled reports whether requests at this quota must consult the
// backend.
//
// An infinite rate skips it: there is nothing to ration, and a round-trip per
// request to be told so would be pure cost. The check is here rather than in
// gate because [Inf] is this package's constant now, and gate would have had
// to compare against a bare math.MaxFloat64 to make the same decision.
func (l *Limiter) sharedEnabled(q Quota) bool {
	return l.gate != nil && q.Rate != Inf
}

// Client returns a handle bound to userID. It is lightweight and safe for
// concurrent use; every Client derived from one Limiter shares that Limiter's
// rate-limiter state and store.
func (l *Limiter) Client(userID string) *Client {
	return &Client{userID: userID, lim: l}
}

// Close stops the background GC goroutine and flushes all in-memory user state
// to the configured store. Close is idempotent; it reports the store's close
// error, if any.
//
// It cancels in-flight requests rather than waiting for them: every request
// runs under a context derived from the Limiter's own. Use [Limiter.Shutdown]
// to let them finish first.
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
	l.fireShuttingDown()

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

// enter registers an operation that must finish before shutdown completes. It
// reports false when the Limiter is already shutting down, in which case
// nothing was registered and [Limiter.leave] must not be called.
//
// The check and the Add share one mutex acquisition on purpose: that is what
// stops a registration slipping in between finish setting shuttingDown and its
// activeWg.Wait. Every path that touches the store after this point goes
// through here, so the invariant lives in one place rather than being restated
// — and forgotten — at each call site.
func (l *Limiter) enter() bool {
	l.shutdownMu.RLock()
	defer l.shutdownMu.RUnlock()
	if l.shuttingDown {
		return false
	}
	l.activeWg.Add(1)
	return true
}

// leave releases a registration taken by [Limiter.enter].
func (l *Limiter) leave() { l.activeWg.Done() }

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
// Store I/O has two producers — the GC sweep and new-user loads — so both must
// be drained first. gcWg in
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
		l.activeWg.Wait()
		// Persist before discarding: dropUsers empties the shards, so a flush
		// after it would find nothing to write.
		if l.state.Persists() {
			l.state.Flush(l.reg.SnapshotAll())
		}
		// Drop whether or not there is a store: shutdown discards every user's
		// in-memory state either way, and an observer watching the population
		// should see it go rather than have it vanish silently.
		l.reg.DropAll()
		var cerr error
		// Discovered by assertion rather than required by StateStore: a store
		// with nothing to release should not have to write an empty method.
		if c, ok := l.store.(io.Closer); ok && l.store != nil {
			cerr = c.Close()
		}
		if cerr != nil {
			l.cfg.Logger.Warn("pace: close store", "err", cerr)
			l.closeErr = fmt.Errorf("pace: close store: %w", cerr)
		}
	})
	return l.closeErr
}

// gcLoop drives the idle-user sweep.
func (l *Limiter) gcLoop() {
	defer l.gcWg.Done()
	ticker := time.NewTicker(l.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.reg.Sweep()
		case <-l.ctx.Done():
			return
		}
	}
}
