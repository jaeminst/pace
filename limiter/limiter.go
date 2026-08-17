package limiter

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/jaeminst/pace/store"

	"github.com/jaeminst/pace/breaker"
	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/runner"
	"github.com/jaeminst/pace/sqlite"
)

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
	ctx        context.Context
	cancel     context.CancelFunc
	// reg owns the user population: the sharded map, each user's bucket,
	// their persistence and their eviction. See registry.go for the wiring.
	reg   *registry.Registry
	store store.Store // nil when no persistence is configured
	// stateIsSQLite records whether store is the sqliteStore handle wrapped as
	// a StateStore, rather than a caller-supplied backend. When it is false and
	// sqliteStore is non-nil the two are separate resources, and Close has to
	// shut both.
	stateIsSQLite bool
	owner         string // identifies this process when claiming durable jobs
	stats         counters
	closeOnce     sync.Once
	closeErr      error // recorded by the first close; returned by every later one
	gcWg          sync.WaitGroup
	// shutdown tracking
	shutdownMu   sync.RWMutex
	shuttingDown bool
	activeWg     sync.WaitGroup
	// The durable queue. Both are non-nil exactly when DBPath is set, and
	// queue != nil if and only if sqliteStore != nil.
	//
	// sqliteStore stays here rather than moving behind queue: sqlite
	// already owns the tables, the live send path claims and reads results
	// through it, and DeadJobs needs it too. Routing those through the queue
	// would add pass-through methods that remove no coupling.
	sqliteStore *sqlite.Store
	queue       *runner.Queue
	// The in-process singleflight. Not queue state: it deduplicates concurrent
	// callers of the same job ID within one process, which is meaningful with
	// no queue at all, and it caches *Response — the one type that must not
	// cross into runner.
	inflightMu sync.Mutex
	inflight   map[string]*future
	// quotaBreaker short-circuits a failing SharedQuota; zero value is closed.
	quotaBreaker breaker.Breaker
	// hooks is nil in production; see hooks.go.
	hooks atomic.Pointer[hooks]
}

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

// openStore resolves cfg's two persistence fields into the state store and the
// durable queue, either of which may be nil.
//
// They are not alternatives, which is what the mutually-exclusive check used to
// make them. DBPath owns the durable queue; Store owns per-user token state.
// Forbidding both meant a caller with a Redis backend could never have a queue
// at all, silently — openStore returned a *sqlite.Store only on the DBPath
// branch, and New has no other way to get one.
//
// When both are set, SQLite still opens but serves the queue alone and leaves
// user_state empty. The third return value says whether the Limiter owns that
// handle as its state store too, which decides whether Close has one thing to
// shut or two.
func openStore(cfg Config) (store.Store, *sqlite.Store, bool, error) {
	var db *sqlite.Store
	if cfg.DBPath != "" {
		s, err := sqlite.OpenStore(cfg.DBPath)
		if err != nil {
			return nil, nil, false, fmt.Errorf("pace: open store: %w", err)
		}
		db = s
	}
	switch {
	case cfg.Store != nil:
		return cfg.Store, db, false, nil
	case db != nil:
		return sqliteStateStore{s: db}, db, true, nil
	}
	return nil, nil, false, nil
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
	st, sqlite, stateIsSQLite, err := openStore(cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	l := &Limiter{
		cfg:           cfg,
		httpClient:    &http.Client{Transport: cfg.Transport},
		ctx:           ctx,
		cancel:        cancel,
		store:         st,
		stateIsSQLite: stateIsSQLite,
		inflight:      make(map[string]*future),
		owner:         newOwnerID(),
	}
	l.reg = l.newRegistry()
	l.gcWg.Add(1)
	go l.gcLoop()

	// Wire up the durable queue when the SQLite backend is active. The schema
	// is created by OpenStore's migration, so there is nothing to set up here.
	//
	// Assigned before Start, never inside newQueue: the first thing a replayed
	// job does is call back through the dispatcher into l.queue, so building
	// and starting in one step would race this assignment.
	if sqlite != nil {
		l.sqliteStore = sqlite
		l.queue = l.newQueue(sqlite)
		l.queue.Start()
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
		if l.queue != nil {
			// After the GC (which drives PurgeResults) and before activeWg, so
			// no queue goroutine is still inside a dispatch when the store
			// closes. Queue.Wait drains the poller before the replay.
			l.queue.Wait()
		}
		l.activeWg.Wait()
		// Persist before discarding: dropUsers empties the shards, so a flush
		// after it would find nothing to write.
		if l.persistsState() {
			l.flush(l.reg.SnapshotAll())
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
		// A caller-supplied Store and a DBPath queue are two separate handles.
		// Closing only l.store would leak the SQLite file, which on Windows
		// means the next t.TempDir cleanup fails rather than anything obvious.
		if l.sqliteStore != nil && !l.stateIsSQLite {
			cerr = errors.Join(cerr, l.sqliteStore.Close())
		}
		if cerr != nil {
			l.cfg.Logger.Warn("pace: close store", "err", cerr)
			l.closeErr = fmt.Errorf("pace: close store: %w", cerr)
		}
	})
	return l.closeErr
}

// hooks are test seams: points where a test needs to observe or pause the
// Limiter to make a race deterministic rather than time it.
//
// The set is nil in production and every call site checks for that, so the cost
// is an atomic load and a predictable branch, sitting next to work that dwarfs
// both — a mutex acquisition, a database write, an HTTP round-trip. Keeping
// them in one named type rather than as loose function fields on Limiter makes
// it obvious what they are and keeps the production struct describing
// production.
//
// The pointer is atomic because New starts the GC goroutine before a test can
// install anything, so the write genuinely races the read.
//
// They exist because the alternative is sleeping. A test that waits 20ms for a
// goroutine to reach a particular line is both slower than it needs to be and
// wrong under load, which is exactly when CI runs.
type hooks struct {
	// getOrCreate fires in userFor's cold path, before the write lock, so a
	// test can force two goroutines to race for the same new user.
	getOrCreate func()

	// beforeWait fires immediately before a caller blocks for a token, so a
	// test can act once the goroutine is genuinely waiting.
	beforeWait func()

	// durableBeforeEnqueue fires in the durable path before the job row is
	// written.
	durableBeforeEnqueue func()

	// afterSweep fires at the end of each GC sweep, so a test can wait for one
	// to have happened instead of guessing how long the ticker takes.
	afterSweep func()

	// shuttingDown fires once Shutdown has closed the door to new requests but
	// before it has cancelled anything, which is the only window in which the
	// "refused because shutting down" branch is reachable.
	shuttingDown func()

	// afterPoll fires at the end of each queue poll, once everything that was
	// due has finished. It is what lets a test assert that nothing further
	// happened: waiting for N polls to complete proves the queue was inspected
	// and found nothing, where sleeping only proves that time passed.
	afterPoll func()

	// beforeQuotaTake fires immediately before a SharedQuota call, so a test
	// can drive a shutdown or a breaker transition into that window without
	// timing it.
	beforeQuotaTake func()
}

// fireGetOrCreate and friends keep the nil checks in one place.
func (l *Limiter) fireGetOrCreate() {
	if h := l.hooks.Load(); h != nil && h.getOrCreate != nil {
		h.getOrCreate()
	}
}

func (l *Limiter) fireBeforeWait() {
	if h := l.hooks.Load(); h != nil && h.beforeWait != nil {
		h.beforeWait()
	}
}

func (l *Limiter) fireDurableBeforeEnqueue() {
	if h := l.hooks.Load(); h != nil && h.durableBeforeEnqueue != nil {
		h.durableBeforeEnqueue()
	}
}

func (l *Limiter) fireAfterSweep() {
	if h := l.hooks.Load(); h != nil && h.afterSweep != nil {
		h.afterSweep()
	}
}

func (l *Limiter) fireShuttingDown() {
	if h := l.hooks.Load(); h != nil && h.shuttingDown != nil {
		h.shuttingDown()
	}
}

func (l *Limiter) fireAfterPoll() {
	if h := l.hooks.Load(); h != nil && h.afterPoll != nil {
		h.afterPoll()
	}
}

func (l *Limiter) fireBeforeQuotaTake() {
	if h := l.hooks.Load(); h != nil && h.beforeQuotaTake != nil {
		h.beforeQuotaTake()
	}
}
