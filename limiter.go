package pace

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
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
	// durable queue (non-nil only when opened via DBPath)
	sqliteStore *store.Store
	inflightMu  sync.Mutex
	inflight    map[string]*future
	replayWg    sync.WaitGroup
	workerWg    sync.WaitGroup
	// queueSlots bounds durable-job concurrency across every path that runs
	// them. It must be one channel for the whole Limiter: giving the startup
	// drain and the background poller a semaphore each would let them run
	// QueueWorkers jobs apiece.
	queueSlots chan struct{}
	// quotaBreaker short-circuits a failing SharedQuota; zero value is closed.
	quotaBreaker quotaBreaker
	// hooks is nil in production; see hooks.go.
	hooks atomic.Pointer[hooks]
}

// validate reports the first invalid field in cfg.
func (cfg *Config) validate() error {
	if cfg.BaseURL == "" {
		return &ConfigError{Field: "BaseURL", Err: errors.New("required")}
	}
	if err := validateBaseURL(cfg.BaseURL); err != nil {
		return &ConfigError{Field: "BaseURL", Value: cfg.BaseURL, Err: err}
	}
	if cfg.Rate <= 0 || math.IsNaN(float64(cfg.Rate)) {
		// NaN needs saying separately: it is not <= 0, so the check above lets
		// it through, and the bucket built from it holds NaN tokens and refuses
		// every request for the life of the process. Found by fuzzing.
		return &ConfigError{Field: "Rate", Value: cfg.Rate, Err: errors.New("must be greater than zero")}
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
	cfg.Rate = finiteRate(cfg.Rate)
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
	if cfg.QuotaTimeout <= 0 {
		cfg.QuotaTimeout = 500 * time.Millisecond
	}
	cfg.Queue = cfg.Queue.withDefaults()
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

// queueBatchFactor is how many due jobs a single poll fetches per worker. A
// small multiple keeps the workers fed without loading the whole backlog.
const queueBatchFactor = 4

// noIdempotencyHeader is the sentinel a caller sets QueueConfig.IdempotencyHeader
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

// validateBaseURL rejects a base that cannot produce a usable request URL.
//
// Checking it at New turns a typo into one clear error at startup, instead of
// an opaque failure from http.NewRequest on every call afterwards.
func validateBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if !u.IsAbs() {
		return errors.New("must be absolute, including a scheme")
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("unsupported scheme %q, want http or https", u.Scheme)
	}
	// Hostname rather than Host: "http://:" and "http://:8080" both have a
	// non-empty Host and no hostname at all, and a base like that produces
	// requests aimed at nothing while passing every other check. Found by
	// fuzzing buildURL.
	if u.Hostname() == "" {
		return errors.New("missing host")
	}
	return nil
}

// buildURL joins path onto the base and merges any query values set on the
// request.
//
// The path is concatenated rather than resolved with url.URL.JoinPath, which
// would percent-encode a query string written inline — "/items?limit=10" is
// common and would become "/items%3Flimit=10". What concatenation gets wrong is
// the seam, in both directions, so both are normalised.
//
// The missing slash is the one that matters. Against a base with no path of its
// own, a path that does not start with "/" runs straight into the host:
// "https://api.example.com" + ".evil.com/x" is a request to a host the caller
// never named. With any part of the path coming from user input that is a
// request-forgery primitive, so a separator is inserted rather than trusted to
// be there. Found by fuzzing.
func (l *Limiter) buildURL(path string, extra url.Values) (string, error) {
	base := l.cfg.BaseURL
	var full string
	switch {
	case strings.HasSuffix(base, "/") && strings.HasPrefix(path, "/"):
		full = base + path[1:]
	case !strings.HasSuffix(base, "/") && !strings.HasPrefix(path, "/"):
		full = base + "/" + path
	default:
		full = base + path
	}
	if len(extra) == 0 {
		return full, nil
	}
	u, err := url.Parse(full)
	if err != nil {
		return "", fmt.Errorf("pace: build request: %w", err)
	}
	q := u.Query()
	for k, vs := range extra {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

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

// openStore resolves cfg's two persistence fields into the state store and the
// durable queue, either of which may be nil.
//
// They are not alternatives, which is what the mutually-exclusive check used to
// make them. DBPath owns the durable queue; Store owns per-user token state.
// Forbidding both meant a caller with a Redis backend could never have a queue
// at all, silently — openStore returned a *store.Store only on the DBPath
// branch, and New has no other way to get one.
//
// When both are set, SQLite still opens but serves the queue alone and leaves
// user_state empty. The third return value says whether the Limiter owns that
// handle as its state store too, which decides whether Close has one thing to
// shut or two.
func openStore(cfg Config) (StateStore, *store.Store, bool, error) {
	var sqlite *store.Store
	if cfg.DBPath != "" {
		s, err := store.OpenStore(cfg.DBPath)
		if err != nil {
			return nil, nil, false, fmt.Errorf("pace: open store: %w", err)
		}
		sqlite = s
	}
	switch {
	case cfg.Store != nil:
		return cfg.Store, sqlite, false, nil
	case sqlite != nil:
		return sqliteStateStore{s: sqlite}, sqlite, true, nil
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
		shards:        newShards(cfg.Shards),
		owner:         newOwnerID(),
		queueSlots:    make(chan struct{}, cfg.Queue.Workers),
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
		l.workerWg.Add(1)
		go l.pollQueue()
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

// acquire blocks until userID has a token or ctx is done. ctx must already be
// merged with the Limiter's lifetime via withLifetime. Callers are responsible
// for the activeWg registration around it.
func (l *Limiter) acquire(ctx context.Context, userID string) error {
	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	u := l.userFor(ctx, userID)
	u.lastUsed.Store(now.UnixNano())

	if q := (Quota{Rate: Limit(u.bucket.Limit()), Burst: u.bucket.Burst()}); l.sharedEnabled(q) {
		return l.acquireShared(ctx, userID, u, q)
	}

	if tokens := u.bucket.TokensAt(now); tokens < 1 {
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: userID,
			Delay:  u.bucket.DelayAt(now),
			Tokens: tokens,
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
		})
	}
	l.fireBeforeWait()
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
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
			// Measured at the point of failure, not at entry: this is the
			// number a caller reads to decide when to try again.
			Delay: u.bucket.DelayAt(l.cfg.Clock.Now()),
			Err:   err,
		}
	}
	return nil
}

// allow consumes a token for userID if one is immediately available.
func (l *Limiter) allow(userID string) bool {
	if !l.enter() {
		return false
	}
	defer l.leave()

	l.stats.requests.Add(1)
	now := l.cfg.Clock.Now()
	// Allow never blocks, so it gets its own bounded context rather than
	// inheriting one it was not given. It hangs off the Limiter's context so a
	// Close arriving mid-load cancels the store read instead of waiting out
	// StoreTimeout.
	ctx, cancel := context.WithTimeout(l.ctx, l.cfg.StoreTimeout)
	defer cancel()
	u := l.userFor(ctx, userID)
	u.lastUsed.Store(now.UnixNano())

	q := Quota{Rate: Limit(u.bucket.Limit()), Burst: u.bucket.Burst()}
	if l.sharedEnabled(q) {
		return l.allowShared(ctx, userID, u, q, now)
	}

	if !u.bucket.AllowAt(now) {
		l.observeThrottled(ctx, ThrottleInfo{
			UserID: userID,
			Delay:  u.bucket.DelayAt(now),
			Tokens: u.bucket.TokensAt(now),
			Limit:  Limit(u.bucket.Limit()),
			Burst:  u.bucket.Burst(),
		})
		return false
	}
	return true
}

// tokens reports the available tokens for userID, and whether the user has
// in-memory state at all.
func (l *Limiter) tokens(userID string) (float64, bool) {
	sh := l.shardFor(userID)
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if !ok {
		return 0, false
	}
	return u.bucket.TokensAt(l.cfg.Clock.Now()), true
}

// evictUser removes userID from memory, persisting the current token state
// first when a store is configured.
//
// The shard lock covers only the map surgery. Both of the things that follow —
// the store write and the observer callback — run outside it, for the reason
// the sweep already documents: neither may be executed with a shard held shut.
// An observer that calls back into the Limiter would otherwise deadlock against
// the very lock this function took, and a slow store would stall every user who
// hashes to this shard.
//
// Unlike the sweep the store write is still synchronous, because Evict's
// contract is that state is persisted by the time it returns. The observer
// fires after it, so a failed save is not reported as a clean eviction.
func (l *Limiter) evictUser(ctx context.Context, userID string) (bool, error) {
	if !l.enter() {
		return false, ErrClosed
	}
	defer l.leave()

	now := l.cfg.Clock.Now()
	sh := l.shardFor(userID)

	sh.mu.Lock()
	u, ok := sh.users[userID]
	if ok {
		delete(sh.users, userID)
		sh.live.Add(-1)
	}
	sh.mu.Unlock()

	if !ok {
		return false, nil
	}

	if l.persistsState() {
		sn := snap{userID: userID, tokens: u.bucket.TokensAt(now), lastUsed: u.lastUsed.Load()}
		saveCtx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
		defer cancel()
		if err := l.store.Save(saveCtx, userID, sn.state()); err != nil {
			return true, fmt.Errorf("pace: evict %q: %w", userID, err)
		}
	}
	l.observeEvicted(userID, EvictExplicit)
	return true, nil
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
		l.workerWg.Wait()
		l.replayWg.Wait()
		l.activeWg.Wait()
		// Persist before discarding: dropUsers empties the shards, so a flush
		// after it would find nothing to write.
		if l.persistsState() {
			l.saveAll()
		}
		// Drop whether or not there is a store: shutdown discards every user's
		// in-memory state either way, and an observer watching the population
		// should see it go rather than have it vanish silently.
		l.dropUsers()
		var cerr error
		if l.store != nil {
			cerr = l.store.Close()
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
		l.cfg.Queue.OnDeadLetter(DeadJob{
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

// dropUsers discards every user's in-memory state, counts the drop as
// evictions, and tells the observer about each one.
//
// It empties the shards rather than only reading them. A Stats call after Close
// must not report a population that no longer exists — the alternative is a
// snapshot that says "N users" and "+N evictions" at the same time, which
// cannot both be true.
//
// It runs after the final flush, so the state it discards has already been
// persisted. The observer fires outside the shard lock, as everywhere else.
func (l *Limiter) dropUsers() {
	notify := l.cfg.Observer != nil && l.cfg.Observer.UserEvicted != nil

	var dropped int64
	for i := range l.shards {
		sh := &l.shards[i]
		sh.mu.Lock()
		var ids []string
		if notify {
			ids = make([]string, 0, len(sh.users))
			for id := range sh.users {
				ids = append(ids, id)
			}
		}
		dropped += int64(len(sh.users))
		clear(sh.users)
		sh.live.Store(0)
		sh.mu.Unlock()

		// observeEvicted counts as it notifies.
		for _, id := range ids {
			l.observeEvicted(id, EvictShutdown)
		}
	}
	if !notify {
		// Nobody is listening, so the per-user list was never built; count the
		// drop in one go instead.
		l.stats.evictions.Add(max(0, dropped))
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

// withRequestTimeout bounds one HTTP round-trip, if RequestTimeout is set.
//
// It is applied after the rate-limit token is acquired. A request queued behind
// throttling has not started, and charging that wait against its timeout would
// make the timeout a function of how busy the user is rather than of how slow
// the server is.
func (l *Limiter) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if l.cfg.RequestTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, l.cfg.RequestTimeout)
}

// timed re-attaches a request to a deadline-bearing context, if there is one.
//
// http.Request.WithContext copies the whole request, so it is skipped entirely
// when no RequestTimeout is configured — which is the default, and the path
// most callers take.
func (l *Limiter) timed(ctx context.Context, req *http.Request) *http.Request {
	if l.cfg.RequestTimeout <= 0 {
		return req
	}
	return req.WithContext(ctx)
}
