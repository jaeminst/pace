package pace

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	shards     [numShards]*shard
	ctx        context.Context
	cancel     context.CancelFunc
	store      storer // nil when no persistence is configured
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
	return nil
}

// withDefaults returns a copy of cfg with every optional field resolved, so
// nothing downstream has to re-check for zero values.
func (cfg Config) withDefaults() Config {
	if cfg.Burst <= 0 {
		cfg.Burst = 1
	}
	if cfg.IdleExpiry <= 0 {
		cfg.IdleExpiry = 10 * time.Minute
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = time.Minute
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

// openStore returns the storer implied by cfg, or nil if no persistence is requested.
func openStore(cfg Config) (storer, error) {
	switch {
	case cfg.Store != nil:
		return &storeWrapper{s: cfg.Store}, nil
	case cfg.DBPath != "":
		s, err := store.OpenStore(cfg.DBPath)
		if err != nil {
			return nil, fmt.Errorf("pace: open store: %w", err)
		}
		return s, nil
	}
	return nil, nil
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
	st, err := openStore(cfg)
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
	}
	for i := range numShards {
		l.shards[i] = &shard{users: make(map[string]*user)}
	}
	l.gcWg.Add(1)
	go l.gcLoop()

	// Wire up durable queue when the SQLite store is active.
	if sq, ok := st.(*store.Store); ok {
		if err := sq.Setup(); err != nil {
			_ = l.close()
			return nil, fmt.Errorf("pace: init queue schema: %w", err)
		}
		l.sqliteStore = sq
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

	closeErr := l.close() // flush + close store, stop GC loop (idempotent via closeOnce)
	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}

// request acquires a rate-limit token for userID and returns a chainable
// [*Request] ready to execute. It blocks until a token is available,
// the caller's context expires, or the Limiter is closed/shut down.
func (l *Limiter) request(ctx context.Context, userID string) (*Request, error) {
	select {
	case <-l.ctx.Done():
		return nil, ErrClosed
	default:
	}
	// Reject requests during graceful shutdown; register as active otherwise.
	l.shutdownMu.RLock()
	if l.shuttingDown {
		l.shutdownMu.RUnlock()
		return nil, ErrClosed
	}
	l.activeWg.Add(1)
	l.shutdownMu.RUnlock()
	defer l.activeWg.Done()

	now := l.cfg.Clock.Now()
	u := l.userFor(userID)
	u.lastUsed.Store(now.UnixNano())
	if l.cfg.OnThrottle != nil && !u.bucket.HasTokenAt(now) {
		l.cfg.OnThrottle(userID)
	}
	if err := u.bucket.Wait(ctx, l.ctx); err != nil {
		// Ask the Limiter's own context whether it shut down, rather than
		// inferring it from the caller's context still being live. The
		// limiter reports "would exceed context deadline" without waiting,
		// so ctx.Err() is legitimately nil in that case too — treating that
		// as ErrClosed told callers the Limiter was closed when it was not.
		if l.ctx.Err() != nil {
			return nil, ErrClosed
		}
		return nil, &LimitError{
			UserID: userID,
			Limit:  l.cfg.Rate,
			Burst:  l.cfg.Burst,
			Err:    err,
		}
	}
	return newRequest(ctx, l.httpClient, l.cfg.BaseURL), nil
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

// close stops the GC goroutine and flushes in-memory state. It is idempotent;
// repeated calls return the error recorded by the first.
func (l *Limiter) close() error {
	// sync.Once establishes happens-before for closeErr, so later callers read
	// what the first writer stored.
	l.closeOnce.Do(func() {
		l.cancel()
		// Drain replay goroutines: they observe ErrClosed and exit promptly,
		// ensuring no concurrent DB access during saveAll/store.Close.
		l.replayWg.Wait()
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

// replay re-executes all jobs that were persisted but never completed.
func (l *Limiter) replay() {
	defer l.replayWg.Done()
	jobs, err := l.sqliteStore.Pending()
	if err != nil {
		l.cfg.Logger.Warn("pace: replay: load pending", "err", err)
		return
	}
	for _, j := range jobs {
		l.replayWg.Go(func() {
			req := newDurableRequest(context.Background(), l, j.UserID, j.ID)
			req.body = j.Body
			maps.Copy(req.headers, j.Headers)
			if _, err := req.do(j.Method, j.Path); err != nil {
				l.cfg.Logger.Warn("pace: replay: execute", "id", j.ID, "err", err)
			}
		})
	}
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
