package pace

import (
	"context"
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

// Client throttles outbound HTTP requests on a per-user basis toward a single
// endpoint. A single Client is safe for concurrent use by multiple goroutines.
// Create one with [New] and release resources with [Close] or [Shutdown].
type engine struct {
	cfg        Config
	httpClient *http.Client
	shards     [numShards]*shard
	ctx        context.Context
	cancel     context.CancelFunc
	idleExpiry time.Duration
	gcInterval time.Duration
	clock      Clock
	logger     *slog.Logger
	store      storer // nil when no persistence is configured
	onThrottle func(userID string)
	closeOnce  sync.Once
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

// New creates a Client from cfg. It starts a background GC goroutine and
// opens the configured store (SQLite or custom). Call [Client.Close] or
// [Client.Shutdown] when the Client is no longer needed.
//
// When [Config.Name] is set, Get/Post/etc. can be called directly on the
// returned Client. Otherwise use [Client.For](userID) to bind a user identity.
func New(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, errors.New("pace: Config.BaseURL is required")
	}
	if cfg.RatePerMinute <= 0 {
		return nil, errors.New("pace: Config.RatePerMinute must be > 0")
	}
	if cfg.Store != nil && cfg.DBPath != "" {
		return nil, errors.New("pace: Config.Store and Config.DBPath are mutually exclusive")
	}
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
	transport := cfg.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}

	ctx, cancel := context.WithCancel(context.Background())
	st, err := openStore(cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	e := &engine{
		cfg:        cfg,
		httpClient: &http.Client{Transport: transport},
		ctx:        ctx,
		cancel:     cancel,
		idleExpiry: cfg.IdleExpiry,
		gcInterval: cfg.GCInterval,
		clock:      cfg.Clock,
		logger:     cfg.Logger,
		onThrottle: cfg.OnThrottle,
		store:      st,
		inflight:   make(map[string]*future),
	}
	for i := range numShards {
		e.shards[i] = &shard{users: make(map[string]*user)}
	}
	e.gcWg.Add(1)
	go e.gcLoop()

	// Wire up durable queue when the SQLite store is active.
	if sq, ok := st.(*store.Store); ok {
		if err := sq.Setup(); err != nil {
			e.close()
			return nil, fmt.Errorf("pace: init queue schema: %w", err)
		}
		e.sqliteStore = sq
		e.replayWg.Add(1)
		go e.replay()
	}

	return &Client{userID: cfg.Name, eng: e}, nil
}

// request acquires a rate-limit token for userID and returns a chainable
// [*Request] ready to execute. It blocks until a token is available,
// the caller's context expires, or the Client is closed/shut down.
func (c *engine) request(ctx context.Context, userID string) (*Request, error) {
	select {
	case <-c.ctx.Done():
		return nil, ErrClosed
	default:
	}
	// Reject requests during graceful shutdown; register as active otherwise.
	c.shutdownMu.RLock()
	if c.shuttingDown {
		c.shutdownMu.RUnlock()
		return nil, ErrClosed
	}
	c.activeWg.Add(1)
	c.shutdownMu.RUnlock()
	defer c.activeWg.Done()

	u := c.userFor(userID)
	u.lastUsed.Store(c.clock.Now().UnixNano())
	if c.onThrottle != nil && !u.bucket.HasToken() {
		c.onThrottle(userID)
	}
	if err := u.bucket.Wait(ctx, c.ctx); err != nil {
		if ctx.Err() == nil {
			return nil, ErrClosed
		}
		return nil, err
	}
	return newRequest(ctx, c.httpClient, c.cfg.BaseURL), nil
}

// Tokens returns the approximate number of available tokens for userID.
// Returns -1 if the user has no in-memory state (not yet seen, or already GC'd).
func (c *engine) tokens(userID string) float64 {
	sh := c.shardFor(userID)
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if !ok {
		return -1
	}
	return u.bucket.Tokens()
}

// Evict removes userID from the in-memory shard immediately. If a store is
// configured, the current token state is saved before removal. Returns false
// if the user had no in-memory state.
func (c *engine) evictUser(userID string) bool {
	sh := c.shardFor(userID)
	sh.mu.Lock()
	u, ok := sh.users[userID]
	if ok {
		c.evict(sh, userID, u)
	}
	sh.mu.Unlock()
	return ok
}

// Shutdown stops the Client gracefully. It prevents new requests and waits
// until ctx expires (or all in-flight requests finish) before cleaning up.
// If ctx expires first, remaining waiters are force-cancelled and
// Shutdown returns ctx.Err(). The store is always flushed and closed on
// return. Shutdown is idempotent via the underlying Close call.
func (c *engine) shutdown(ctx context.Context) error {
	// Stop accepting new requests.
	c.shutdownMu.Lock()
	c.shuttingDown = true
	c.shutdownMu.Unlock()

	// Wait for active requests to finish, honouring the caller's deadline.
	waitDone := make(chan struct{})
	go func() {
		c.activeWg.Wait()
		close(waitDone)
	}()

	var shutdownErr error
	select {
	case <-waitDone:
	case <-ctx.Done():
		shutdownErr = ctx.Err()
		c.cancel() // force-cancel remaining waiters
		<-waitDone
	}

	c.close() // flush + close store, stop GC loop (idempotent via closeOnce)
	return shutdownErr
}

// Close shuts down the background GC goroutine and flushes all in-memory
// user states to the configured store. Close is idempotent.
func (c *engine) close() {
	c.closeOnce.Do(func() {
		c.cancel()
		// Drain replay goroutines: they observe ErrClosed and exit promptly,
		// ensuring no concurrent DB access during saveAll/store.Close.
		c.replayWg.Wait()
		if c.store != nil {
			c.saveAll()
			if err := c.store.Close(); err != nil {
				c.logger.Warn("pace: close store", "err", err)
			}
		}
	})
}

// replay re-executes all jobs that were persisted but never completed.
func (c *engine) replay() {
	defer c.replayWg.Done()
	jobs, err := c.sqliteStore.Pending()
	if err != nil {
		c.logger.Warn("pace: replay: load pending", "err", err)
		return
	}
	for _, j := range jobs {
		j := j
		c.replayWg.Add(1)
		go func() {
			defer c.replayWg.Done()
			req := newDurableRequest(context.Background(), c, j.UserID, j.ID)
			req.body = j.Body
			for k, v := range j.Headers {
				req.headers[k] = v
			}
			if _, err := req.do(j.Method, j.Path); err != nil {
				c.logger.Warn("pace: replay: execute", "id", j.ID, "err", err)
			}
		}()
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
