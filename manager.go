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

// storer is the internal persistence interface. *store.Store (SQLite) and
// storeWrapper (wrapping a public StateStore) both satisfy it.
type storer interface {
	Save(userID string, tokens map[string]float64, lastUsed int64) error
	Load(userID string) (map[string]store.SavedState, error)
	Close() error
}

// storeWrapper adapts a public StateStore to the internal storer interface.
type storeWrapper struct{ s StateStore }

func (w *storeWrapper) Save(userID string, tokens map[string]float64, lastUsed int64) error {
	states := make(map[string]SavedState, len(tokens))
	for ep, t := range tokens {
		states[ep] = SavedState{Tokens: t, LastUsed: lastUsed}
	}
	return w.s.Save(userID, states)
}

func (w *storeWrapper) Load(userID string) (map[string]store.SavedState, error) {
	ss, err := w.s.Load(userID)
	if err != nil {
		return nil, err
	}
	result := make(map[string]store.SavedState, len(ss))
	for ep, s := range ss {
		result[ep] = store.SavedState{Tokens: s.Tokens, LastUsed: s.LastUsed}
	}
	return result, nil
}

func (w *storeWrapper) Close() error { return w.s.Close() }

// Manager throttles outbound HTTP requests on a per-user, per-endpoint basis.
// A single Manager is safe for concurrent use by multiple goroutines.
// Create one with [New] and release resources with [Close] or [Shutdown].
type Manager struct {
	shards     [numShards]*shard
	endpoints  map[string]*ep // immutable after New
	ctx        context.Context
	cancel     context.CancelFunc
	idleExpiry time.Duration
	gcInterval time.Duration
	clock      Clock
	logger     *slog.Logger
	store      storer // nil when no persistence is configured
	onThrottle func(userID, endpointName string)
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

// fillDefaults fills in zero-value Config fields with their defaults
// and returns the effective http.RoundTripper.
func fillDefaults(cfg *Config) http.RoundTripper {
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
		return http.DefaultTransport
	}
	return cfg.Transport
}

// makeEndpoints validates and constructs the endpoint map.
func makeEndpoints(eps map[string]Endpoint, transport http.RoundTripper) (map[string]*ep, error) {
	out := make(map[string]*ep, len(eps))
	for name, ec := range eps {
		if ec.BaseURL == "" {
			return nil, fmt.Errorf("pace: endpoint %q: BaseURL is required", name)
		}
		if ec.RatePerMinute <= 0 {
			return nil, fmt.Errorf("pace: endpoint %q: RatePerMinute must be > 0", name)
		}
		if ec.Burst <= 0 {
			ec.Burst = 1
		}
		out[name] = &ep{cfg: ec, client: &http.Client{Transport: transport}}
	}
	return out, nil
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

// New creates a Manager from cfg. It starts a background GC goroutine and
// opens the configured store (SQLite or custom). Call [Manager.Close] or
// [Manager.Shutdown] when the Manager is no longer needed.
func New(cfg Config) (*Manager, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("pace: no endpoints configured")
	}
	if cfg.Store != nil && cfg.DBPath != "" {
		return nil, errors.New("pace: Config.Store and Config.DBPath are mutually exclusive")
	}
	transport := fillDefaults(&cfg)

	endpoints, err := makeEndpoints(cfg.Endpoints, transport)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	st, err := openStore(cfg)
	if err != nil {
		cancel()
		return nil, err
	}

	m := &Manager{
		endpoints:  endpoints,
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
		m.shards[i] = &shard{users: make(map[string]*user)}
	}
	m.gcWg.Add(1)
	go m.gcLoop()

	// Wire up durable queue when the SQLite store is active.
	if sq, ok := st.(*store.Store); ok {
		if err := sq.Setup(); err != nil {
			m.Close()
			return nil, fmt.Errorf("pace: init queue schema: %w", err)
		}
		m.sqliteStore = sq
		m.replayWg.Add(1)
		go m.replay()
	}

	return m, nil
}

// Request acquires a rate-limit token for userID on endpointName and returns a
// chainable [*Request] ready to execute. It blocks until a token is available,
// the caller's context expires, or the Manager is closed/shut down.
func (m *Manager) Request(ctx context.Context, userID, endpointName string) (*Request, error) {
	ep, ok := m.endpoints[endpointName]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownEndpoint, endpointName)
	}
	select {
	case <-m.ctx.Done():
		return nil, ErrClosed
	default:
	}
	// Reject requests during graceful shutdown; register as active otherwise.
	m.shutdownMu.RLock()
	if m.shuttingDown {
		m.shutdownMu.RUnlock()
		return nil, ErrClosed
	}
	m.activeWg.Add(1)
	m.shutdownMu.RUnlock()
	defer m.activeWg.Done()

	u := m.userFor(userID)
	u.lastUsed.Store(m.clock.Now().UnixNano())
	if m.onThrottle != nil && !u.buckets[endpointName].HasToken() {
		m.onThrottle(userID, endpointName)
	}
	if err := u.buckets[endpointName].Wait(ctx, m.ctx); err != nil {
		if ctx.Err() == nil {
			return nil, ErrClosed
		}
		return nil, err
	}
	return newRequest(ctx, ep.client, ep.cfg.BaseURL), nil
}

// Get is a convenience wrapper around [Manager.Request] that also executes an
// HTTP GET to path.
func (m *Manager) Get(ctx context.Context, userID, endpointName, path string) (*Response, error) {
	req, err := m.Request(ctx, userID, endpointName)
	if err != nil {
		return nil, err
	}
	return req.Get(path)
}

// Durable returns a chainable [*Request] that executes with exactly-once
// semantics, identified by id. The first call persists the job to SQLite,
// acquires a rate-limit token, executes the HTTP request, and caches the
// result. Subsequent calls with the same id return the cached result without
// a new request. Concurrent calls with the same id share one in-flight
// execution.
//
// If the process exits before the request completes, the new Manager instance
// automatically replays the job. Requires [Config.DBPath]; the first call to
// Get/Post/etc. returns [ErrNoPersistence] otherwise.
//
// Example:
//
//	resp, err := mgr.Durable(ctx, chargeID, "alice", "payments").
//	    SetHeader("Idempotency-Key", chargeID).
//	    Post("/v1/charge")
func (m *Manager) Durable(ctx context.Context, id, userID, endpointName string) *Request {
	if m.sqliteStore == nil {
		return &Request{durableErr: ErrNoPersistence}
	}
	e, ok := m.endpoints[endpointName]
	if !ok {
		return &Request{durableErr: fmt.Errorf("%w: %s", ErrUnknownEndpoint, endpointName)}
	}
	return newDurableRequest(ctx, m, userID, endpointName, id, e)
}

// Tokens returns the approximate number of available tokens for userID on
// endpointName. Returns -1 if the user has no in-memory state (not yet seen,
// or already GC'd). Returns [ErrUnknownEndpoint] if endpointName is not
// configured.
func (m *Manager) Tokens(userID, endpointName string) (float64, error) {
	if _, ok := m.endpoints[endpointName]; !ok {
		return 0, fmt.Errorf("%w: %s", ErrUnknownEndpoint, endpointName)
	}
	sh := m.shardFor(userID)
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if !ok {
		return -1, nil
	}
	return u.buckets[endpointName].Tokens(), nil
}

// Evict removes userID from the in-memory shard immediately. If a store is
// configured, the current token state is saved before removal. Returns false
// if the user had no in-memory state.
func (m *Manager) Evict(userID string) bool {
	sh := m.shardFor(userID)
	sh.mu.Lock()
	u, ok := sh.users[userID]
	if ok {
		m.evict(sh, userID, u)
	}
	sh.mu.Unlock()
	return ok
}

// Shutdown stops the Manager gracefully. It prevents new requests and waits
// until ctx expires (or all in-flight requests finish) before cleaning up.
// If ctx expires first, remaining waiters are force-cancelled and
// Shutdown returns ctx.Err(). The store is always flushed and closed on
// return. Shutdown is idempotent via the underlying Close call.
func (m *Manager) Shutdown(ctx context.Context) error {
	// Stop accepting new requests.
	m.shutdownMu.Lock()
	m.shuttingDown = true
	m.shutdownMu.Unlock()

	// Wait for active requests to finish, honouring the caller's deadline.
	waitDone := make(chan struct{})
	go func() {
		m.activeWg.Wait()
		close(waitDone)
	}()

	var shutdownErr error
	select {
	case <-waitDone:
	case <-ctx.Done():
		shutdownErr = ctx.Err()
		m.cancel() // force-cancel remaining waiters
		<-waitDone
	}

	m.Close() // flush + close store, stop GC loop (idempotent via closeOnce)
	return shutdownErr
}

// Close shuts down the background GC goroutine and flushes all in-memory
// user states to the configured store. Close is idempotent.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.cancel()
		// Drain replay goroutines: they observe ErrClosed and exit promptly,
		// ensuring no concurrent DB access during saveAll/store.Close.
		m.replayWg.Wait()
		if m.store != nil {
			m.saveAll()
			if err := m.store.Close(); err != nil {
				m.logger.Warn("pace: close store", "err", err)
			}
		}
	})
}

// replay re-executes all jobs that were persisted but never completed.
func (m *Manager) replay() {
	defer m.replayWg.Done()
	jobs, err := m.sqliteStore.Pending()
	if err != nil {
		m.logger.Warn("pace: replay: load pending", "err", err)
		return
	}
	for _, j := range jobs {
		j := j
		m.replayWg.Add(1)
		go func() {
			defer m.replayWg.Done()
			e, ok := m.endpoints[j.Endpoint]
			if !ok {
				m.logger.Warn("pace: replay: unknown endpoint", "id", j.ID, "endpoint", j.Endpoint)
				return
			}
			req := newDurableRequest(context.Background(), m, j.UserID, j.Endpoint, j.ID, e)
			req.body = j.Body
			for k, v := range j.Headers {
				req.headers[k] = v
			}
			if _, err := req.do(j.Method, j.Path); err != nil {
				m.logger.Warn("pace: replay: execute", "id", j.ID, "err", err)
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
