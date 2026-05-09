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

// storer is the persistence interface used by Manager. *store.Store satisfies
// this interface; tests can inject a mock via SetManagerStore (export_test.go).
type storer interface {
	Save(userID string, tokens map[string]float64, lastUsed int64) error
	Load(userID string) (map[string]store.SavedState, error)
	Close() error
}

// Manager throttles outbound HTTP requests on a per-user, per-endpoint basis.
// A single Manager is safe for concurrent use by multiple goroutines.
// Create one with [New] and release resources with [Close].
type Manager struct {
	shards     [numShards]*shard
	endpoints  map[string]*endpoint // immutable after New
	ctx        context.Context
	cancel     context.CancelFunc
	idleExpiry time.Duration
	gcInterval time.Duration
	clock      Clock
	logger     *slog.Logger
	store      storer // nil when DBPath is unset
	onThrottle func(userID, endpointName string)
	closeOnce  sync.Once
	gcWg       sync.WaitGroup
	// _testHookGetOrCreate is called in getOrCreateUser cold path before the write
	// lock; nil in production. It exists only to enable deterministic double-check
	// tests without sleeping.
	_testHookGetOrCreate func()
}

// New creates a Manager from cfg. It starts a background GC goroutine and
// opens the SQLite store if cfg.DBPath is set. Call [Manager.Close] when the
// Manager is no longer needed.
func New(cfg Config) (*Manager, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("pace: no endpoints configured")
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
	m := &Manager{
		endpoints:  make(map[string]*endpoint, len(cfg.Endpoints)),
		ctx:        ctx,
		cancel:     cancel,
		idleExpiry: cfg.IdleExpiry,
		gcInterval: cfg.GCInterval,
		clock:      cfg.Clock,
		logger:     cfg.Logger,
		onThrottle: cfg.OnThrottle,
	}
	for i := range numShards {
		m.shards[i] = &shard{users: make(map[string]*userBuckets)}
	}
	for name, ec := range cfg.Endpoints {
		if ec.BaseURL == "" {
			cancel()
			return nil, fmt.Errorf("pace: endpoint %q: BaseURL is required", name)
		}
		if ec.RatePerMinute <= 0 {
			cancel()
			return nil, fmt.Errorf("pace: endpoint %q: RatePerMinute must be > 0", name)
		}
		if ec.Burst <= 0 {
			ec.Burst = 1
		}
		m.endpoints[name] = &endpoint{
			cfg:    ec,
			client: &http.Client{Transport: transport},
		}
	}
	if cfg.DBPath != "" {
		s, err := store.OpenStore(cfg.DBPath)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("pace: open store: %w", err)
		}
		m.store = s
	}
	m.gcWg.Add(1)
	go m.gcLoop()
	return m, nil
}

// Request acquires a rate-limit token for userID on endpointName and returns a
// chainable [*Request] ready to execute. It blocks until a token is available,
// the caller's context expires, or the Manager is closed.
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
	u := m.getOrCreateUser(userID)
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

// Evict removes userID from the in-memory shard immediately. If DBPath is
// configured, the current token state is saved before removal. Returns false
// if the user had no in-memory state.
func (m *Manager) Evict(userID string) bool {
	sh := m.shardFor(userID)
	sh.mu.Lock()
	u, ok := sh.users[userID]
	if ok {
		m.evictUser(sh, userID, u)
	}
	sh.mu.Unlock()
	return ok
}

// Close shuts down the background GC goroutine. If a DBPath was configured,
// it flushes all in-memory user states to SQLite before closing the database.
// Close is idempotent and safe to call more than once.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.cancel()
		if m.store != nil {
			m.saveAll()
			if err := m.store.Close(); err != nil {
				m.logger.Warn("pace: close store", "err", err)
			}
		}
	})
}
