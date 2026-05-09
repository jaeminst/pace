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
	store      *store.Store // nil when DBPath is unset
	closeOnce  sync.Once
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
