// Package pace provides per-user, per-endpoint outbound HTTP rate limiting.
//
// Each user gets an independent token bucket per endpoint, so one user's traffic
// never affects another's quota. A single background goroutine handles idle-user
// GC; the number of goroutines does not grow with the user count.
//
//	mgr, err := pace.New(pace.Config{
//	    Endpoints: map[string]pace.EndpointConfig{
//	        "api": {BaseURL: "https://api.example.com", RatePerMinute: 60},
//	    },
//	})
//	if err != nil { log.Fatal(err) }
//	defer mgr.Close()
//
//	resp, err := mgr.Get(ctx, "user-123", "api", "/items/42")
package pace

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	numShards = 256 // must be a power of two for the bitmask fast-path
	shardMask = numShards - 1
)

// ErrClosed is returned by Request and Get after the Manager has been closed.
var ErrClosed = errors.New("pace: manager closed")

// ErrUnknownEndpoint is returned when the endpoint name is not present in
// Config.Endpoints.
var ErrUnknownEndpoint = errors.New("pace: unknown endpoint")

// Clock abstracts wall-clock time. Implement it to control time in tests.
type Clock interface {
	Now() time.Time
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }

// EndpointConfig configures a single named endpoint.
type EndpointConfig struct {
	// BaseURL is the base URL prepended to every request path. Required.
	BaseURL string

	// RatePerMinute is the maximum number of requests per user per minute.
	// Must be greater than zero.
	RatePerMinute int

	// Burst is the maximum number of tokens that can accumulate when the
	// endpoint is idle. Zero or negative values default to 1.
	Burst int
}

// Config configures a [Manager].
type Config struct {
	// Endpoints maps endpoint names to their configurations. Required.
	Endpoints map[string]EndpointConfig

	// IdleExpiry is how long a user can be inactive before their in-memory
	// state is garbage-collected. Zero defaults to 10 minutes.
	IdleExpiry time.Duration

	// GCInterval controls how often the idle-user GC sweep runs.
	// Zero defaults to 1 minute.
	GCInterval time.Duration

	// Transport is the HTTP transport used for all requests. Nil defaults to
	// [http.DefaultTransport].
	Transport http.RoundTripper

	// Clock overrides wall-clock time. Nil uses the real system clock.
	// Useful for deterministic GC testing.
	Clock Clock

	// Logger receives internal warnings (e.g. store I/O errors during GC).
	// Nil defaults to [slog.Default].
	Logger *slog.Logger

	// DBPath is an optional path to a SQLite file used to persist per-user
	// token state across process restarts. Leave empty to disable persistence.
	DBPath string
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
	store      *store // nil when DBPath is unset
	closeOnce  sync.Once
}

type shard struct {
	mu    sync.RWMutex
	users map[string]*userBuckets
}

type endpoint struct {
	cfg    EndpointConfig
	client *http.Client
}

type userBuckets struct {
	buckets  map[string]*bucket // immutable after creation; no lock needed for reads
	lastUsed atomic.Int64       // unix nanoseconds; updated atomically
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
		s, err := openStore(cfg.DBPath)
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
	if err := u.buckets[endpointName].wait(ctx, m.ctx); err != nil {
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
			if err := m.store.close(); err != nil {
				m.logger.Warn("pace: close store", "err", err)
			}
		}
	})
}

func (m *Manager) shardFor(userID string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(userID))
	return m.shards[h.Sum32()&shardMask]
}

func (m *Manager) getOrCreateUser(userID string) *userBuckets {
	sh := m.shardFor(userID)
	// hot path: existing user needs only a read lock
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if ok {
		return u
	}
	// cold path: new user — double-check under write lock to avoid races
	sh.mu.Lock()
	if u, ok = sh.users[userID]; ok {
		sh.mu.Unlock()
		return u
	}
	u = m.createUserBuckets(userID)
	sh.users[userID] = u
	sh.mu.Unlock()
	return u
}

func (m *Manager) createUserBuckets(userID string) *userBuckets {
	u := &userBuckets{buckets: make(map[string]*bucket, len(m.endpoints))}
	var saved map[string]savedState
	if m.store != nil {
		if ss, err := m.store.load(userID); err == nil {
			saved = ss
		} else {
			m.logger.Warn("pace: load user state", "user", userID, "err", err)
		}
	}
	now := m.clock.Now()
	for name, ep := range m.endpoints {
		if ss, ok := saved[name]; ok {
			u.buckets[name] = restoreBucket(ep.cfg.RatePerMinute, ep.cfg.Burst, ss.tokens, time.Unix(0, ss.lastUsed))
			if ss.lastUsed > u.lastUsed.Load() {
				u.lastUsed.Store(ss.lastUsed)
			}
		} else {
			u.buckets[name] = newBucket(ep.cfg.RatePerMinute, ep.cfg.Burst)
		}
	}
	if u.lastUsed.Load() == 0 {
		u.lastUsed.Store(now.UnixNano())
	}
	return u
}

func (m *Manager) saveAll() {
	for _, sh := range m.shards {
		sh.mu.RLock()
		for id, u := range sh.users {
			if err := m.store.save(id, u.buckets, u.lastUsed.Load()); err != nil {
				m.logger.Warn("pace: save on close", "user", id, "err", err)
			}
		}
		sh.mu.RUnlock()
	}
}

func (m *Manager) gcLoop() {
	ticker := time.NewTicker(m.gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			m.collectIdle()
		case <-m.ctx.Done():
			return
		}
	}
}

func (m *Manager) collectIdle() {
	cutoff := m.clock.Now().Add(-m.idleExpiry).UnixNano()
	for _, sh := range m.shards {
		sh.mu.Lock()
		for id, u := range sh.users {
			if u.lastUsed.Load() < cutoff {
				if m.store != nil {
					if err := m.store.save(id, u.buckets, u.lastUsed.Load()); err != nil {
						m.logger.Warn("pace: gc save", "user", id, "err", err)
					}
				}
				delete(sh.users, id)
			}
		}
		sh.mu.Unlock()
	}
}
