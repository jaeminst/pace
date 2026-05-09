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
	numShards = 256
	shardMask = numShards - 1
)

var (
	ErrClosed          = errors.New("pace: manager closed")
	ErrUnknownEndpoint = errors.New("pace: unknown endpoint")
)

// Clock abstracts time for testing GC behavior.
type Clock interface {
	Now() time.Time
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }

type EndpointConfig struct {
	BaseURL       string
	RatePerMinute int
	Burst         int // 0 → 1
}

type Config struct {
	Endpoints  map[string]EndpointConfig
	IdleExpiry time.Duration    // 0 → 10m
	Transport  http.RoundTripper // nil → http.DefaultTransport
	Clock      Clock             // nil → real clock
	Logger     *slog.Logger      // nil → slog.Default()
	DBPath     string            // optional: SQLite file path for persistence
}

type Manager struct {
	shards     [numShards]*shard
	endpoints  map[string]*endpoint // immutable after New
	ctx        context.Context
	cancel     context.CancelFunc
	idleExpiry time.Duration
	clock      Clock
	logger     *slog.Logger
	store      *store // nil if DBPath not set
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
	buckets  map[string]*bucket // immutable after creation → no lock needed for reads
	lastUsed atomic.Int64       // unix nano
}

func New(cfg Config) (*Manager, error) {
	if len(cfg.Endpoints) == 0 {
		return nil, errors.New("pace: no endpoints configured")
	}
	if cfg.IdleExpiry <= 0 {
		cfg.IdleExpiry = 10 * time.Minute
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
		clock:      cfg.Clock,
		logger:     cfg.Logger,
	}
	for i := range numShards {
		m.shards[i] = &shard{users: make(map[string]*userBuckets)}
	}
	for name, ec := range cfg.Endpoints {
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

// Request throttles the call against userID's bucket for endpointName and
// returns a ready-to-use *Request. Blocks until a token is available.
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

// Get is a convenience wrapper that throttles and executes a GET.
func (m *Manager) Get(ctx context.Context, userID, endpointName, path string) (*Response, error) {
	req, err := m.Request(ctx, userID, endpointName)
	if err != nil {
		return nil, err
	}
	return req.Get(path)
}

// Close stops the GC loop and, if a store is configured, flushes all active
// user states to SQLite before closing the DB.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.cancel()
		if m.store != nil {
			m.saveAll()
			m.store.close()
		}
	})
}

func (m *Manager) shardFor(userID string) *shard {
	h := fnv.New32a()
	h.Write([]byte(userID))
	return m.shards[h.Sum32()&shardMask]
}

func (m *Manager) getOrCreateUser(userID string) *userBuckets {
	sh := m.shardFor(userID)
	// hot path: RLock only
	sh.mu.RLock()
	u, ok := sh.users[userID]
	sh.mu.RUnlock()
	if ok {
		return u
	}
	// cold path: create with double-check
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
	ticker := time.NewTicker(time.Minute)
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
