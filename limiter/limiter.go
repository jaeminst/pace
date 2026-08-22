package limiter

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/config"

	"github.com/jaeminst/pace/gate"
	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/store"
)

// Limiter paces work on a per-user basis. It owns every resource involved: the
// user population, the idle-user GC goroutine, the shared-quota gate and the
// state store.
//
// Every method takes the user ID it applies to, because a Limiter is the whole
// population rather than one member of it — api.go is the list. Binding an
// identity once and speaking HTTP through it is
// github.com/jaeminst/pace/client.Client, which is what a caller normally
// holds.
//
// Create one with [New], which takes the same
// [github.com/jaeminst/pace/config.Config] a caller writes — or, more usually,
// let github.com/jaeminst/pace/client.New do it. Release resources with
// [Limiter.Close] or [Limiter.Shutdown]. A Limiter is safe for concurrent use
// by multiple goroutines.
type Limiter struct {
	cfg    config.Config // resolved by the front door, and immutable after New
	ctx    context.Context
	cancel context.CancelFunc
	// quota is the default this Limiter is currently applying — where cfg.Rate
	// and cfg.Burst started, and whatever [Limiter.SetDefaultQuota] has made of
	// it since. It is the reason cfg is not the whole story.
	//
	// A pointer to an immutable pair rather than two atomic numbers: the rate
	// and the burst have to be read together or a reader gets a combination
	// nobody set, which is the same argument registry makes for reading a
	// user's tokens and last-used stamp as one snapshot.
	//
	// Always non-nil — New seeds it before anything can read it. That is the
	// opposite invariant to hooks below, which is deliberately nil in
	// production; the two fields are not the same shape of thing.
	quota atomic.Pointer[bucket.Quota]
	// reg owns the user population: the sharded map, each user's bucket,
	// their persistence and their eviction. newRegistry below is the wiring.
	reg   *registry.Registry
	store store.Store // nil when no persistence is configured
	// state is the persistence policy over that store, and what the registry
	// actually calls. It is rebuilt, never mutated, when store changes.
	state        *persistence
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

// New builds an engine from an already-resolved
// [github.com/jaeminst/pace/config.Config] and starts its GC goroutine. Call
// [Limiter.Close] or [Limiter.Shutdown] when it is no longer needed.
//
// It panics on a Config it cannot work with rather than returning an error.
// [github.com/jaeminst/pace/config.Config.Resolve] is what returns an error, and
// it cannot produce a Config this rejects — so anything wrong at this point came
// from a struct filled in by hand, which is a wiring bug. See validate.go for
// the six fields it reads.
func New(cfg config.Config) *Limiter {
	validate(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	l := &Limiter{
		cfg:    cfg,
		ctx:    ctx,
		cancel: cancel,
		store:  cfg.Store,
	}
	// Before newRegistry below, whose QuotaFor closure loads it, and before the
	// GC goroutine exists at all.
	l.quota.Store(&bucket.Quota{Rate: cfg.Rate, Burst: cfg.Burst})

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
// what lets a persistence hold no state of its own; l.store stays the one
// place the store lives, because Close reads it too.
func (l *Limiter) newState() *persistence {
	return &persistence{
		store:    l.store,
		shadowed: l.cfg.Shared.Backend != nil,
		timeout:  l.cfg.StoreTimeout,
		logger:   l.cfg.Logger,
	}
}

// newRegistry wires the user population to this Limiter.
//
// Everything the registry needs arrives as a value or a function, so it never
// imports this package. The split is not arbitrary: the registry decides which
// users exist and when they are evicted, and holds the shard locks while doing
// it; everything below decides what persisting or reporting one *means*, which
// is where persistence, observe.Observer and the quota vocabulary live.
func (l *Limiter) newRegistry() *registry.Registry {
	return registry.New(registry.Spec{
		Shards:     l.cfg.Shards,
		IdleExpiry: l.cfg.IdleExpiry,
		Now:        l.cfg.Clock.Now,
		QuotaFor: func(userID string) (float64, int) {
			q := l.quotaFor(userID)
			return float64(q.Rate), q.Burst
		},
		// Method values on the adapter, so a store swapped in after
		// construction is honoured: newState rebuilds it and the registry keeps
		// calling through l.state.
		Persists: func() bool { return l.state.persists() },
		Load: func(ctx context.Context, userID string) (registry.Snapshot, bool) {
			return l.state.load(ctx, userID)
		},
		Save: func(ctx context.Context, s registry.Snapshot) error {
			return l.state.save(ctx, s)
		},
		Flush:    func(snaps []registry.Snapshot) { l.state.flush(snaps) },
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
	if l.cfg.Shared.Backend == nil {
		return nil
	}
	return gate.New(l.ctx, gate.Spec{
		Backend:   l.cfg.Shared.Backend,
		Namespace: l.cfg.Shared.Namespace,
		Timeout:   l.cfg.Shared.Timeout,
		OnError:   l.cfg.Shared.OnError,
		Logger:    l.cfg.Logger,
		Now:       l.cfg.Clock.Now,
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
// gate because [github.com/jaeminst/pace/bucket.Inf] belongs to the vocabulary a
// caller writes, and gate would have had to compare against a bare
// math.MaxFloat64 to make the same decision.
func (l *Limiter) sharedEnabled(q bucket.Quota) bool {
	return l.gate != nil && q.Rate != bucket.Inf
}

// ReloadQuotas re-resolves every user currently holding in-memory state and
// applies the result to their live bucket, keeping the tokens they have already
// accrued. Call it after changing whatever
// [github.com/jaeminst/pace/config.Config.QuotaFor] reads, or after
// [Limiter.SetDefaultQuota]; it picks up both.
//
// Users not in memory need nothing: their bucket is resolved fresh the next time
// they appear. Before this existed, changing a quota meant building a new
// Limiter, which dropped every bucket in the process. For one user,
// [Limiter.ReloadQuota] does the same in O(1).
//
// It walks every shard, so it is a maintenance operation rather than something
// to call per request. Each shard is copied under its own read lock and released
// before the quota is resolved, so a slow QuotaFor never blocks a request — at
// the cost that the reload is a series of per-shard snapshots rather than one
// instant across the whole Limiter. The default is read per user for the same
// reason the clock is: the walk is not an instant, and pretending otherwise
// would mean new buckets created during it disagreeing with reloaded ones.
//
// A caller already blocked in Wait finishes at the pace it started with. The
// bucket arms one timer when the wait begins and does not re-arm it, so a raise
// does not shorten a wait already under way — it applies to the next
// acquisition.
func (l *Limiter) ReloadQuotas() { l.reg.Reload() }
