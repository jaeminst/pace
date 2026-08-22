package limiter

import (
	"context"
	"time"

	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/persist"
	"github.com/jaeminst/pace/registry"
)

// newState builds the persistence half of the registry.
//
// It is rebuilt rather than mutated when the backing store changes, which is
// what lets [persist.Adapter] hold no state of its own; l.store stays the one
// place the store lives, because Close reads it too.
func (l *Limiter) newState() *persist.Adapter {
	return persist.New(persist.Config{
		Store:    l.store,
		Shadowed: l.cfg.Shared.Quota != nil,
		Timeout:  l.cfg.StoreTimeout,
		Logger:   l.cfg.Logger,
	})
}

// newRegistry wires the user population to this Limiter.
//
// Everything the registry needs arrives as a value or a function, so it never
// imports this package. The split is not arbitrary: the registry decides which
// users exist and when they are evicted, and holds the shard locks while doing
// it; everything below decides what persisting or reporting one *means*, which
// is where [persist.Adapter], [Observer] and [Quota] live.
func (l *Limiter) newRegistry() *registry.Registry {
	return registry.New(registry.Config{
		Shards:     l.cfg.Shards,
		IdleExpiry: l.cfg.IdleExpiry,
		Now:        l.cfg.Now,
		QuotaFor: func(userID string) (float64, int) {
			q := l.cfg.Quota(userID)
			return float64(q.Rate), q.Burst
		},
		// Method values on the adapter, so a store swapped in after
		// construction is honoured: newState rebuilds it and the registry keeps
		// calling through l.state.
		Persists: func() bool { return l.state.Persists() },
		Load: func(ctx context.Context, userID string) (registry.Snapshot, bool) {
			return l.state.Load(ctx, userID)
		},
		Save: func(ctx context.Context, s registry.Snapshot) error {
			return l.state.Save(ctx, s)
		},
		Flush:    func(snaps []registry.Snapshot) { l.state.Flush(snaps) },
		Observes: l.observesEvictions,
		OnEvict:  l.onEvict,
		// Method values, not the hooks themselves: New starts the GC goroutine
		// before a test can install one.
		OnGetOrCreate: l.fireGetOrCreate,
		AfterSweep:    l.fireAfterSweep,
	})
}

// quotaOf reports what this user's bucket is currently enforcing.
//
// The bucket is the source of truth, not Config: Config.QuotaFor may have
// given this user their own, and [Limiter.ReloadQuotas] may have changed it
// since. Every report — LimitError, ThrottleInfo, Client.Quota, and the
// TakeRequest handed to a shared backend — reads it from here.
func quotaOf(u *registry.User) Quota {
	return Quota{Rate: Limit(u.Bucket().Limit()), Burst: u.Bucket().Burst()}
}

// onEvict translates one eviction into the public report. The registry counts
// them; this only tells anybody who asked to hear about it.
func (l *Limiter) onEvict(e registry.Eviction) {
	if l.cfg.Observer == nil || l.cfg.Observer.UserEvicted == nil {
		return
	}
	// The Limiter's own context: cancelled at Close, so a hook doing bounded
	// work can bail instead of holding up shutdown.
	l.cfg.Observer.UserEvicted(l.ctx, observe.EvictInfo{
		UserID:   e.UserID,
		Reason:   evictReasonOf(e.Reason),
		Tokens:   e.Tokens,
		LastUsed: e.LastUsed,
	})
}

func evictReasonOf(r registry.Reason) observe.EvictReason {
	switch r {
	case registry.Explicit:
		return observe.EvictExplicit
	case registry.Shutdown:
		return observe.EvictShutdown
	default: // registry.Idle
		return observe.EvictIdle
	}
}

// evictUser removes userID from memory, behind the same shutdown barrier as
// every other entry point that touches the store.
func (l *Limiter) evictUser(ctx context.Context, userID string) (bool, error) {
	if !l.enter() {
		return false, ErrClosed
	}
	defer l.leave()
	return l.reg.Evict(ctx, userID)
}

// gcLoop drives the idle-user sweep.
func (l *Limiter) gcLoop() {
	defer l.gcWg.Done()
	ticker := time.NewTicker(l.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.reg.Sweep()
		case <-l.ctx.Done():
			return
		}
	}
}
