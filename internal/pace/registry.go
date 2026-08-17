package pace

import (
	"context"
	"fmt"
	"time"

	"github.com/jaeminst/pace/internal/registry"
)

// newRegistry wires the user population to this Limiter.
//
// Everything the registry needs arrives as a value or a function, so it never
// imports this package. The split is not arbitrary: the registry decides which
// users exist and when they are evicted, and holds the shard locks while doing
// it; everything below decides what persisting or reporting one *means*, which
// is where [StateStore], [Observer] and [Quota] live. That is why the
// BatchStateStore assertion is here rather than there — it is a question about
// the caller's store, asked at the moment of the write.
func (l *Limiter) newRegistry() *registry.Registry {
	return registry.New(registry.Config{
		Shards:     l.cfg.Shards,
		IdleExpiry: l.cfg.IdleExpiry,
		Now:        l.cfg.Clock.Now,
		QuotaFor: func(userID string) (float64, int) {
			q := l.quotaFor(userID)
			return float64(q.Rate), q.Burst
		},
		Persists: l.persistsState,
		Load:     l.loadState,
		Save:     l.saveOne,
		Flush:    l.flush,
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
// The bucket is the source of truth, not Config: [Config.QuotaFor] may have
// given this user their own, and [Limiter.ReloadQuotas] may have changed it
// since. Every report — LimitError, ThrottleInfo, Client.Quota, and the
// TakeRequest handed to a shared backend — reads it from here.
func quotaOf(u *registry.User) Quota {
	return Quota{Rate: Limit(u.Bucket().Limit()), Burst: u.Bucket().Burst()}
}

// persistsState reports whether per-user token state should be written to and
// read from [Config.Store].
//
// A shared quota turns the local bucket into a shadow, and a shadow must never
// be persisted. The bucket no longer describes what this user has spent — it
// describes what this replica has spent, which is a fraction of it. Restoring
// replica A's snapshot into replica B would have B throttling itself for
// traffic it never sent, and the inequality that makes the shadow safe
// (shadowTokens >= sharedTokens) is exactly what that breaks.
//
// The authoritative count lives in the backend, which is the point of
// configuring one.
func (l *Limiter) persistsState() bool {
	return l.store != nil && l.cfg.Shared.Quota == nil
}

// loadState reads a user's persisted state, if any. A store error is logged and
// treated as "no saved state": a fresh bucket is the safe fallback, and failing
// the request because persistence is unavailable would be worse than briefly
// granting a full burst.
func (l *Limiter) loadState(ctx context.Context, userID string) (registry.Snapshot, bool) {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()
	st, found, err := l.store.Load(ctx, userID)
	if err != nil {
		l.cfg.Logger.Warn("pace: load user state", "user", userID, "err", err)
		return registry.Snapshot{}, false
	}
	return registry.Snapshot{UserID: userID, Tokens: st.Tokens, LastUsed: st.LastUsed}, found
}

// saveOne persists one user and reports whether it worked. It backs
// [Client.Evict], whose contract is that state is written by the time it
// returns, so unlike flush it neither swallows the error nor detaches the
// context.
func (l *Limiter) saveOne(ctx context.Context, s registry.Snapshot) error {
	ctx, cancel := context.WithTimeout(ctx, l.cfg.StoreTimeout)
	defer cancel()
	if err := l.store.Save(ctx, s.UserID, State{Tokens: s.Tokens, LastUsed: s.LastUsed}); err != nil {
		return fmt.Errorf("pace: evict %q: %w", s.UserID, err)
	}
	return nil
}

// flush persists snapshots with no lock held. Stores that can write a batch in
// one transaction do; the rest fall back to one call per user, still outside
// every lock.
//
// The batch capability is discovered per call rather than resolved once, so a
// store swapped in after construction is honoured.
//
// It runs on context.Background rather than the Limiter's context: the final
// flush happens after the Limiter has been cancelled, and inheriting a
// cancelled context would discard exactly the state Close exists to save.
// StoreTimeout is what bounds it instead.
func (l *Limiter) flush(snaps []registry.Snapshot) {
	if !l.persistsState() || len(snaps) == 0 {
		return
	}
	if bs, ok := l.store.(BatchStateStore); ok {
		const chunk = 512 // bound each round-trip so one sweep cannot monopolise the store
		batch := make([]UserState, 0, min(chunk, len(snaps)))
		for start := 0; start < len(snaps); start += chunk {
			batch = batch[:0]
			for _, sn := range snaps[start:min(start+chunk, len(snaps))] {
				batch = append(batch, UserState{
					UserID: sn.UserID,
					State:  State{Tokens: sn.Tokens, LastUsed: sn.LastUsed},
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
			err := bs.SaveBatch(ctx, batch)
			cancel()
			if err != nil {
				l.cfg.Logger.Warn("pace: flush state", "count", len(batch), "err", err)
			}
		}
		return
	}
	for _, sn := range snaps {
		ctx, cancel := context.WithTimeout(context.Background(), l.cfg.StoreTimeout)
		err := l.store.Save(ctx, sn.UserID, State{Tokens: sn.Tokens, LastUsed: sn.LastUsed})
		cancel()
		if err != nil {
			l.cfg.Logger.Warn("pace: flush state", "user", sn.UserID, "err", err)
		}
	}
}

// onEvict translates one eviction into the public report. The registry counts
// them; this only tells anybody who asked to hear about it.
func (l *Limiter) onEvict(e registry.Eviction) {
	if l.cfg.Observer == nil || l.cfg.Observer.UserEvicted == nil {
		return
	}
	// The Limiter's own context: cancelled at Close, so a hook doing bounded
	// work can bail instead of holding up shutdown.
	l.cfg.Observer.UserEvicted(l.ctx, EvictInfo{
		UserID:   e.UserID,
		Reason:   evictReasonOf(e.Reason),
		Tokens:   e.Tokens,
		LastUsed: e.LastUsed,
	})
}

func evictReasonOf(r registry.Reason) EvictReason {
	switch r {
	case registry.Explicit:
		return EvictExplicit
	case registry.Shutdown:
		return EvictShutdown
	default: // registry.Idle
		return EvictIdle
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

// gcLoop drives the idle-user sweep and, when there is a durable queue, the
// expiry of its cached results. Both are background housekeeping against the
// same store on the same schedule, which is why one ticker serves them.
func (l *Limiter) gcLoop() {
	defer l.gcWg.Done()
	ticker := time.NewTicker(l.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.reg.Sweep()
			l.purgeResults()
		case <-l.ctx.Done():
			return
		}
	}
}
