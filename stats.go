package pace

import (
	"sync/atomic"
	"time"
)

// Stats is a point-in-time snapshot of a [Limiter].
//
// Counters are monotonic since the Limiter was created. Users is sampled per
// shard rather than under one lock, so it is accurate to the moment each shard
// was read rather than to a single instant — which is the right trade for a
// number whose purpose is a gauge on a dashboard.
type Stats struct {
	// Users is how many users currently hold in-memory state.
	Users int64

	// Requests counts attempts to obtain a token, whether or not one was
	// granted and whether or not the request was then dispatched.
	Requests uint64
	// Throttled counts those that had to wait for a token.
	Throttled uint64
	// Wait is the total expected wait across throttled requests.
	Wait time.Duration
	// Errors counts dispatched requests that came back without a response. A
	// request that never obtained a token was never dispatched; it is counted
	// by Throttled instead.
	Errors uint64
	// Evictions counts users dropped from memory, for any reason.
	Evictions uint64
}

// counters holds the atomic tallies behind Stats. They are separate from the
// snapshot type so that Stats stays a plain value a caller can copy and diff.
type counters struct {
	requests  atomic.Uint64
	throttled atomic.Uint64
	waitNanos atomic.Int64
	errors    atomic.Uint64
	evictions atomic.Uint64
}

// Stats returns a snapshot of the Limiter's counters and current population.
//
// It is cheap enough to call on a scrape interval: the counters are atomic
// loads, and the user count sums a per-shard tally rather than acquiring every
// shard lock.
func (l *Limiter) Stats() Stats {
	var users int64
	for i := range l.shards {
		users += l.shards[i].live.Load()
	}
	return Stats{
		Users:     users,
		Requests:  l.stats.requests.Load(),
		Throttled: l.stats.throttled.Load(),
		Wait:      time.Duration(l.stats.waitNanos.Load()),
		Errors:    l.stats.errors.Load(),
		Evictions: l.stats.evictions.Load(),
	}
}
