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
//
// Every counter is int64, including the ones that cannot go negative. Metrics
// code diffs consecutive snapshots, and unsigned subtraction wraps silently on
// the reset a restarted Limiter produces; a negative delta is a visible bug
// where 18446744073709551615 is a mystery.
type Stats struct {
	// Users is how many users currently hold in-memory state.
	Users int64

	// Requests counts attempts to obtain a token, whether or not one was
	// granted and whether or not the request was then dispatched.
	Requests int64
	// Throttled counts those that had to wait for a token.
	//
	// It under-counts on one path: with a [WaitingSharedQuota], the backend
	// owns the wait and pace cannot tell in advance whether a caller will be
	// parked, so nothing is reported. See [Observer.Throttled].
	Throttled int64
	// WaitTotal is the sum of the expected waits across throttled requests.
	// It is a running total, not a current or average value — divide by
	// Throttled for a mean.
	WaitTotal time.Duration
	// Errors counts dispatched requests that came back without a response. A
	// request that never obtained a token was never dispatched; it is counted
	// by Throttled instead.
	Errors int64
	// Evictions counts users dropped from memory, for any reason.
	Evictions int64

	// QuotaTakes counts requests for a token made to [Config.SharedQuota],
	// whether granted, refused, or failed. Zero when no shared quota is
	// configured.
	QuotaTakes int64
	// QuotaRefused counts those the backend answered with a refusal. A healthy
	// shared limiter refuses constantly; this is the shape of the load, not an
	// alarm.
	QuotaRefused int64
	// QuotaErrors counts the times the backend could not give an answer at all
	// — it failed, timed out, or the circuit breaker was short-circuiting calls
	// to one already known to be down.
	//
	// This is the number worth alerting on. A shared limiter whose backend is
	// unreachable keeps serving traffic under the default
	// [QuotaFallbackLocal], at the configured rate *per replica* rather than in
	// total, and nothing else in this snapshot would show it.
	QuotaErrors int64
}

// counters holds the atomic tallies behind Stats. They are separate from the
// snapshot type so that Stats stays a plain value a caller can copy and diff.
type counters struct {
	requests     atomic.Int64
	throttled    atomic.Int64
	waitNanos    atomic.Int64
	errors       atomic.Int64
	evictions    atomic.Int64
	quotaTakes   atomic.Int64
	quotaRefused atomic.Int64
	quotaErrors  atomic.Int64
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
		Users:        users,
		Requests:     l.stats.requests.Load(),
		Throttled:    l.stats.throttled.Load(),
		WaitTotal:    time.Duration(l.stats.waitNanos.Load()),
		Errors:       l.stats.errors.Load(),
		Evictions:    l.stats.evictions.Load(),
		QuotaTakes:   l.stats.quotaTakes.Load(),
		QuotaRefused: l.stats.quotaRefused.Load(),
		QuotaErrors:  l.stats.quotaErrors.Load(),
	}
}
