package limiter

import (
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/observe"
)

// counters holds the atomic tallies behind Stats. They are separate from the
// snapshot type so that Stats stays a plain value a caller can copy and diff.
type counters struct {
	requests     atomic.Int64
	throttled    atomic.Int64
	waitNanos    atomic.Int64
	errors       atomic.Int64
	quotaTakes   atomic.Int64
	quotaRefused atomic.Int64
	quotaErrors  atomic.Int64
}

// Stats returns a snapshot of the Limiter's counters and current population.
//
// It is cheap enough to call on a scrape interval: the counters are atomic
// loads, and the user count sums a per-shard tally rather than acquiring every
// shard lock.
func (l *Limiter) Stats() observe.Stats {
	return observe.Stats{
		Users:        l.reg.Users(),
		Requests:     l.stats.requests.Load(),
		Throttled:    l.stats.throttled.Load(),
		WaitTotal:    time.Duration(l.stats.waitNanos.Load()),
		Errors:       l.stats.errors.Load(),
		Evictions:    l.reg.Evictions(),
		QuotaTakes:   l.stats.quotaTakes.Load(),
		QuotaRefused: l.stats.quotaRefused.Load(),
		QuotaErrors:  l.stats.quotaErrors.Load(),
	}
}
