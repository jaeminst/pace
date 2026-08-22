package limiter

import (
	"sync/atomic"
	"time"

	"github.com/jaeminst/pace/observe"
)

// counters holds the atomic tallies behind Stats. They are separate from the
// snapshot type so that Stats stays a plain value a caller can copy and diff.
type counters struct {
	requests  atomic.Int64
	throttled atomic.Int64
	waitNanos atomic.Int64
	errors    atomic.Int64
}

// Stats returns a snapshot of the Limiter's counters and current population.
//
// It is cheap enough to call on a scrape interval: the counters are atomic
// loads, and the key count sums a per-shard tally rather than acquiring every
// shard lock.
func (l *Limiter) Stats() observe.Stats {
	s := observe.Stats{
		Keys:      l.reg.Keys(),
		Requests:  l.stats.requests.Load(),
		Throttled: l.stats.throttled.Load(),
		WaitTotal: time.Duration(l.stats.waitNanos.Load()),
		Errors:    l.stats.errors.Load(),
		Evictions: l.reg.Evictions(),
	}
	// The shared-quota counters live with the component that writes them, and
	// there is no component when no backend is configured — which is the same
	// thing the zeroes here would have said.
	if l.gate != nil {
		s.QuotaTakes, s.QuotaRefused, s.QuotaErrors = l.gate.Takes(), l.gate.Refused(), l.gate.Errors()
	}
	return s
}
