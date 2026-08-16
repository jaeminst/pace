package pace

import (
	"math"
	"strconv"
	"time"
)

// Limit is a maximum request rate, expressed in requests per second.
//
// The zero Limit allows no requests. Build one with [PerSecond], [PerMinute],
// [PerHour], or [Every] rather than converting a number directly, so the unit
// is visible at the call site:
//
//	Rate: pace.PerMinute(60)
type Limit float64

// Inf is a Limit that permits requests without throttling. A Client configured
// with Inf ignores Burst.
const Inf = Limit(math.MaxFloat64)

// PerSecond returns the Limit permitting n requests per second.
func PerSecond(n float64) Limit { return Limit(n) }

// PerMinute returns the Limit permitting n requests per minute.
func PerMinute(n float64) Limit { return Limit(n / 60) }

// PerHour returns the Limit permitting n requests per hour.
func PerHour(n float64) Limit { return Limit(n / 3600) }

// Every returns the Limit permitting one request per interval.
// Every(0) or a negative interval returns [Inf].
func Every(interval time.Duration) Limit {
	if interval <= 0 {
		return Inf
	}
	return Limit(1 / interval.Seconds())
}

// String renders the Limit in the largest unit that keeps the number at or
// above one, so PerMinute(30) prints as "30/min" and PerMinute(60) as "1/s".
func (l Limit) String() string {
	switch {
	case l == Inf:
		return "Inf"
	case l <= 0:
		return "0"
	case l >= 1:
		return fmtRate(float64(l)) + "/s"
	case l*60 >= 1:
		return fmtRate(float64(l)*60) + "/min"
	default:
		return fmtRate(float64(l)*3600) + "/hour"
	}
}

// fmtRate trims the float to something a human reads without losing the value.
func fmtRate(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}
