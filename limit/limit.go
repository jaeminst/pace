package limit

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
//	Rate: limit.PerMinute(60)
type Limit float64

// Inf is a Limit that permits requests without throttling. A Limiter
// configured with Inf ignores Burst.
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

// Quota is the rate and burst in force for one user.
//
// The zero Quota selects Config.Rate and Config.Burst, and each field falls
// back independently. That is deliberate rather than incidental: a
// Config.QuotaFor backed by a map returns the zero Quota for every user the
// map does not mention, which is exactly the default those users should get.
//
// Persisted token state carries no quota. A user restored from a a store
// is given whatever QuotaFor returns at that moment, and their saved tokens are
// capped at the current burst — so lowering someone's tier takes effect on
// their next restore instead of granting them a ceiling they no longer have.
type Quota struct {
	// Rate is the maximum request rate. Zero or negative selects Config.Rate.
	Rate Limit

	// Burst is the token ceiling. Zero or negative selects Config.Burst.
	Burst int
}

// Finite maps a true infinity onto [Inf], the value the token bucket can
// actually work with.
//
// [Limit] is a float64, so nothing stops a caller writing Limit(math.Inf(1)) —
// which reads as "no limit" and is a reasonable thing to try. But Inf is
// math.MaxFloat64 rather than a real infinity precisely so the arithmetic
// downstream stays defined: handing x/time/rate a genuine +Inf produces a
// bucket whose token count is NaN, and one that therefore refuses every request
// for the life of the process. Found by fuzzing RestoreBucket.
//
// A NaN needs no case here. It fails the `> 0` test above, so a NaN from
// QuotaFor falls back to the validated Config.Rate; a NaN in Config.Rate itself
// is rejected by validate.
func Finite(r Limit) Limit {
	if math.IsInf(float64(r), 0) {
		return Inf
	}
	return r
}
