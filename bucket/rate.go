package bucket

import (
	"math"
	"strconv"
	"time"
)

// Limit is a maximum request rate, expressed in requests per second.
//
// The zero Limit allows no requests. Build one with [PerSecond], [PerMinute],
// [PerHour], or [Every] rather than converting a number directly, so the unit
// is visible where it is written: PerMinute(60) and PerSecond(1) are the same
// value, and only one of them says which the author meant.
//
//	Rate: bucket.PerMinute(60)
type Limit float64

// Inf is a Limit that permits requests without throttling. A bucket configured
// with Inf ignores its burst.
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
// It is an absolute pair, not a patch on something else. A Quota that comes
// back from [github.com/jaeminst/pace/config.Config.QuotaFor] is the answer for
// that user, zero fields included — which is why a map-backed QuotaFor has to
// say what an unlisted user gets rather than leaving it to a default the
// library holds. Until v0.14.0 a zero field selected a Config-wide default;
// that default is gone, and with it the question of which of the two was
// right when they disagreed.
//
// Persisted token state carries no quota. A user restored from a store is given
// whatever QuotaFor returns at that moment, over whatever the default is at that
// moment, and their saved tokens are capped at the current burst — so lowering
// someone's tier takes effect on their next restore instead of granting them a
// ceiling they no longer have. This is the path a demotion lands on quietly: a
// user who was evicted before the change comes back on the new terms with no
// reload involved.
type Quota struct {
	// Rate is the maximum request rate. A rate at or below zero is one no
	// bucket can refill from, so the user it describes is throttled to a
	// standstill once their initial burst is spent.
	Rate Limit

	// Burst is the token ceiling. Zero or negative is raised to one, since a
	// bucket that can hold nothing can never hand anything out.
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
// A NaN needs no case here, and is deliberately let through: it fails every
// `> 0` test downstream, which is how the engine recognises a rate it cannot
// use. Clamping it to something usable here would hide the mistake instead.
func Finite(r Limit) Limit {
	if math.IsInf(float64(r), 0) {
		return Inf
	}
	return r
}
