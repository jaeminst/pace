// parse.go reads a rate the way a person writes one.
//
// [PerMinute](60) says the unit in Go; "60/m" says it in a config file, an
// environment variable, or a flag — which is where a rate limit usually comes
// from. The two produce the same [Limit], and [Limit.String] round-trips
// through [ParseLimit], so a value can be written out and read back.

package bucket

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// units maps every accepted spelling of a period onto its length in seconds.
//
// A table rather than a switch because the spellings are data: "m" and "min"
// and "minutes" differ in nothing but how much the author felt like typing.
// Note that "m" is minutes, never milli — this is a rate, and a
// requests-per-millisecond limit is not a thing anyone writes.
var units = map[string]float64{
	"s": 1, "sec": 1, "secs": 1, "second": 1, "seconds": 1,
	"m": 60, "min": 60, "mins": 60, "minute": 60, "minutes": 60,
	"h": 3600, "hr": 3600, "hrs": 3600, "hour": 3600, "hours": 3600,
}

// rpUnits maps the "rpm" family onto the same periods. Kept separate from
// units because "rps" is a whole token, not "r" plus "ps": folding them into
// one table would make "6r/s" and "6ps" parse, and neither is a rate anyone
// wrote on purpose.
var rpUnits = map[string]float64{"rps": 1, "rpm": 60, "rph": 3600}

// ErrBadLimit is the error [ParseLimit] and [ParseQuota] report. Use
// [errors.Is] on it; the message names what was wrong with the input.
var ErrBadLimit = errors.New("bucket: not a rate")

// ParseLimit reads a rate written the way a person writes one.
//
// The number comes first, then the period, in any of these spellings:
//
//	6/m   6/min   6/minute   6/minutes   6rpm   6RPM
//	1/s   1/sec   1/second   1/seconds   1rps   1RPS
//	100/h 100/hr  100/hour   100/hours   100rph 100RPH
//
// Case is ignored, surrounding and internal spaces are allowed ("6 / min"), and
// the number may be fractional ("2.5/s"). "inf" — also "infinite" and
// "unlimited" — is [Inf], the rate that throttles nothing.
//
// A rate at or below zero is refused rather than returned, because a bucket
// built from one never refills and the caller almost certainly did not mean to
// stop every request forever. Everything else that does not parse is refused
// too; errors wrap [ErrBadLimit].
//
// It accepts what [Limit.String] produces, so a Limit survives a round-trip
// through text.
func ParseLimit(s string) (Limit, error) {
	raw := s
	s = strings.ToLower(strings.Join(strings.Fields(s), ""))
	if s == "" {
		return 0, fmt.Errorf("%w: %q is empty", ErrBadLimit, raw)
	}
	switch s {
	case "inf", "infinite", "unlimited":
		return Inf, nil
	}

	num, unit, err := split(s, raw)
	if err != nil {
		return 0, err
	}

	n, err := strconv.ParseFloat(num, 64)
	if errors.Is(err, strconv.ErrRange) {
		// ParseFloat hands back ±Inf here as well as the error. Refusing is
		// better than taking it: a number that overflows a float64 is a
		// mistake, and "inf" is how someone says they meant no limit.
		return 0, fmt.Errorf("%w: %q is too large a number", ErrBadLimit, raw)
	}
	if err != nil {
		return 0, fmt.Errorf("%w: %q has no number in front of %q", ErrBadLimit, raw, unit)
	}
	// A NaN fails every comparison below, so it needs saying first or it slips
	// through the `<= 0` guard and reaches the bucket as a rate that refuses
	// every request for the life of the process.
	if math.IsNaN(n) {
		return 0, fmt.Errorf("%w: %q is not a number", ErrBadLimit, raw)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%w: %q is not above zero", ErrBadLimit, raw)
	}
	// Reachable as "inf/s" and its spellings, where the period is written out
	// and ParseFloat reads the infinity without complaint.
	if math.IsInf(n, 1) {
		return Inf, nil
	}

	per, ok := units[unit]
	if !ok {
		per = rpUnits[unit]
	}
	return Finite(Limit(n / per)), nil
}

// split separates the number from the period, whichever of the two spellings
// was used. It reports the unit it found so the caller's error can quote it.
func split(s, raw string) (num, unit string, err error) {
	if num, unit, ok := strings.Cut(s, "/"); ok {
		if _, known := units[unit]; !known {
			return "", "", fmt.Errorf("%w: %q is not a period in %q; try s, min or hour",
				ErrBadLimit, unit, raw)
		}
		return num, unit, nil
	}
	for suffix := range rpUnits {
		if rest, ok := strings.CutSuffix(s, suffix); ok {
			return rest, suffix, nil
		}
	}
	return "", "", fmt.Errorf("%w: %q names no period; write 6/min or 6rpm", ErrBadLimit, raw)
}

// NewLimit is [ParseLimit] for a rate written in the source, where a bad one is
// a typo rather than a condition to handle. It panics on anything ParseLimit
// refuses.
//
//	config.Config{BaseURL: "…", Quota: bucket.NewQuota("6/m", 10)}
//
// Use ParseLimit for a rate that arrives from a file, a flag or an environment
// variable — anywhere the string is not something you can fix by editing the
// line above the failure.
func NewLimit(s string) Limit {
	l, err := ParseLimit(s)
	if err != nil {
		panic(err.Error())
	}
	return l
}

// ParseQuota reads a rate and pairs it with a burst.
//
// The burst is a plain int because there is nothing to spell: a ceiling is a
// count of tokens, with no unit to get wrong. A burst below one is raised to
// one, as everywhere else — a bucket that can hold nothing can never hand
// anything out.
func ParseQuota(rate string, burst int) (Quota, error) {
	l, err := ParseLimit(rate)
	if err != nil {
		return Quota{}, err
	}
	if burst < 1 {
		burst = 1
	}
	return Quota{Rate: l, Burst: burst}, nil
}

// NewQuota is [ParseQuota] for a quota written in the source. It panics on a
// rate [ParseLimit] refuses.
//
//	Quota: bucket.NewQuota("6/m", 10)
func NewQuota(rate string, burst int) Quota {
	q, err := ParseQuota(rate, burst)
	if err != nil {
		panic(err.Error())
	}
	return q
}
