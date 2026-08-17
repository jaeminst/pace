package queue

import (
	"math"
	"math/rand/v2"
	"time"
)

// RetryPolicy controls how a durable job is retried after a delivery failure.
//
// It governs delivery, not success. A request that reached the server and came
// back with a 500 was delivered; whether that counts as failure is the caller's
// judgement, expressed through [Config.RetryOn].
type RetryPolicy struct {
	// MaxAttempts is the total number of sends allowed for one job, including
	// the first. Reaching it dead-letters the job. Zero defaults to 5.
	MaxAttempts int

	// BaseDelay is the wait before the second attempt. Zero defaults to 500ms.
	BaseDelay time.Duration

	// MaxDelay caps the backoff. Zero defaults to 30s.
	MaxDelay time.Duration

	// Multiplier grows the delay each attempt. Zero or less than one
	// defaults to 2.
	Multiplier float64

	// NoJitter disables randomising the delay. Leave it false.
	//
	// Jitter is on by default because the failure that matters is correlated:
	// an upstream outage stalls every job at once, and a fixed schedule sends
	// them all back at the same instant. Full jitter — a uniform draw from
	// [0, computed] — spreads that out; it is one line and strictly better
	// than retrying in lockstep.
	NoJitter bool
}

// WithDefaults resolves every optional field, so the schedule can be read
// without re-checking for zeroes at each step.
func (p RetryPolicy) WithDefaults() RetryPolicy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 5
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 500 * time.Millisecond
	}
	if p.MaxDelay <= 0 {
		p.MaxDelay = 30 * time.Second
	}
	if p.Multiplier < 1 {
		p.Multiplier = 2
	}
	return p
}

// Backoff returns how long to wait before the given attempt number, where
// attempt 1 has already been made.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	// float64 keeps the exponent from overflowing for large attempt counts;
	// MaxDelay caps it long before precision matters.
	delay := float64(p.BaseDelay) * math.Pow(p.Multiplier, float64(attempt-1))
	if capped := float64(p.MaxDelay); delay > capped || math.IsInf(delay, 0) {
		delay = capped
	}
	if p.NoJitter {
		return time.Duration(delay)
	}
	return time.Duration(rand.Float64() * delay) //nolint:gosec // jitter needs spread, not unpredictability
}
