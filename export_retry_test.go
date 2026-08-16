package pace

import "time"

// Backoff exposes RetryPolicy's schedule so tests can assert on it without
// waiting out real delays.
func Backoff(p RetryPolicy, attempt int) time.Duration {
	return p.withDefaults().backoff(attempt)
}

// PurgeResults runs the cached-result purge without waiting for the GC tick.
func PurgeResults(l *Limiter) { l.purgeResults() }
