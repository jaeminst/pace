package pace

import "time"

// Backoff exposes RetryPolicy's schedule so tests can assert on it without
// waiting out real delays.
func Backoff(p RetryPolicy, attempt int) time.Duration {
	return p.withDefaults().backoff(attempt)
}
