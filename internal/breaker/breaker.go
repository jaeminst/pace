// Package breaker short-circuits calls to a dependency that is failing.
//
// It is a state machine over a failure count and a clock, and nothing else: it
// takes no context, performs no I/O, and knows nothing about what it is
// guarding. That is what makes it testable without the subsystem around it —
// which matters here, because the half-open state exists precisely to handle a
// dead backend, and reaching that state through a live one costs a real
// timeout per transition.
package breaker

import (
	"sync"
	"time"
)

// These are not configurable on purpose: the job is to stop a dead dependency
// charging every request a full timeout, and nobody is going to tune that. A
// configurable version would be two more fields on a public Config that must
// keep working forever.
const (
	// Threshold is how many consecutive failures open the breaker.
	Threshold = 5
	// Cooldown is how long it stays open before a single probe is let through
	// to test the dependency.
	Cooldown = 5 * time.Second
)

// Breaker short-circuits calls to a dependency that is failing, so a dead one
// costs one timeout every Cooldown rather than one per request.
//
// It has three states. Closed: everything through, consecutive failures
// counted. Open: nothing through until the cooldown elapses. Half-open: exactly
// one probe through, and every other caller refused until that probe reports
// back. Without the half-open state the cooldown expiring released the whole
// backlog at once, and each of them paid a full timeout to rediscover that the
// dependency was still down — which is the cost the breaker exists to avoid.
//
// The zero value is closed and ready to use.
type Breaker struct {
	mu       sync.Mutex
	failures int
	openTill time.Time
	// probing is set while a half-open probe is in flight. It is cleared by
	// whichever of Succeeded, Failed or Abandoned resolves that probe, so every
	// path out of a guarded call must call one of them.
	probing bool
}

// Allow reports whether a call should be attempted at now, claiming the
// half-open probe if this is the caller that gets it.
func (b *Breaker) Allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.openTill.IsZero():
		return true // closed
	case now.Before(b.openTill):
		return false // open
	case b.probing:
		return false // half-open, and somebody else has the probe
	default:
		b.probing = true
		return true
	}
}

// Succeeded records a call that worked, closing the breaker.
func (b *Breaker) Succeeded() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.failures = 0
	b.openTill = time.Time{}
	b.probing = false
}

// Failed records a call that did not work.
func (b *Breaker) Failed(now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.probing {
		// The probe is the verdict. Re-open on it alone rather than waiting for
		// another full threshold, which is what made recovery from a still-dead
		// dependency cost five more timeouts every cooldown.
		b.probing = false
		b.openTill = now.Add(Cooldown)
		b.failures = 0
		return
	}
	b.failures++
	if b.failures >= Threshold {
		b.openTill = now.Add(Cooldown)
		b.failures = 0
	}
}

// Abandoned releases a probe that produced no verdict, because the caller went
// away before the dependency answered. That says nothing about the dependency,
// so it must neither close the breaker nor count against it — but it must
// release the probe, or the half-open state would never resolve and the breaker
// would stay shut forever.
func (b *Breaker) Abandoned() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.probing = false
}
