package pace

import (
	"context"
	"time"
)

// Observer receives notifications about what a [Limiter] is doing. Every field
// is optional; a nil hook is skipped.
//
// It is a struct of functions rather than an interface on purpose. An interface
// cannot gain a method after v1 without breaking every implementation, and the
// events worth reporting will grow; a struct can gain a field. This is the same
// reasoning that keeps [Config] a struct.
//
// Hooks run on the caller's goroutine, in the request path. Keep them cheap —
// increment a counter, do not make a network call — or hand the work to a
// channel of your own.
type Observer struct {
	// Throttled is called when a request has to wait for a token, before it
	// waits.
	Throttled func(ctx context.Context, info ThrottleInfo)

	// RequestFinished is called after each HTTP round-trip, whether it
	// succeeded or not.
	RequestFinished func(ctx context.Context, info RequestInfo)

	// UserEvicted is called when a user's in-memory state is dropped.
	UserEvicted func(userID string, reason EvictReason)

	// JobTransition is called when a durable job changes state.
	JobTransition func(info JobInfo)
}

// ThrottleInfo describes a request that must wait for a token.
type ThrottleInfo struct {
	UserID string
	// Delay is how long the wait is expected to last. This is the number a
	// metrics pipeline actually wants; the previous callback reported only
	// that throttling had happened.
	Delay time.Duration
	// Tokens is the count available at the moment of the check.
	Tokens float64
	// Limit and Burst are the configuration in force for this user.
	Limit Limit
	Burst int
}

// RequestInfo describes a completed HTTP round-trip.
type RequestInfo struct {
	UserID string
	Method string
	Path   string
	// Status is the HTTP status code, or zero when no response arrived.
	Status int
	// Latency covers the round-trip only, not the wait for a token.
	Latency time.Duration
	// Durable reports whether the request went through the durable queue.
	Durable bool
	// Err is non-nil when no response was received.
	Err error
}

// EvictReason says why a user's in-memory state was dropped.
type EvictReason int

const (
	// EvictIdle means the GC sweep collected an inactive user.
	EvictIdle EvictReason = iota
	// EvictExplicit means a caller invoked Client.Evict.
	EvictExplicit
	// EvictShutdown means the Limiter closed and flushed everything.
	EvictShutdown
)

func (r EvictReason) String() string {
	switch r {
	case EvictIdle:
		return "idle"
	case EvictExplicit:
		return "explicit"
	case EvictShutdown:
		return "shutdown"
	default:
		return "unknown"
	}
}

// JobPhase is a durable job's position in its lifecycle, as reported to
// [Observer.JobTransition].
type JobPhase int

const (
	// JobClaimed means a worker took ownership and is about to send.
	JobClaimed JobPhase = iota
	// JobCompleted means the response was recorded.
	JobCompleted
	// JobRetrying means the attempt failed and another is scheduled.
	JobRetrying
	// JobDead means the job was abandoned.
	JobDead
)

func (p JobPhase) String() string {
	switch p {
	case JobClaimed:
		return "claimed"
	case JobCompleted:
		return "completed"
	case JobRetrying:
		return "retrying"
	case JobDead:
		return "dead"
	default:
		return "unknown"
	}
}

// JobInfo describes a durable job changing state.
type JobInfo struct {
	ID     string
	UserID string
	Method string
	Phase  JobPhase
	// Attempt is the attempt number this transition belongs to.
	Attempt int
	// RetryIn is set for JobRetrying: how long until the next attempt.
	RetryIn time.Duration
	// Reason is set for JobDead.
	Reason string
	// Err is the failure that caused a retry or a death, when there was one.
	Err error
}

// observeThrottled fires the throttle hooks, if any are configured.
func (l *Limiter) observeThrottled(ctx context.Context, info ThrottleInfo) {
	l.stats.throttled.Add(1)
	l.stats.waitNanos.Add(info.Delay.Nanoseconds())
	if l.cfg.Observer != nil && l.cfg.Observer.Throttled != nil {
		l.cfg.Observer.Throttled(ctx, info)
	}
}

// observesRequests reports whether anything is listening for finished requests.
//
// Call sites check it before assembling a RequestInfo. The struct carries
// strings, a duration and an error, and building one per request costs real
// allocation on a path that most callers run with no observer at all.
func (l *Limiter) observesRequests() bool {
	return l.cfg.Observer != nil && l.cfg.Observer.RequestFinished != nil
}

// countRequest records the outcome of a dispatched round-trip.
func (l *Limiter) countRequest(err error) {
	if err != nil {
		l.stats.errors.Add(1)
	}
}

func (l *Limiter) observeEvicted(userID string, reason EvictReason) {
	l.stats.evictions.Add(1)
	if l.cfg.Observer != nil && l.cfg.Observer.UserEvicted != nil {
		l.cfg.Observer.UserEvicted(userID, reason)
	}
}

func (l *Limiter) observeJob(info JobInfo) {
	if l.cfg.Observer != nil && l.cfg.Observer.JobTransition != nil {
		l.cfg.Observer.JobTransition(info)
	}
}
