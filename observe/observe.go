package observe

import (
	"context"
	"time"

	"github.com/jaeminst/pace/rate"
)

// Observer receives notifications about what a a Limiter is doing. Every field
// is optional; a nil hook is skipped.
//
// It is a struct of functions rather than an interface on purpose. An interface
// cannot gain a method after v1 without breaking every implementation, and the
// events worth reporting will grow; a struct can gain a field. This is the same
// reasoning that keeps limiter.Config a struct.
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

	// UserEvicted is called when a user's in-memory state is dropped, after it
	// has been persisted. A failed save is reported as an error to whoever
	// asked for the eviction rather than announced here as a clean one.
	//
	// No shard lock is held, so the hook may call back into the Limiter —
	// Client.Tokens, Limiter.Stats, even Client.Evict on another user.
	UserEvicted func(ctx context.Context, info EvictInfo)

	// JobTransition is called when a durable job changes state.
	JobTransition func(ctx context.Context, info JobInfo)
}

// EvictInfo describes a user whose in-memory state has just been dropped.
type EvictInfo struct {
	UserID string
	// Reason says which of the three ways it happened.
	Reason EvictReason
	// Tokens is the count the user held when they were dropped. For an idle
	// sweep that is the value persisted; for a shutdown it is the last one seen.
	Tokens float64
	// LastUsed is when the user last took a token.
	LastUsed time.Time
}

// ThrottleInfo describes a request that must wait for a token.
type ThrottleInfo struct {
	UserID string
	// Delay is how long the wait is expected to last. This is the number a
	// metrics pipeline actually wants; the previous callback reported only
	// that throttling had happened.
	Delay time.Duration
	// Tokens is the count available at the moment of the check.
	//
	// With a shared.Config.Quota configured this is the backend's own count
	// when it reports one (shared.Grant.Tokens), since the local bucket is only a
	// shadow of the shared quota and never authoritative. A backend that does
	// not track tokens leaves the shadow's count here.
	Tokens float64
	// Limit and Burst are the configuration in force for this user.
	Limit rate.Limit
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

// Stats is a point-in-time snapshot of a a Limiter.
//
// Counters are monotonic since the Limiter was created. Users is sampled per
// shard rather than under one lock, so it is accurate to the moment each shard
// was read rather than to a single instant — which is the right trade for a
// number whose purpose is a gauge on a dashboard.
//
// Every counter is int64, including the ones that cannot go negative. Metrics
// code diffs consecutive snapshots, and unsigned subtraction wraps silently on
// the reset a restarted Limiter produces; a negative delta is a visible bug
// where 18446744073709551615 is a mystery.
type Stats struct {
	// Users is how many users currently hold in-memory state.
	Users int64

	// Requests counts attempts to obtain a token, whether or not one was
	// granted and whether or not the request was then dispatched.
	Requests int64
	// Throttled counts those that had to wait for a token.
	//
	// It under-counts on one path: with a a shared.Waiter, the backend
	// owns the wait and pace cannot tell in advance whether a caller will be
	// parked, so nothing is reported. See Observer.Throttled.
	Throttled int64
	// WaitTotal is the sum of the expected waits across throttled requests.
	// It is a running total, not a current or average value — divide by
	// Throttled for a mean.
	WaitTotal time.Duration
	// Errors counts dispatched requests that came back without a response. A
	// request that never obtained a token was never dispatched; it is counted
	// by Throttled instead.
	Errors int64
	// Evictions counts users dropped from memory, for any reason.
	Evictions int64

	// QuotaTakes counts requests for a token made to shared.Config.Quota,
	// whether granted, refused, or failed. Zero when no shared quota is
	// configured.
	QuotaTakes int64
	// QuotaRefused counts those the backend answered with a refusal. A healthy
	// shared limiter refuses constantly; this is the shape of the load, not an
	// alarm.
	QuotaRefused int64
	// QuotaErrors counts the times the backend could not give an answer at all
	// — it failed, timed out, or the circuit breaker was short-circuiting calls
	// to one already known to be down.
	//
	// This is the number worth alerting on. A shared limiter whose backend is
	// unreachable keeps serving traffic under the default
	// [QuotaFallbackLocal], at the configured rate *per replica* rather than in
	// total, and nothing else in this snapshot would show it.
	QuotaErrors int64
}
