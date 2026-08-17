package pace

import (
	"errors"
	"fmt"

	"time"

	"github.com/jaeminst/pace/internal/registry"
)

// ErrClosed is returned once the [Limiter] has been closed or has begun
// shutting down. It reports that the Limiter will accept no further work — not
// that a particular request timed out; see [LimitError] for that.
//
// A [Client] has no lifecycle of its own, so this is always about the Limiter
// it came from.
var ErrClosed = errors.New("pace: limiter closed")

// ErrNoQueue is returned by [Client.Durable] when no durable queue is
// configured.
//
// The queue lives in the SQLite file named by [Config.DBPath]. [Config.Store]
// persists per-user token state and does not provide one; set both if you want
// a custom state backend and a durable queue. The message says so, because the
// alternative is a log line at every boot that queue-less callers do not need.
var ErrNoQueue = errors.New("pace: durable queue not configured: set Config.DBPath (Config.Store alone does not provide one)")

// ErrJobClaimed reports that another worker — possibly in another process
// sharing the same database — already owns this durable job. It is not a
// failure: it means the request is being sent exactly once, by someone else.
var ErrJobClaimed = errors.New("pace: durable job is claimed by another worker")

// ErrInvalidID is returned by [Client.Durable] when id is empty. An empty ID
// cannot identify a job, so it is rejected rather than quietly degrading to a
// non-durable request.
var ErrInvalidID = errors.New("pace: durable id must not be empty")

// ErrBodyTooLarge is returned when a response body exceeds
// [Config.MaxResponseBytes].
var ErrBodyTooLarge = errors.New("pace: response body too large")

// ErrStreamDurable is returned by [Request.Stream] on a durable request. The
// queue caches a response so it can be returned to a later caller, which it
// cannot do for a stream that is consumed once.
var ErrStreamDurable = errors.New("pace: Stream is not available for durable requests")

// ConfigError reports an invalid [Config] field. It is returned only by [New].
type ConfigError struct {
	// Field is the offending field's name, without the Config prefix.
	Field string
	// Value is what was supplied, when showing it helps.
	Value any
	// Err is the underlying cause, if any.
	Err error
}

func (e *ConfigError) Error() string {
	switch {
	case e.Err != nil && e.Value != nil:
		return fmt.Sprintf("pace: invalid Config.%s (%v): %v", e.Field, e.Value, e.Err)
	case e.Err != nil:
		return fmt.Sprintf("pace: invalid Config.%s: %v", e.Field, e.Err)
	case e.Value != nil:
		return fmt.Sprintf("pace: invalid Config.%s: %v", e.Field, e.Value)
	default:
		return "pace: invalid Config." + e.Field
	}
}

func (e *ConfigError) Unwrap() error { return e.Err }

// LimitError reports that a request could not obtain a rate-limit token.
//
// It is what makes throttling actionable: without it, a caller whose deadline
// expired while queued for a token cannot tell that case apart from any other
// context deadline.
//
//	var le *pace.LimitError
//	if errors.As(err, &le) {
//	    retryAfter(le.Delay)
//	}
type LimitError struct {
	// UserID is the identity whose bucket was exhausted.
	UserID string
	// Limit and Burst are the configuration in force for that user.
	Limit Limit
	Burst int
	// Delay is how long the caller would have had to wait. It is zero when
	// the wait length could not be determined.
	Delay time.Duration
	// Err is the underlying cause: context.DeadlineExceeded,
	// context.Canceled, or an error from the limiter.
	Err error
}

func (e *LimitError) Error() string {
	if e.Delay > 0 {
		// Rounded: the caller reads Delay for the exact figure, and an error
		// string carrying nine significant digits is just noise.
		return fmt.Sprintf("pace: rate limit for %q (%s, burst %d): %v; retry in %v",
			e.UserID, e.Limit, e.Burst, e.Err, e.Delay.Round(time.Millisecond))
	}
	return fmt.Sprintf("pace: rate limit for %q (%s, burst %d): %v",
		e.UserID, e.Limit, e.Burst, e.Err)
}

func (e *LimitError) Unwrap() error { return e.Err }

// throttled turns a failed wait into the error every waiting path reports.
//
// The Limiter's own context decides between the two outcomes rather than the
// caller's: the bucket reports "would exceed context deadline" without waiting,
// so ctx.Err() is legitimately nil in that case too, and reading it told
// callers the Limiter was closed when it was not.
//
// Delay is measured here, at the point of failure, not at entry — it is the
// number a caller reads to decide when to try again.
func (l *Limiter) throttled(userID string, u *registry.User, err error) error {
	if l.ctx.Err() != nil {
		return ErrClosed
	}
	q := quotaOf(u)
	return &LimitError{
		UserID: userID,
		Limit:  q.Rate,
		Burst:  q.Burst,
		Delay:  u.Bucket().DelayAt(l.cfg.Clock.Now()),
		Err:    err,
	}
}
