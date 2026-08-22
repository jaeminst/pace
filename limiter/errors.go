package limiter

import (
	"errors"
	"fmt"
	"time"

	"github.com/jaeminst/pace/config"

	"github.com/jaeminst/pace/gate"
	"github.com/jaeminst/pace/registry"
)

// ErrClosed is returned once the [Limiter] has been closed or has begun
// shutting down. It reports that the Limiter will accept no further work — not
// that a particular request timed out; see [LimitError] for that.
//
// A handle derived from a Limiter has no lifecycle of its own, so this is
// always about the Limiter it came from.
var ErrClosed = errors.New("pace: limiter closed")

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
	Limit config.Limit
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

// throttledFromGate turns what the gate returns into what a caller expects.
//
// A failure to obtain a token inside the caller's deadline is this package's
// LimitError, carrying the user and the limit in force. Anything else — a
// refusal under shared.Deny, ErrClosed — is already the error the caller should
// see, and passes through. The gate marks the difference rather than calling
// back here to build the error, which is what keeps LimitError out of its API.
func (l *Limiter) throttledFromGate(userID string, u *registry.User, err error) error {
	var we *gate.WaitError
	if errors.As(err, &we) {
		return l.throttled(userID, u, we.Cause)
	}
	return err
}

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
		Delay:  u.Bucket().DelayAt(l.cfg.Now()),
		Err:    err,
	}
}
