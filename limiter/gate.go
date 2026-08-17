package limiter

import (
	"context"
	"errors"
	"time"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/gate"
	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/registry"
)

// newGate wires the shared-quota decision to this Limiter.
//
// It is nil when no backend is configured, which is what sharedEnabled tests
// for: building one would mean a breaker and three counters on every Limiter
// that never consults a backend.
func (l *Limiter) newGate() *gate.Gate {
	if l.cfg.Shared.Quota == nil {
		return nil
	}
	return gate.New(l.ctx, gate.Config{
		Quota:     l.cfg.Shared.Quota,
		Namespace: l.cfg.Shared.Namespace,
		Timeout:   l.cfg.Shared.Timeout,
		OnError:   l.cfg.Shared.OnError,
		Logger:    l.cfg.Logger,
		Now:       l.cfg.Clock.Now,
		Closed:    ErrClosed,
		Throttled: l.reportBucketThrottle,
		// Method values, not the hooks themselves: a test may install one after
		// the Limiter has started.
		BeforeWait:      l.fireBeforeWait,
		BeforeQuotaTake: l.fireBeforeQuotaTake,
	})
}

// sharedEnabled reports whether requests must consult a shared backend.
func (l *Limiter) sharedEnabled(q rate.Quota) bool {
	return l.gate != nil && gate.Enabled(q)
}

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

// reportBucketThrottle is Config.Throttled for the gate: the same report as
// reportThrottleTokens, reached with a bucket rather than a user because that
// is all the gate holds.
func (l *Limiter) reportBucketThrottle(
	ctx context.Context, userID string, b *bucket.Bucket, delay time.Duration, at time.Time, tokens *float64,
) {
	l.reportBucketTokens(ctx, userID, b, delay, at, tokens)
}
