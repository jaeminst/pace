// api.go is what the engine offers its owner: one method per question that can
// be asked about a user, each keyed by that user's identity.
//
// The engine has no notion of HTTP. A Client — the handle a caller actually
// holds — belongs to github.com/jaeminst/pace/client, which binds a user ID
// once and delegates here. These are the questions it delegates.
//
// The two that are not here are in observer.go, next to the counters they move:
// [Limiter.StartTiming] and [Limiter.FinishRequest].
//
// The two that are not here are in observer.go, next to the counters they move:
// [Limiter.StartTiming] and [Limiter.FinishRequest].

package limiter

import (
	"context"

	"github.com/jaeminst/pace/config"
)

// Enter registers work that must finish before a shutdown completes, and
// returns a context cancelled when either ctx or the Limiter's own lifetime
// ends. It reports false when the Limiter is already shutting down, in which
// case nothing was registered and the returned func must not be called.
//
// The returned func releases both. It is deliberately one func rather than two:
// forgetting the second is a hang at shutdown, and there is no path on which a
// caller wants one without the other.
//
// A caller that must let work outlive its own return — a streamed body, say —
// passes the func on rather than deferring it. That is the whole reason this is
// separate from [Limiter.Acquire]: the barrier and the token have different
// lifetimes, and the token is also paid *after* whatever setup might fail
// without costing the user anything.
func (l *Limiter) Enter(ctx context.Context) (context.Context, func(), bool) {
	if !l.enter() {
		return ctx, nil, false
	}
	merged, release := l.withLifetime(ctx)
	return merged, func() {
		release()
		l.leave()
	}, true
}

// Acquire blocks until userID has a token, ctx is done, or the Limiter is
// closed. ctx must already have come from [Limiter.Enter].
//
// A failure to obtain a token inside the caller's deadline is a [*LimitError]
// carrying the user and the limit in force; [ErrClosed] means the Limiter is
// shutting down.
func (l *Limiter) Acquire(ctx context.Context, userID string) error {
	return l.acquire(ctx, userID)
}

// Allow reports whether one request may proceed right now for userID,
// consuming a token if so. It never waits.
//
// It is not free, which is why it takes a context: a user's first request may
// load their saved state, and with a shared quota configured every request the
// local bucket admits costs a backend call.
func (l *Limiter) Allow(ctx context.Context, userID string) bool {
	return l.allow(ctx, userID)
}

// Wait blocks until a token is available for userID, ctx is done, or the
// Limiter is closed. It consumes the token.
//
// That is inherent rather than an oversight: a Wait cut short by ctx already
// gives its token back, and once Wait has returned there is no signal that
// would tell the engine the caller changed their mind. [Limiter.Reserve] is the
// answer when you want to see the wait before committing to it.
func (l *Limiter) Wait(ctx context.Context, userID string) error {
	ctx, done, ok := l.Enter(ctx)
	if !ok {
		return ErrClosed
	}
	defer done()
	return l.acquire(ctx, userID)
}

// Tokens returns the tokens currently available to userID, and whether that
// user has in-memory state at all. A user who has not been seen, or who has
// been garbage-collected, reports (0, false).
//
// The comma-ok form replaces a -1 sentinel, which could not be told apart from
// a legitimately negative token count.
func (l *Limiter) Tokens(userID string) (float64, bool) { return l.tokens(userID) }

// Evict removes userID from memory, first persisting their token state if a
// store is configured. It reports whether the user had in-memory state, and any
// error from that persistence.
//
// It takes a context because it performs store I/O, and it reports the error
// rather than logging it: the caller asked for this write.
func (l *Limiter) Evict(ctx context.Context, userID string) (bool, error) {
	ctx, done, ok := l.Enter(ctx)
	if !ok {
		return false, ErrClosed
	}
	defer done()
	return l.reg.Evict(ctx, userID)
}

// Quota returns the rate and burst in force for userID.
//
// While the user holds in-memory state this is what their bucket is actually
// enforcing, which can differ from what [github.com/jaeminst/pace/config.Config.QuotaFor] would return now — see
// [Limiter.ReloadQuotas]. Otherwise it is what they would be given on their
// next request. Unlike [Limiter.Tokens] it always has an answer, because a
// quota is configuration rather than state.
func (l *Limiter) Quota(userID string) config.Quota {
	if u, ok := l.reg.Lookup(userID); ok {
		return quotaOf(u)
	}
	return l.cfg.Quota(userID)
}
