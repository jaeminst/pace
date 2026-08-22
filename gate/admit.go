package gate

import (
	"context"
	"fmt"
	"time"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/shared"
)

// Allow is the non-blocking decision: it reports whether the request may
// proceed, and when it may not, how long the caller would have to wait and how
// many tokens the backend says remain.
//
// It returns those rather than reporting them itself because on this path
// nothing is waited for, so the owner can report after the fact with everything
// in hand. [Gate.Acquire] is the one that cannot.
//
// The local bucket is consulted first, as a shadow, and the reservation it
// produces is cancelled unless the backend also grants. Two properties make
// that sound:
//
//   - The shadow and the shared bucket are configured with the same rate and
//     burst, and this replica's consumption is a subset of the fleet's, so the
//     shadow always holds at least as many tokens as the shared bucket does. A
//     shadow that refuses therefore proves the backend would have refused too,
//     which is what makes skipping the round-trip safe. The quota is read off
//     the shadow rather than passed in, so the two cannot describe different
//     limits — which they did until v0.13.0, whenever a quota changed while a
//     request was in flight.
//   - The converse does not hold, so a shadow that grants proves nothing and
//     the backend still has to be asked.
//
// The cancel on refusal is not tidiness. Consuming the shadow for a request the
// backend rejected breaks the inequality in the dangerous direction: a replica
// that keeps losing the race would ratchet its own shadow down to zero and stop
// asking, while the shared quota still had room for it.
func (g *Gate) Allow(
	ctx context.Context, userID string, b *bucket.Bucket, now time.Time,
) (ok bool, delay time.Duration, tokens *float64) {
	res := b.ReserveAt(now)
	if !res.OK() || res.DelayFrom(now) > 0 {
		res.CancelAt(now)
		return false, b.DelayAt(now), nil
	}

	rateLimit, burst := b.Quota()
	grant, granted, err := g.Take(ctx, userID, rateLimit, burst)
	if granted && err == nil {
		return true, 0, nil
	}
	// Cancelled at the instant the token was taken, not at "now". CancelAt
	// refuses to refund once the reservation's time to act has passed, and with
	// a real clock even a nanosecond of drift during the backend call would be
	// enough — which would make the refund depend on clock granularity rather
	// than on the decision.
	res.CancelAt(now)

	// The cancel above put the shadow token back, so the bucket's own DelayAt
	// would report zero and tell the caller a refused request needs no wait.
	return false, RetryDelay(grant, rateLimit), grant.Tokens
}

// Acquire blocks until the backend admits the request, the caller's context is
// done, or the failure policy decides.
//
// Each round reserves against the local shadow, waits out whatever delay that
// reservation implies, and only then spends a backend round-trip. The shadow
// argument from [Gate.Allow] applies unchanged: waiting locally costs the
// backend nothing and cannot admit a request the backend would refuse.
//
// A refusal from the backend cancels the shadow reservation, for the reason
// Allow gives: a replica that consumed locally for requests it was refused
// would ratchet itself below its own share while the shared quota still had
// room.
//
// Spec.Throttled fires at most once per request rather than once per round,
// so a long wait is not reported as a burst of throttles.
//
// An error the owner should report as its own throttle is wrapped in
// [*WaitError]; anything else is returned as it is.
func (g *Gate) Acquire(ctx context.Context, userID string, b *bucket.Bucket) error {
	// Before the closure below, which that path allocates and never calls.
	if waiter, canWait := g.cfg.Backend.(shared.Waiter); canWait {
		return g.wait(ctx, waiter, userID, b)
	}

	reported := false
	report := func(delay time.Duration, tokens *float64) {
		if reported {
			return
		}
		reported = true
		g.cfg.Throttled(ctx, userID, b, delay, g.cfg.Now(), tokens)
	}

	for {
		// Per round, not per call. This loop can run for minutes, and the
		// shadow below already reflects a quota changed underneath it — asking
		// the backend about the quota this request started with would tell it
		// to size a bucket that no longer exists.
		rateLimit, burst := b.Quota()

		now := g.cfg.Now()
		res := b.ReserveAt(now)
		if !res.OK() {
			return &WaitError{Cause: errUnsatisfiable}
		}
		if delay := res.DelayFrom(now); delay > 0 {
			report(delay, nil)
			g.cfg.BeforeWait()
			if err := g.sleep(ctx, delay); err != nil {
				res.CancelAt(now)
				return &WaitError{Cause: err}
			}
		}

		grant, ok, err := g.Take(ctx, userID, rateLimit, burst)
		if err != nil {
			res.CancelAt(now)
			return err
		}
		if ok {
			return nil
		}
		res.CancelAt(now)

		delay := RetryDelay(grant, rateLimit)
		report(delay, grant.Tokens)
		if err := g.sleep(ctx, pollDelay(delay)); err != nil {
			return &WaitError{Cause: err}
		}
	}
}

// wait uses a backend that can park a waiter, so pace does not poll.
//
// While the backend is answering, the local shadow is not consulted: the
// backend has taken responsibility for the wait, and gating it locally as well
// would delay a caller the backend was ready to serve.
//
// When the backend cannot answer, that stops being true, and the failure policy
// has to mean the same thing here as everywhere else. It used to return nil on
// every failure path, which made shared.FallbackLocal — the default, and the
// conservative one — admit without limit: a backend that goes down opened the
// breaker after five failures and then served every user instantly for the
// whole cooldown. The fallback now does what its name says and waits on this
// replica's own bucket.
func (g *Gate) wait(
	ctx context.Context, w shared.Waiter, userID string, b *bucket.Bucket,
) error {
	if !g.breaker.Allow(g.cfg.Now()) {
		g.failures.Add(1)
		return g.waitFallback(ctx, b, errBreakerOpen)
	}

	// No throttle is reported on this path, and that is a known gap rather than
	// an oversight. The backend owns the wait, so pace cannot know in advance
	// whether this caller will be parked or served instantly — and
	// Observer.Throttled is documented as firing *before* the wait, with the
	// expected delay. Firing it unconditionally, which is what this used to do,
	// made Stats.Throttled equal Stats.Requests on every waiting deployment;
	// inventing a delay after the fact would be no better. Under-counting a
	// case pace genuinely cannot observe beats reporting a number that is wrong.
	g.cfg.BeforeWait()
	g.cfg.BeforeQuotaTake()
	g.takes.Add(1)

	// Read once, here: this is one blocking call the backend owns, so a quota
	// changed while the caller is parked cannot be picked up. The caller
	// finishes under the quota in force when it arrived. Unlike the polling
	// path above there is no round to re-read on.
	rateLimit, burst := b.Quota()
	err := w.Wait(ctx, g.request(userID, rateLimit, burst))
	switch {
	case err == nil:
		g.breaker.Succeeded()
		return nil
	case g.ctx.Err() != nil:
		return g.cfg.Closed
	case ctx.Err() != nil:
		// The caller gave up; that is not the backend's failure.
		g.breaker.Abandoned()
		return ctx.Err()
	}

	g.failures.Add(1)
	g.breaker.Failed(g.cfg.Now())
	g.cfg.Logger.Warn("pace: shared quota wait", "user", userID, "err", err)
	return g.waitFallback(ctx, b, err)
}

// waitFallback applies the failure policy on the waiting path, where there is
// no shadow reservation already in hand.
func (g *Gate) waitFallback(ctx context.Context, b *bucket.Bucket, cause error) error {
	switch g.cfg.OnError {
	case shared.Deny:
		return fmt.Errorf("%w: %w", shared.ErrUnavailable, cause)
	case shared.Allow:
		return nil
	default: // shared.FallbackLocal
		// Enforce the configured rate for this replica, which is what the
		// polling path gets for free by reserving against the shadow first.
		g.cfg.BeforeWait()
		if err := b.Wait(ctx); err != nil {
			return &WaitError{Cause: err}
		}
		return nil
	}
}

// sleep waits for d, or until ctx is done.
//
// A non-positive d still consults ctx. It used to return nil outright, which
// made a polling loop that computed a zero delay uncancellable: nothing in the
// loop except the backend itself could then notice the caller had given up.
func (g *Gate) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
