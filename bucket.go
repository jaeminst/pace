package pace

import (
	"context"
	"math"
	"time"

	"golang.org/x/time/rate"
)

// bucket wraps a [rate.Limiter] with a wait method that honours both the
// caller's context and the manager's lifetime context.
type bucket struct {
	limiter *rate.Limiter
}

func newBucket(ratePerMinute, burst int) *bucket {
	r := rate.Every(time.Minute / time.Duration(ratePerMinute))
	return &bucket{limiter: rate.NewLimiter(r, burst)}
}

// restoreBucket creates a bucket whose token count reflects savedTokens
// accumulated from savedAt up to now, capped at burst.
func restoreBucket(ratePerMinute, burst int, savedTokens float64, savedAt time.Time) *bucket {
	ratePerSec := float64(ratePerMinute) / 60.0
	elapsed := time.Since(savedAt).Seconds()
	restoredTokens := math.Min(float64(burst), savedTokens+elapsed*ratePerSec)

	r := rate.Every(time.Minute / time.Duration(ratePerMinute))
	l := rate.NewLimiter(r, burst)
	// Fresh limiter starts at burst; drain the excess to reach restoredTokens.
	if drain := int(math.Round(float64(burst) - restoredTokens)); drain > 0 && drain <= burst {
		l.ReserveN(time.Now(), drain)
	}
	return &bucket{limiter: l}
}

func (b *bucket) tokens() float64 {
	return b.limiter.Tokens()
}

// wait blocks until one token is available, the caller's context is done, or
// the manager is shut down (managerCtx).
//
// context.AfterFunc is used instead of a manually spawned goroutine so that
// no goroutine is created in the common case (manager still alive).
func (b *bucket) wait(ctx, managerCtx context.Context) error {
	merged, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(managerCtx, cancel)
	defer stop()
	if err := b.limiter.Wait(merged); err != nil {
		if ctx.Err() == nil {
			// merged was cancelled by managerCtx, not by the caller
			return ErrClosed
		}
		return err
	}
	return nil
}
