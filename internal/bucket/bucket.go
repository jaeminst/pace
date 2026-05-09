package bucket

import (
	"context"
	"math"
	"time"

	"golang.org/x/time/rate"
)

// Bucket wraps a [rate.Limiter] with a Wait method that honours both the
// caller's context and the manager's lifetime context.
type Bucket struct {
	limiter *rate.Limiter
}

// NewBucket creates a Bucket that allows ratePerMinute events per minute with
// the given burst ceiling.
func NewBucket(ratePerMinute, burst int) *Bucket {
	r := rate.Every(time.Minute / time.Duration(ratePerMinute))
	return &Bucket{limiter: rate.NewLimiter(r, burst)}
}

// RestoreBucket creates a Bucket whose token count reflects savedTokens
// accumulated from savedAt up to now, capped at burst.
func RestoreBucket(ratePerMinute, burst int, savedTokens float64, savedAt time.Time) *Bucket {
	ratePerSec := float64(ratePerMinute) / 60.0
	elapsed := time.Since(savedAt).Seconds()
	restoredTokens := math.Min(float64(burst), savedTokens+elapsed*ratePerSec)

	r := rate.Every(time.Minute / time.Duration(ratePerMinute))
	l := rate.NewLimiter(r, burst)
	// Fresh limiter starts at burst; drain the excess to reach restoredTokens.
	if drain := int(math.Round(float64(burst) - restoredTokens)); drain > 0 && drain <= burst {
		l.ReserveN(time.Now(), drain)
	}
	return &Bucket{limiter: l}
}

// Tokens returns the current number of available tokens.
func (b *Bucket) Tokens() float64 {
	return b.limiter.Tokens()
}

// Wait blocks until one token is available, the caller's context is done, or
// the manager is shut down (managerCtx).
//
// context.AfterFunc is used instead of a manually spawned goroutine so that
// no goroutine is created in the common case (manager still alive).
func (b *Bucket) Wait(ctx, managerCtx context.Context) error {
	merged, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(managerCtx, cancel)
	defer stop()
	return b.limiter.Wait(merged)
}
