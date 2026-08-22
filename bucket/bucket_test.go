package bucket

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"
)

// epsilon is the tolerance for token comparisons. Restore is exact arithmetic,
// so this only absorbs float64 representation error, not rounding.
const epsilon = 1e-9

var origin = time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)

func TestNewBucketStartsFull(t *testing.T) {
	for _, burst := range []int{1, 10, 1000} {
		b := NewBucket(Quota{Rate: 1, Burst: burst}) // 1 token/sec
		if got := b.TokensAt(origin); math.Abs(got-float64(burst)) > epsilon {
			t.Errorf("burst %d: TokensAt = %v, want %v", burst, got, burst)
		}
	}
}

// TestRestoreBucketExact is the test that fractional state depends on. An
// implementation that rounds the restored token count to a whole number — as
// draining via an integer ReserveN argument does — fails every fractional case
// here while still reporting full statement coverage of RestoreBucket.
func TestRestoreBucketExact(t *testing.T) {
	const perSec = 1.0 // elapsed seconds == tokens accrued

	tests := []struct {
		name        string
		burst       int
		savedTokens float64
		elapsed     time.Duration
		want        float64
	}{
		{"empty, no time passed", 10, 0, 0, 0},
		{"fractional, no time passed", 10, 0.5, 0, 0.5},
		{"fractional, no time passed, other", 10, 2.7, 0, 2.7},
		{"full, no time passed", 10, 10, 0, 10},
		{"fractional plus fractional refill", 10, 2.7, 1500 * time.Millisecond, 4.2},
		{"half refill", 10, 0, 5 * time.Second, 5},
		{"refill to exactly full", 10, 5, 5 * time.Second, 10},
		{"refill past full clamps to burst", 10, 5, 30 * time.Second, 10},
		{"saved above burst clamps to burst", 10, 25, 0, 10},
		{"negative saved clamps to zero", 10, -1, 0, 0},
		{"negative saved still refills", 10, -1, 3 * time.Second, 2},
		{"burst of one, fractional", 1, 0.25, 0, 0.25},
		{"burst of one, saturates", 1, 0.25, 10 * time.Second, 1},
		{"elapsed far exceeds burst", 10, 0, 10 * time.Minute, 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			savedAt := origin.Add(-tt.elapsed)
			b := RestoreBucket(Quota{Rate: Limit(perSec), Burst: tt.burst}, tt.savedTokens, savedAt, origin)
			if got := b.TokensAt(origin); math.Abs(got-tt.want) > epsilon {
				t.Errorf("TokensAt(now) = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRestoreBucketSavedAtInFuture(t *testing.T) {
	// Clock skew, or a fake clock wound backwards. Elapsed time must clamp to
	// zero rather than subtracting credit the key never spent.
	savedAt := origin.Add(time.Hour)
	b := RestoreBucket(Quota{Rate: 1, Burst: 10}, 4.5, savedAt, origin)
	if got := b.TokensAt(origin); math.Abs(got-4.5) > epsilon {
		t.Errorf("TokensAt(now) = %v, want 4.5", got)
	}
}

func TestRestoreBucketCorruptedState(t *testing.T) {
	// A REAL column can hand back these after a truncated write or hand edit.
	// Granting no credit is the safe direction for a throttle.
	for _, savedTokens := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		b := RestoreBucket(Quota{Rate: 1, Burst: 10}, savedTokens, origin, origin)
		got := b.TokensAt(origin)
		if math.IsNaN(got) {
			t.Fatalf("savedTokens=%v produced NaN tokens", savedTokens)
		}
		if got < 0 || got > 10 {
			t.Errorf("savedTokens=%v: TokensAt = %v, want within [0, 10]", savedTokens, got)
		}
	}
}

func TestRestoreBucketSlowRateDoesNotOverflow(t *testing.T) {
	// tokens/perSec here exceeds what a time.Duration can express, so the
	// drain instant must clamp instead of wrapping into the future.
	b := RestoreBucket(Quota{Rate: Limit(1.0 / 60.0), Burst: math.MaxInt32}, math.MaxInt32, origin, origin)
	got := b.TokensAt(origin)
	if got < 0 || got > math.MaxInt32 {
		t.Errorf("TokensAt = %v, want within [0, %v]", got, math.MaxInt32)
	}
}

func TestRestoreBucketThenConsume(t *testing.T) {
	// Restored fractional state must behave like earned state: 2.7 tokens
	// allows two immediate events and refuses the third.
	b := RestoreBucket(Quota{Rate: 1, Burst: 10}, 2.7, origin, origin)
	for i := range 2 {
		if !b.AllowAt(origin) {
			t.Fatalf("event %d: AllowAt = false, want true", i)
		}
	}
	if b.AllowAt(origin) {
		t.Error("AllowAt = true after draining to 0.7, want false")
	}
	if got := b.TokensAt(origin); math.Abs(got-0.7) > epsilon {
		t.Errorf("TokensAt = %v, want 0.7", got)
	}
}

// TestAllowAtNeedsAWholeToken: a fractional balance below one is not enough,
// and the shortfall is made up by refilling rather than rounded away.
func TestAllowAtNeedsAWholeToken(t *testing.T) {
	b := RestoreBucket(Quota{Rate: 1, Burst: 10}, 0.999, origin, origin)
	if b.AllowAt(origin) {
		t.Error("AllowAt = true with 0.999 tokens, want false")
	}
	// One more second of refill crosses the threshold.
	if !b.AllowAt(origin.Add(time.Second)) {
		t.Error("AllowAt = false one second later, want true")
	}
}

// TestSetQuotaAtKeepsAccruedTokens covers what ReloadQuotas relies on: raising
// or lowering a key's quota must not reset what they have already earned.
func TestSetQuotaAtKeepsAccruedTokens(t *testing.T) {
	b := RestoreBucket(Quota{Rate: 1, Burst: 10}, 4, origin, origin)

	b.SetQuotaAt(origin, Quota{Rate: 5, Burst: 20})
	if got := b.TokensAt(origin); math.Abs(got-4) > epsilon {
		t.Errorf("TokensAt after raising the quota = %v, want the 4 already accrued", got)
	}
	if q := b.Quota(); q.Rate != 5 || q.Burst != 20 {
		t.Errorf("Quota = %+v, want {Rate:5 Burst:20}", q)
	}

	// Lowering the ceiling below the balance clamps it: the ceiling is what the
	// bucket may hold.
	b.SetQuotaAt(origin, Quota{Rate: 5, Burst: 2})
	if got := b.TokensAt(origin); got > 2+epsilon {
		t.Errorf("TokensAt after lowering burst to 2 = %v, want at most 2", got)
	}
}

func TestWaitReturnsWhenTokenAvailable(t *testing.T) {
	b := NewBucket(Quota{Rate: 1_000, Burst: 1})
	if err := b.Wait(context.Background()); err != nil {
		t.Errorf("Wait = %v, want nil", err)
	}
}

func TestWaitCancelledByCallerContext(t *testing.T) {
	// One token per hour, burst already spent: the caller's context is the
	// only thing that can end this wait.
	b := NewBucket(Quota{Rate: Limit(1.0 / 3600.0), Burst: 1}) // one token per hour
	if !b.limiter.AllowN(time.Now(), 1) {
		t.Fatal("could not drain the initial burst")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := b.Wait(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Wait = %v, want context.Canceled", err)
	}
}

func TestWaitCancelledWhileBlocked(t *testing.T) {
	// Cancellation arrives after Wait is already blocked, rather than on the
	// already-cancelled fast path. Merging the owning limiter's lifetime into
	// ctx is the caller's job, so from the bucket's side both look the same.
	b := NewBucket(Quota{Rate: Limit(1.0 / 3600.0), Burst: 1}) // one token per hour
	if !b.limiter.AllowN(time.Now(), 1) {
		t.Fatal("could not drain the initial burst")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- b.Wait(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Wait = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return after cancellation")
	}
}

// TestFiniteRejectsWhatRateLimiterCannotHold: rate.Limiter's own Inf is
// math.MaxFloat64, not a real infinity, and handing it a genuine one poisons
// every token count downstream into NaN. Found by fuzzing RestoreBucket.
func TestFiniteRejectsWhatRateLimiterCannotHold(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   float64
		want float64
	}{
		{"NaN becomes no refill", math.NaN(), 0},
		{"positive infinity becomes the largest representable rate", math.Inf(1), math.MaxFloat64},
		{"negative infinity becomes no refill", math.Inf(-1), 0},
		{"a finite rate is untouched", 1.5, 1.5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := usableRate(tt.in); got != tt.want {
				t.Errorf("usableRate(%v) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}

	// And the constructors it guards must produce a usable bucket regardless.
	for _, perSec := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		b := NewBucket(Quota{Rate: Limit(perSec), Burst: 5})
		if got := b.TokensAt(origin); math.IsNaN(got) {
			t.Errorf("NewBucket(Quota{Rate: Limit(%v), Burst: 5}).TokensAt = NaN", perSec)
		}
		r := RestoreBucket(Quota{Rate: Limit(perSec), Burst: 5}, 2, origin, origin)
		if got := r.TokensAt(origin); math.IsNaN(got) {
			t.Errorf("RestoreBucket(%v, …).TokensAt = NaN", perSec)
		}
	}
}

// TestQuotaIsOnePair: the rate and the ceiling must always be a pair somebody
// configured.
//
// rate.Limiter reports them through two separately locked methods, so reading
// both gave combinations that never existed — and that pair is what pace reports
// in LimitError, ThrottleInfo, Client.Quota and shared.TakeRequest. A backend
// sizing its bucket from TakeRequest could be handed a quota nobody set.
//
// -race cannot find this. Both reads are properly synchronised on their own; it
// is the composition that is wrong. So the assertion has to name the legal pairs
// and reject everything else.
func TestQuotaIsOnePair(t *testing.T) {
	b := NewBucket(Quota{Rate: 1, Burst: 1})

	const rounds = 20000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range rounds {
			if i%2 == 0 {
				b.SetQuotaAt(origin, Quota{Rate: 1, Burst: 1})
			} else {
				b.SetQuotaAt(origin, Quota{Rate: 100, Burst: 50})
			}
		}
	}()

	for range rounds {
		switch q := b.Quota(); {
		case q.Rate == 1 && q.Burst == 1:
		case q.Rate == 100 && q.Burst == 50:
		default:
			t.Fatalf("Quota = %+v; neither pair was ever configured", q)
		}
	}
	<-done
}
