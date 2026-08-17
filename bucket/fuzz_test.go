package bucket

import (
	"math"
	"testing"
	"time"
)

// FuzzRestoreBucket asserts the one invariant every caller depends on:
// whatever a store hands back, the restored bucket holds between zero and burst
// tokens.
//
// RestoreBucket already special-cases NaN, infinities and overflow, which is
// exactly why it is worth fuzzing — the code claims to handle hostile input and
// the tests only ever checked three values somebody thought of. A store can
// return anything: a hand-edited row, a truncated write, a float that
// round-tripped through a REAL column, a clock that went backwards.
func FuzzRestoreBucket(f *testing.F) {
	f.Add(1.0, 10, 5.0, int64(0), int64(0))
	f.Add(0.0, 1, 0.0, int64(0), int64(0))
	f.Add(1.0/60.0, math.MaxInt32, float64(math.MaxInt32), int64(0), int64(0))
	f.Add(1.0, 10, math.NaN(), int64(0), int64(1e9))
	f.Add(1.0, 10, math.Inf(1), int64(0), int64(1e9))
	f.Add(1.0, 10, -5.0, int64(1e18), int64(0)) // savedAt in the future
	f.Add(math.MaxFloat64, 1, 1.0, int64(0), int64(1e18))

	f.Fuzz(func(t *testing.T, perSec float64, burst int, savedTokens float64, savedAtNanos, nowNanos int64) {
		// Config.validate rejects a non-positive rate, and withDefaults raises a
		// non-positive burst to one, so those combinations cannot reach here.
		if !(perSec > 0) || burst <= 0 {
			t.Skip()
		}
		// A burst beyond int32 is not something pace can be configured into
		// either, and x/time/rate's own arithmetic is not defined for it.
		if burst > math.MaxInt32 {
			t.Skip()
		}

		savedAt := time.Unix(0, savedAtNanos)
		now := time.Unix(0, nowNanos)

		b := RestoreBucket(perSec, burst, savedTokens, savedAt, now)
		got := b.TokensAt(now)

		if math.IsNaN(got) {
			t.Fatalf("TokensAt = NaN for perSec=%v burst=%d saved=%v", perSec, burst, savedTokens)
		}
		if got < 0 {
			t.Errorf("TokensAt = %v, want at least 0 (perSec=%v burst=%d saved=%v)",
				got, perSec, burst, savedTokens)
		}
		// Allow a hair of slack: the drain-and-refill reconstruction is
		// floating point, so exact equality at the ceiling is not guaranteed.
		if got > float64(burst)+1e-6 {
			t.Errorf("TokensAt = %v, want at most the burst of %d (perSec=%v saved=%v)",
				got, burst, perSec, savedTokens)
		}
	})
}

// FuzzDrainInstant checks the helper RestoreBucket's correctness rests on: the
// instant it returns must never be in the future, or the bucket would be
// restored holding more than it was saved with.
func FuzzDrainInstant(f *testing.F) {
	f.Add(1.0, 5.0, int64(0))
	f.Add(0.0, 5.0, int64(0))
	f.Add(1e-300, 1e300, int64(0))
	f.Add(math.MaxFloat64, math.MaxFloat64, int64(1e18))

	f.Fuzz(func(t *testing.T, perSec, tokens float64, nowNanos int64) {
		if math.IsNaN(perSec) || math.IsNaN(tokens) {
			t.Skip()
		}
		now := time.Unix(0, nowNanos)
		got := drainInstant(now, tokens, perSec)
		if got.After(now) {
			t.Errorf("drainInstant = %v, which is after now (%v): tokens=%v perSec=%v",
				got, now, tokens, perSec)
		}
	})
}
