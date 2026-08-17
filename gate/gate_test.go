package gate

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestPollDelayNeverWakesEarly: the backend's RetryAfter says when a retry
// could succeed, so jittering below it spends a round-trip to be told the same
// thing.
func TestPollDelayNeverWakesEarly(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, time.Millisecond, time.Second, time.Hour} {
		got := pollDelay(d)
		if d <= 0 {
			if got != 0 {
				t.Errorf("pollDelay(%v) = %v, want 0", d, got)
			}
			continue
		}
		if got < d {
			t.Errorf("pollDelay(%v) = %v, want at least %v", d, got, d)
		}
		if got > d+d/2 {
			t.Errorf("pollDelay(%v) = %v, want at most %v", d, got, d+d/2)
		}
	}
}

// TestSleepHonoursCancellationAtZeroDelay: a polling loop that computes a zero
// delay must still be cancellable. sleep returned nil immediately for d <= 0
// without consulting ctx, so the only thing that could end the loop was a
// backend that eventually granted.
func TestSleepHonoursCancellationAtZeroDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := (&Gate{}).sleep(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("sleep(cancelled ctx, 0) = %v, want context.Canceled", err)
	}
	if err := (&Gate{}).sleep(context.Background(), 0); err != nil {
		t.Errorf("sleep(live ctx, 0) = %v, want nil", err)
	}
}
