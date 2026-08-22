package limiter_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/rate"
)

// TestAbandonedRequestCostsNothing pins where the token is taken. Acquiring it
// when the builder was handed out meant a caller who built a Request and then
// returned early — on a validation failure, a branch, a panic — silently burned
// quota that nothing could give back.
func TestAbandonedRequestCostsNothing(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = rate.PerMinute(6)
		c.Burst = 3
		// A frozen clock, so the comparison below is exact: a live one refills
		// the bucket between readings.
		c.Clock = newFakeClock()
	})
	alice := lim.Client("alice")

	// Force the bucket into existence so Tokens() reports real state.
	if !alice.Allow(context.Background()) {
		t.Fatal("Allow = false on a fresh burst of 3")
	}
	before := tokensOf(alice)

	for range 10 {
		_ = alice.Request().SetHeader("X-Test", "1").SetBody([]byte("body"))
	}

	if after := tokensOf(alice); after != before {
		t.Errorf("10 abandoned Requests moved the token count from %v to %v", before, after)
	}
}

// TestRequestTokenTakenAtSendTime is the other half: the token must still be
// spent when the request actually goes out.
func TestRequestTokenTakenAtSendTime(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = rate.PerMinute(6)
		c.Burst = 3
	})
	alice := lim.Client("alice")
	ctx := context.Background()

	if _, err := alice.Request().Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}
	after := tokensOf(alice)
	if after > 2.01 {
		t.Errorf("Tokens() = %v after one request against a burst of 3, want ~2", after)
	}
}

func TestAllowDoesNotBlockAndConsumes(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = rate.PerMinute(6) // 10s per token: no refill during the test
		c.Burst = 2
	})
	alice := lim.Client("alice")

	if !alice.Allow(context.Background()) {
		t.Fatal("first Allow = false, want true")
	}
	if !alice.Allow(context.Background()) {
		t.Fatal("second Allow = false, want true")
	}
	if alice.Allow(context.Background()) {
		t.Error("third Allow = true after the burst of 2 was spent, want false")
	}
	// A different user is unaffected.
	if !lim.Client("bob").Allow(context.Background()) {
		t.Error("bob's Allow = false, want true")
	}
}

func TestAllowAfterClose(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if lim.Client("alice").Allow(context.Background()) {
		t.Error("Allow = true on a closed Limiter, want false")
	}
}

func TestWaitConsumesToken(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) {
		c.Rate = rate.PerMinute(6)
		c.Burst = 2
	})
	alice := lim.Client("alice")
	ctx := context.Background()

	if err := alice.Wait(ctx); err != nil {
		t.Fatalf("first Wait: %v", err)
	}
	if err := alice.Wait(ctx); err != nil {
		t.Fatalf("second Wait: %v", err)
	}

	// The burst is gone and refill is 10s away, so a short deadline must fail
	// with a LimitError rather than block.
	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	var le *pace.LimitError
	if err := alice.Wait(deadlined); !errors.As(err, &le) {
		t.Errorf("third Wait = %v, want *pace.LimitError", err)
	}
}

func TestWaitAfterClose(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lim.Client("alice").Wait(context.Background()); !errors.Is(err, pace.ErrClosed) {
		t.Errorf("Wait on a closed Limiter = %v, want ErrClosed", err)
	}
}
