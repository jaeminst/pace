package breaker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var t0 = time.Unix(1_700_000_000, 0)

// trip drives b to open at t0 and returns the instant it stays open until.
func trip(b *Breaker) time.Time {
	for range Threshold {
		b.Failed(t0)
	}
	return t0.Add(Cooldown)
}

func TestTheZeroValueIsClosed(t *testing.T) {
	var b Breaker
	if !b.Allow(t0) {
		t.Fatal("a Breaker that has seen nothing refused a call")
	}
}

func TestItTakesThresholdFailuresToOpen(t *testing.T) {
	var b Breaker
	for i := range Threshold - 1 {
		b.Failed(t0)
		if !b.Allow(t0) {
			t.Fatalf("opened after %d failures, want %d", i+1, Threshold)
		}
	}
	b.Failed(t0)
	if b.Allow(t0) {
		t.Fatalf("still closed after %d failures", Threshold)
	}
}

func TestASuccessResetsTheCount(t *testing.T) {
	var b Breaker
	for range Threshold - 1 {
		b.Failed(t0)
	}
	b.Succeeded()
	for range Threshold - 1 {
		b.Failed(t0)
	}
	if !b.Allow(t0) {
		t.Fatal("opened on failures that spanned a success; the count did not reset")
	}
}

// TestOnlyOneCallerGetsTheProbe is the half-open state, and the reason this
// package exists separately: reaching it through a live backend costs a real
// timeout per transition, so the behaviour went untested until the cooldown
// could be crossed by arithmetic.
func TestOnlyOneCallerGetsTheProbe(t *testing.T) {
	var b Breaker
	after := trip(&b)

	if b.Allow(after.Add(-time.Nanosecond)) {
		t.Fatal("let a call through a nanosecond before the cooldown elapsed")
	}
	if !b.Allow(after) {
		t.Fatal("refused the probe once the cooldown had elapsed")
	}
	for i := range 10 {
		if b.Allow(after) {
			t.Fatalf("caller %d also got through; the probe is meant to be exactly one", i+2)
		}
	}
}

// TestAFailedProbeReopensOnItsOwn pins the half of the recovery path that a
// threshold-only breaker gets wrong: a probe that fails is a verdict about a
// dependency already known to be sick, so it must not need Threshold more
// failures to re-open. Requiring them let the whole backlog through every
// cooldown, each paying a timeout to rediscover the same thing.
func TestAFailedProbeReopensOnItsOwn(t *testing.T) {
	var b Breaker
	after := trip(&b)

	if !b.Allow(after) {
		t.Fatal("no probe was offered")
	}
	b.Failed(after)

	if b.Allow(after) {
		t.Fatal("the failed probe did not re-open the breaker")
	}
	if b.Allow(after.Add(Cooldown - time.Nanosecond)) {
		t.Fatal("re-opened for less than a full cooldown")
	}
	if !b.Allow(after.Add(Cooldown)) {
		t.Fatal("did not offer a second probe after the second cooldown")
	}
}

func TestASucceedingProbeClosesTheBreaker(t *testing.T) {
	var b Breaker
	after := trip(&b)

	if !b.Allow(after) {
		t.Fatal("no probe was offered")
	}
	b.Succeeded()

	for i := range 3 {
		if !b.Allow(after) {
			t.Fatalf("call %d refused after a successful probe; the breaker did not close", i+1)
		}
	}
}

// TestAnAbandonedProbeIsNeitherVerdictNorDeadlock covers the case where the
// caller went away before the dependency answered. That says nothing about the
// dependency, so it must not count against it — but the probe must still be
// released, or half-open never resolves and the breaker stays shut forever.
func TestAnAbandonedProbeIsNeitherVerdictNorDeadlock(t *testing.T) {
	var b Breaker
	after := trip(&b)

	if !b.Allow(after) {
		t.Fatal("no probe was offered")
	}
	b.Abandoned()

	if !b.Allow(after) {
		t.Fatal("the abandoned probe was never released; the breaker is stuck half-open")
	}
	// Still half-open rather than closed: nothing was learned.
	if b.Allow(after) {
		t.Fatal("abandoning a probe closed the breaker, which is a verdict it cannot support")
	}
}

func TestConcurrentCallersGetOneProbe(t *testing.T) {
	var b Breaker
	after := trip(&b)

	const callers = 64
	var (
		wg      sync.WaitGroup
		allowed atomic.Int64
	)
	start := make(chan struct{})
	for range callers {
		wg.Go(func() {
			<-start
			if b.Allow(after) {
				allowed.Add(1)
			}
		})
	}
	close(start)
	wg.Wait()

	if got := allowed.Load(); got != 1 {
		t.Fatalf("%d of %d concurrent callers got through half-open, want exactly 1", got, callers)
	}
}
