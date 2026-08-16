// Package pacetest provides conformance suites for the interfaces pace asks
// callers to implement.
//
// pace ships no [pace.SharedQuota] backend of its own — a Redis or Postgres
// implementation would be a second module to version, tag and support, for a
// feature whose value depends entirely on your infrastructure. What it ships
// instead is the contract, executable:
//
//	func TestMyRedisQuota(t *testing.T) {
//	    pacetest.QuotaSuite(t, func(t *testing.T) pace.SharedQuota {
//	        return myredis.New(startRedis(t))
//	    })
//	}
//
// The suite asserts the properties pace relies on and cannot check at run time.
// A backend that passes is one pace can use correctly; a backend that has never
// been run against it is a guess.
package pacetest

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

// NewQuota builds a backend for one test. Each call must return a backend with
// no state carried over from a previous one — a fresh Redis database, a fresh
// key prefix, whatever isolation the implementation offers. Registering
// cleanup on t is the usual way.
type NewQuota func(t *testing.T) pace.SharedQuota

// QuotaSuite runs every conformance check against backends built by newQuota.
//
// Each check states the property in its failure message, so a failure names the
// guarantee that was broken rather than the assertion that noticed.
func QuotaSuite(t *testing.T, newQuota NewQuota) {
	t.Helper()
	for _, tc := range []struct {
		name string
		fn   func(*testing.T, NewQuota)
	}{
		{"GrantsWithinBurst", quotaGrantsWithinBurst},
		{"RefusesBeyondBurst", quotaRefusesBeyondBurst},
		{"RefusalConsumesNothing", quotaRefusalConsumesNothing},
		{"RetryAfterIsLongEnough", quotaRetryAfterIsLongEnough},
		{"UsersAreIndependent", quotaUsersAreIndependent},
		{"NamespacesAreIndependent", quotaNamespacesAreIndependent},
		{"ConcurrentTakesDoNotOverGrant", quotaConcurrentTakesDoNotOverGrant},
		{"HonoursContextCancellation", quotaHonoursContextCancellation},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newQuota) })
	}
}

// req builds a TakeRequest for one token.
func req(userID string, burst int) pace.TakeRequest {
	return pace.TakeRequest{
		UserID:    userID,
		Namespace: "pacetest",
		Tokens:    1,
		Quota:     pace.Quota{Rate: pace.PerHour(1), Burst: burst},
	}
}

// take is Take with the error folded into a fatal, for the many call sites that
// only care about the grant.
func take(t *testing.T, q pace.SharedQuota, r pace.TakeRequest) pace.Grant {
	t.Helper()
	g, err := q.Take(context.Background(), r)
	if err != nil {
		t.Fatalf("Take(%q) = %v, want nil", r.UserID, err)
	}
	return g
}

// quotaGrantsWithinBurst: a fresh user may spend their whole burst at once.
// That is what burst means, and pace's shadow bucket assumes it.
func quotaGrantsWithinBurst(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	const burst = 5
	for i := range burst {
		if g := take(t, q, req("alice", burst)); !g.OK {
			t.Fatalf("take %d of %d was refused; a fresh user must be able to spend their whole burst",
				i+1, burst)
		}
	}
}

// quotaRefusesBeyondBurst: the limit has to actually bind. A backend that
// always grants passes nothing else here by accident.
func quotaRefusesBeyondBurst(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	const burst = 3
	for range burst {
		take(t, q, req("alice", burst))
	}
	if g := take(t, q, req("alice", burst)); g.OK {
		t.Errorf("take %d was granted against a burst of %d; the limit must bind", burst+1, burst)
	}
}

// quotaRefusalConsumesNothing is the property pace states in the SharedQuota
// doc and cannot verify at run time. If a refused Take still consumed, a
// throttled user would be pushed further from recovery by the very calls
// checking whether they had recovered.
func quotaRefusalConsumesNothing(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	const burst = 2
	for range burst {
		take(t, q, req("alice", burst))
	}

	// Hammer the exhausted user. If refusals consumed, the debt would grow.
	for range 20 {
		if g := take(t, q, req("alice", burst)); g.OK {
			t.Fatal("a take was granted while the burst was exhausted")
		}
	}

	// A single burst's worth of refill must still restore a single token, which
	// it cannot if the refusals above were charged.
	g := take(t, q, req("alice", burst))
	if g.OK {
		return // the backend refilled during the test; nothing was over-charged
	}
	if g.RetryAfter <= 0 {
		return // the backend does not report a schedule; nothing more to check
	}
	if g.RetryAfter > 2*time.Hour {
		t.Errorf("RetryAfter = %v after 20 refused takes at one token per hour, want at most ~1h: "+
			"refusals appear to be consuming tokens", g.RetryAfter)
	}
}

// quotaRetryAfterIsLongEnough: pace sleeps for RetryAfter and then retries. A
// value that is too short turns one throttled request into a hot loop against
// the backend.
func quotaRetryAfterIsLongEnough(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	// A fast refill, so the test can actually wait it out.
	r := pace.TakeRequest{
		UserID:    "alice",
		Namespace: "pacetest",
		Tokens:    1,
		Quota:     pace.Quota{Rate: pace.PerSecond(20), Burst: 1},
	}
	take(t, q, r)

	g := take(t, q, r)
	if g.OK {
		t.Skip("the backend refilled before the second take; nothing to measure")
	}
	if g.RetryAfter <= 0 {
		t.Skip("the backend does not report RetryAfter")
	}
	if g.RetryAfter > time.Minute {
		t.Fatalf("RetryAfter = %v at 20 tokens per second, want well under a minute", g.RetryAfter)
	}

	time.Sleep(g.RetryAfter)
	if g := take(t, q, r); !g.OK {
		t.Errorf("a take after waiting the reported RetryAfter was still refused; "+
			"RetryAfter must be long enough that a retry can succeed (reported %v)", g.RetryAfter)
	}
}

// quotaUsersAreIndependent: one user exhausting their quota must not throttle
// another. This is the whole premise of pace.
func quotaUsersAreIndependent(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	const burst = 2
	for range burst + 3 {
		take(t, q, req("alice", burst))
	}
	if g := take(t, q, req("bob", burst)); !g.OK {
		t.Error("bob was refused after alice exhausted her quota; users must be independent")
	}
}

// quotaNamespacesAreIndependent: Config.QuotaNamespace exists so that several
// Limiters can share one backend. If it is ignored, they silently share a
// budget instead.
func quotaNamespacesAreIndependent(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	const burst = 2
	a := req("alice", burst)
	b := req("alice", burst)
	b.Namespace = "pacetest-other"

	for range burst + 3 {
		take(t, q, a)
	}
	if g := take(t, q, b); !g.OK {
		t.Error("a take in a second namespace was refused after the first was exhausted; " +
			"Namespace must partition the budget")
	}
}

// quotaConcurrentTakesDoNotOverGrant is the property that cannot be checked
// serially, and the one a client-side read-then-write implementation fails.
func quotaConcurrentTakesDoNotOverGrant(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	const (
		burst  = 10
		racers = 64
	)

	var granted int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range racers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			g, err := q.Take(context.Background(), req("alice", burst))
			if err != nil {
				return
			}
			if g.OK {
				mu.Lock()
				granted++
				mu.Unlock()
			}
		}()
	}
	close(start)
	wg.Wait()

	// One token per hour, so refill during the test is not a plausible
	// explanation for anything above the burst.
	if granted > burst {
		t.Errorf("%d of %d concurrent takes were granted against a burst of %d; "+
			"Take must be atomic at the backend", granted, racers, burst)
	}
	if granted == 0 {
		t.Errorf("none of %d concurrent takes were granted; the backend refused a fresh user", racers)
	}
}

// quotaHonoursContextCancellation: pace bounds every call with
// Config.QuotaTimeout. A backend that ignores the context turns that bound into
// a suggestion, and a slow backend then stalls every request.
func quotaHonoursContextCancellation(t *testing.T, newQuota NewQuota) {
	t.Helper()
	q := newQuota(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = q.Take(ctx, req("alice", 5))
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Take did not return with an already-cancelled context; " +
			"the backend must honour ctx so QuotaTimeout can bound it")
	}
}
