package pace_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/internal/breaker"
	"github.com/jaeminst/pace/pacetest"
)

// gcraQuota is a correct SharedQuota that happens to live in memory. It stands
// in for the Redis backend pace deliberately does not ship: the point of these
// tests is the integration and the conformance suite, neither of which needs a
// network to exercise.
//
// A real backend replaces the mutex with the database's own atomicity and the
// clock with the server's. Everything else is the same arithmetic.
type gcraQuota struct {
	mu     sync.Mutex
	now    func() time.Time
	tokens map[string]float64
	seen   map[string]time.Time
	takes  int
}

func newGCRAQuota(now func() time.Time) *gcraQuota {
	return &gcraQuota{
		now:    now,
		tokens: map[string]float64{},
		seen:   map[string]time.Time{},
	}
}

func (q *gcraQuota) Take(ctx context.Context, r pace.TakeRequest) (pace.Grant, error) {
	if err := ctx.Err(); err != nil {
		return pace.Grant{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.takes++

	key := r.Namespace + "\x00" + r.UserID
	now := q.now()
	perSec := float64(r.Quota.Rate)
	burst := float64(r.Quota.Burst)

	last, ok := q.seen[key]
	if !ok {
		q.tokens[key] = burst
	} else {
		q.tokens[key] = min(burst, q.tokens[key]+now.Sub(last).Seconds()*perSec)
	}
	q.seen[key] = now

	want := float64(r.Tokens)
	if q.tokens[key] < want {
		// Consume nothing on refusal, as the contract requires.
		short := want - q.tokens[key]
		var after time.Duration
		if perSec > 0 {
			after = time.Duration(short / perSec * float64(time.Second))
		}
		left := q.tokens[key]
		return pace.Grant{RetryAfter: after, Tokens: &left}, nil
	}
	q.tokens[key] -= want
	left := q.tokens[key]
	return pace.Grant{OK: true, Tokens: &left}, nil
}

func (q *gcraQuota) takeCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.takes
}

// TestGCRAQuotaPassesTheConformanceSuite runs the suite pace publishes for
// backend authors against a backend that is known to be correct. Without this
// the suite is untested code shipped as a testing tool.
func TestGCRAQuotaPassesTheConformanceSuite(t *testing.T) {
	pacetest.QuotaSuite(t, func(*testing.T) pace.SharedQuota {
		return newGCRAQuota(time.Now)
	})
}

// failingQuota returns err from every Take.
type failingQuota struct {
	err   error
	calls int
	mu    sync.Mutex
}

func (q *failingQuota) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	return pace.Grant{}, q.err
}

func (q *failingQuota) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

func sharedLimiter(t *testing.T, q pace.SharedQuota, opts ...func(*pace.Config)) *pace.Limiter {
	t.Helper()
	cfg := pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerSecond(1000),
		Burst:   100,
		Shared:  pace.SharedConfig{Quota: q},
	}
	for _, o := range opts {
		o(&cfg)
	}
	lim, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	return lim
}

// TestSharedQuotaBindsAcrossReplicas is the feature. Three Limiters, one
// backend, one budget — where three Limiters without a backend would each
// enforce the full rate and together allow three times it.
func TestSharedQuotaBindsAcrossReplicas(t *testing.T) {
	const (
		replicas = 3
		burst    = 10
	)
	backend := newGCRAQuota(time.Now)

	var allowed int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range replicas {
		lim := sharedLimiter(t, backend, func(c *pace.Config) {
			c.Rate = pace.PerHour(1) // refill too slow to matter during the test
			c.Burst = burst
		})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range burst * 2 {
				if lim.Client("alice").Allow(context.Background()) {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	if allowed > burst {
		t.Errorf("%d requests allowed across %d replicas sharing a burst of %d; "+
			"the shared quota is not binding", allowed, replicas, burst)
	}
	if allowed == 0 {
		t.Error("no requests were allowed at all")
	}
}

// TestShadowBucketRefusesWithoutCallingTheBackend covers the optimisation that
// makes this affordable: a replica already over its own share can say no
// without a round-trip, because the shadow holding no tokens proves the shared
// bucket holds none either.
func TestShadowBucketRefusesWithoutCallingTheBackend(t *testing.T) {
	backend := newGCRAQuota(time.Now)
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 2
	})
	alice := lim.Client("alice")

	for range 2 {
		if !alice.Allow(context.Background()) {
			t.Fatal("a request within the burst was refused")
		}
	}
	spent := backend.takeCount()
	if spent != 2 {
		t.Fatalf("the backend saw %d takes for 2 allowed requests, want 2", spent)
	}

	// The shadow is empty now, so these must not reach the backend.
	for range 20 {
		if alice.Allow(context.Background()) {
			t.Fatal("a request was allowed with an empty shadow bucket")
		}
	}
	if got := backend.takeCount(); got != spent {
		t.Errorf("the backend saw %d more takes after the shadow was exhausted, want 0: "+
			"the shadow must short-circuit", got-spent)
	}
}

// TestBackendRefusalDoesNotConsumeTheShadow: consuming locally for a request
// the backend refused would let a replica that keeps losing the race throttle
// itself below its share while the shared quota still had room.
func TestBackendRefusalDoesNotConsumeTheShadow(t *testing.T) {
	// A backend that refuses everything, standing in for a replica that loses
	// every race.
	backend := &alwaysRefuse{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 5
	})
	alice := lim.Client("alice")

	for range 20 {
		if alice.Allow(context.Background()) {
			t.Fatal("a request was allowed by a backend that refuses everything")
		}
	}

	// Every one of those was refused by the backend, so the shadow must be
	// untouched — otherwise this replica has quietly demoted itself.
	if got := tokensOf(alice); got != 5 {
		t.Errorf("shadow tokens = %v after 20 backend refusals, want the full 5", got)
	}
}

type alwaysRefuse struct{}

func (alwaysRefuse) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	return pace.Grant{RetryAfter: time.Millisecond}, nil
}

// TestQuotaFallbackLocalKeepsServing is the default failure policy, and the
// same trade pace makes for StateStore: a bookkeeping outage should not become
// a traffic outage.
func TestQuotaFallbackLocalKeepsServing(t *testing.T) {
	backend := &failingQuota{err: errors.New("connection refused")}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 5 })

	if !lim.Client("alice").Allow(context.Background()) {
		t.Error("a request was refused while the backend was down, under QuotaFallbackLocal")
	}
}

func TestQuotaDenyRefusesWhenTheBackendIsDown(t *testing.T) {
	backend := &failingQuota{err: errors.New("connection refused")}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Burst = 5
		c.Shared.OnError = pace.QuotaDeny
	})

	if lim.Client("alice").Allow(context.Background()) {
		t.Error("a request was allowed while the backend was down, under QuotaDeny")
	}

	err := lim.Client("alice").Wait(context.Background())
	if !errors.Is(err, pace.ErrQuotaUnavailable) {
		t.Errorf("Wait = %v, want ErrQuotaUnavailable", err)
	}
}

func TestQuotaAllowIgnoresTheBackendWhenItIsDown(t *testing.T) {
	backend := &failingQuota{err: errors.New("connection refused")}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Burst = 5
		c.Shared.OnError = pace.QuotaAllow
	})

	if !lim.Client("alice").Allow(context.Background()) {
		t.Error("a request was refused under QuotaAllow")
	}
}

// TestCircuitBreakerStopsCallingADeadBackend: without it, every request pays
// QuotaTimeout to be told the same thing.
func TestCircuitBreakerStopsCallingADeadBackend(t *testing.T) {
	backend := &failingQuota{err: errors.New("connection refused")}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 1000 })
	alice := lim.Client("alice")

	for range 50 {
		alice.Allow(context.Background())
	}

	// Five consecutive failures open the breaker; the rest must be
	// short-circuited rather than attempted.
	if got := backend.callCount(); got > 10 {
		t.Errorf("the backend was called %d times across 50 requests, want it to stop after "+
			"roughly the breaker threshold", got)
	}
	if backend.callCount() == 0 {
		t.Error("the backend was never called")
	}
}

// TestInfRateSkipsTheBackend: there is nothing to ration, so paying a
// round-trip to be told so would be pure cost.
func TestInfRateSkipsTheBackend(t *testing.T) {
	backend := &failingQuota{err: errors.New("should not be called")}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Rate = pace.Inf })

	for range 10 {
		if !lim.Client("alice").Allow(context.Background()) {
			t.Fatal("a request was refused at an infinite rate")
		}
	}
	if got := backend.callCount(); got != 0 {
		t.Errorf("the backend was called %d times at Rate: Inf, want 0", got)
	}
}

// TestSharedQuotaDoesNotPersistTheShadow is the interaction that would be
// silently wrong. With a backend configured the local bucket describes this
// replica's share, not the user's spend — restoring another replica's snapshot
// into it would have this process throttling itself for traffic it never sent.
func TestSharedQuotaDoesNotPersistTheShadow(t *testing.T) {
	st := &recordingStore{}
	backend := newGCRAQuota(time.Now)
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Store = st
		c.Burst = 10
	})

	for range 5 {
		lim.Client("alice").Allow(context.Background())
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	if got := st.saveCount(); got != 0 {
		t.Errorf("the shadow bucket was saved %d times; a shadow must never be persisted", got)
	}
	if got := st.loads; got != 0 {
		t.Errorf("the shadow bucket was loaded from %d times; a shadow must never be restored", got)
	}
}

// TestSharedQuotaWaitPathRetriesUntilGranted covers acquire rather than allow:
// a refusal has to become a wait, not an error.
func TestSharedQuotaWaitPathRetriesUntilGranted(t *testing.T) {
	backend := &refuseThenGrant{remaining: 3}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 100 })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lim.Client("alice").Wait(ctx); err != nil {
		t.Fatalf("Wait = %v, want nil once the backend relented", err)
	}
	if got := backend.callCount(); got < 4 {
		t.Errorf("the backend was called %d times, want at least 4 (3 refusals then a grant)", got)
	}
}

// refuseThenGrant refuses a fixed number of times, then grants.
type refuseThenGrant struct {
	mu        sync.Mutex
	remaining int
	calls     int
}

func (q *refuseThenGrant) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	if q.remaining > 0 {
		q.remaining--
		return pace.Grant{RetryAfter: time.Millisecond}, nil
	}
	return pace.Grant{OK: true}, nil
}

func (q *refuseThenGrant) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

// TestWaitingSharedQuotaIsUsedWhenOffered: the optional extension is found by
// type assertion, exactly as BatchStateStore is, so implementing it changes
// behaviour without changing any type pace names in Config.
func TestWaitingSharedQuotaIsUsedWhenOffered(t *testing.T) {
	backend := &waitingQuota{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 100 })

	if err := lim.Client("alice").Wait(context.Background()); err != nil {
		t.Fatalf("Wait = %v, want nil", err)
	}
	if !backend.waited() {
		t.Error("pace polled with Take instead of using the backend's Wait")
	}
}

type waitingQuota struct {
	mu     sync.Mutex
	waits  int
	takes  int
	waitFn func(context.Context) error
}

func (q *waitingQuota) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.takes++
	return pace.Grant{OK: true}, nil
}

func (q *waitingQuota) Wait(ctx context.Context, _ pace.TakeRequest) error {
	q.mu.Lock()
	q.waits++
	fn := q.waitFn
	q.mu.Unlock()
	if fn != nil {
		return fn(ctx)
	}
	return nil
}

func (q *waitingQuota) waited() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.waits > 0 && q.takes == 0
}

// TestTakeRequestCarriesTheUsersQuota: a backend that stores no configuration
// of its own has to be told what to enforce, and it must be the quota in force
// for that user rather than the Limiter default.
func TestTakeRequestCarriesTheUsersQuota(t *testing.T) {
	seen := make(chan pace.TakeRequest, 4)
	backend := &capturingQuota{seen: seen}

	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerMinute(60)
		c.Burst = 5
		c.Shared.Namespace = "svc-a"
		c.QuotaFor = func(userID string) pace.Quota {
			if userID == "paid" {
				return pace.Quota{Rate: pace.PerMinute(600), Burst: 50}
			}
			return pace.Quota{}
		}
	})

	lim.Client("paid").Allow(context.Background())
	got := <-seen
	if got.UserID != "paid" {
		t.Errorf("UserID = %q, want %q", got.UserID, "paid")
	}
	if got.Namespace != "svc-a" {
		t.Errorf("Namespace = %q, want %q", got.Namespace, "svc-a")
	}
	if got.Tokens != 1 {
		t.Errorf("Tokens = %d, want 1", got.Tokens)
	}
	if got.Quota.Rate != pace.PerMinute(600) || got.Quota.Burst != 50 {
		t.Errorf("Quota = %+v, want the paid user's own 600/min burst 50", got.Quota)
	}

	lim.Client("free").Allow(context.Background())
	got = <-seen
	if got.Quota.Rate != pace.PerMinute(60) || got.Quota.Burst != 5 {
		t.Errorf("Quota = %+v, want the defaults for an unlisted user", got.Quota)
	}
}

type capturingQuota struct{ seen chan pace.TakeRequest }

func (q *capturingQuota) Take(_ context.Context, r pace.TakeRequest) (pace.Grant, error) {
	q.seen <- r
	return pace.Grant{OK: true}, nil
}

// TestSharedQuotaThrottleIsReportedOncePerRequest: reporting per poll would
// turn one throttled request into a spike in the metrics.
func TestSharedQuotaThrottleIsReportedOncePerRequest(t *testing.T) {
	var throttles int
	var mu sync.Mutex

	backend := &refuseThenGrant{remaining: 5}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Burst = 100
		c.Observer = &pace.Observer{
			Throttled: func(context.Context, pace.ThrottleInfo) {
				mu.Lock()
				defer mu.Unlock()
				throttles++
			},
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := lim.Client("alice").Wait(ctx); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	if throttles != 1 {
		t.Errorf("Throttled fired %d times across one request with 5 refusals, want 1", throttles)
	}
}

func TestQuotaErrorPolicyString(t *testing.T) {
	for policy, want := range map[pace.QuotaErrorPolicy]string{
		pace.QuotaFallbackLocal:   "fallback-local",
		pace.QuotaDeny:            "deny",
		pace.QuotaAllow:           "allow",
		pace.QuotaErrorPolicy(99): "unknown",
	} {
		if got := policy.String(); got != want {
			t.Errorf("QuotaErrorPolicy(%d).String() = %q, want %q", policy, got, want)
		}
	}
}

// TestSharedQuotaWaitRespectsContextDeadline: a caller who gives up must get
// ctx's error, and must get a LimitError rather than a nil when the local
// shadow is what they were waiting on.
func TestSharedQuotaWaitRespectsContextDeadline(t *testing.T) {
	backend := &alwaysRefuse{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 1
	})
	alice := lim.Client("alice")
	alice.Allow(context.Background()) // drain the shadow, so the next Wait blocks on refill

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := alice.Wait(ctx)
	if err == nil {
		t.Fatal("Wait returned nil against a backend that refuses everything")
	}
	var le *pace.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("Wait = %v, want a *LimitError", err)
	}
	if le.UserID != "alice" {
		t.Errorf("LimitError.UserID = %q, want alice", le.UserID)
	}
}

// TestSharedQuotaWaitReportsCloseAsErrClosed: shutting the Limiter down mid-wait
// is not the caller's deadline expiring, and the two must not be confused.
func TestSharedQuotaWaitReportsCloseAsErrClosed(t *testing.T) {
	backend := &alwaysRefuse{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 1
	})
	alice := lim.Client("alice")
	alice.Allow(context.Background())

	done := make(chan error, 1)
	go func() { done <- alice.Wait(context.Background()) }()

	// Wait until the request is registered, then close underneath it.
	waitFor(t, "the wait to be in flight", func() bool { return lim.Stats().Requests >= 2 })
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		if !errors.Is(err, pace.ErrClosed) {
			t.Errorf("Wait during Close = %v, want ErrClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Wait never returned after Close")
	}
}

// TestWaitingSharedQuotaFailureFollowsThePolicy: the optional Wait extension
// has to honour OnQuotaError the same way Take does, or a backend that
// implements it gets different failure semantics for free.
//
// The three arms must assert three *different* things. An earlier version had
// QuotaFallbackLocal and QuotaAllow both checking only `err == nil`, which is
// exactly what let the implementation stop distinguishing them: the fallback
// admitted without limit and the test could not see it.
func TestWaitingSharedQuotaFailureFollowsThePolicy(t *testing.T) {
	const burst = 3
	newLim := func(t *testing.T, policy pace.QuotaErrorPolicy) *pace.Limiter {
		t.Helper()
		backend := &waitingQuota{
			waitFn: func(context.Context) error { return errors.New("connection refused") },
		}
		return sharedLimiter(t, backend, func(c *pace.Config) {
			c.Rate = pace.PerHour(1) // refill too slow to matter within the test
			c.Burst = burst
			c.Shared.OnError = policy
		})
	}

	// spend counts how many Waits succeed before one blocks past a short
	// deadline. Under QuotaFallbackLocal that is the local burst; under
	// QuotaAllow it is unbounded.
	spend := func(t *testing.T, lim *pace.Limiter, attempts int) int {
		t.Helper()
		n := 0
		for range attempts {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			err := lim.Client("alice").Wait(ctx)
			cancel()
			if err != nil {
				break
			}
			n++
		}
		return n
	}

	t.Run("deny refuses", func(t *testing.T) {
		err := newLim(t, pace.QuotaDeny).Client("alice").Wait(context.Background())
		if !errors.Is(err, pace.ErrQuotaUnavailable) {
			t.Errorf("Wait = %v, want ErrQuotaUnavailable", err)
		}
	})

	t.Run("fallback enforces the local rate", func(t *testing.T) {
		got := spend(t, newLim(t, pace.QuotaFallbackLocal), burst*4)
		if got != burst {
			t.Errorf("%d requests admitted against a local burst of %d; QuotaFallbackLocal "+
				"must fall back to this replica's bucket, not admit without limit", got, burst)
		}
	})

	t.Run("allow admits without limit", func(t *testing.T) {
		const attempts = burst * 4
		if got := spend(t, newLim(t, pace.QuotaAllow), attempts); got != attempts {
			t.Errorf("%d of %d requests admitted; QuotaAllow must not consult anything", got, attempts)
		}
	})
}

// TestWaitingSharedQuotaPassesTheCallersContext: the backend owns the wait, so
// a caller who gives up must be able to stop it.
func TestWaitingSharedQuotaPassesTheCallersContext(t *testing.T) {
	backend := &waitingQuota{
		waitFn: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		},
	}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 100 })

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := lim.Client("alice").Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Wait = %v, want the caller's deadline error", err)
	}
}

// TestQuotaPollDelayNeverWakesEarly: the backend's RetryAfter says when a retry
// could succeed, so jittering below it spends a round-trip to be told the same
// thing.
func TestQuotaPollDelayNeverWakesEarly(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second, time.Millisecond, time.Second, time.Hour} {
		got := pace.QuotaPollDelay(d)
		if d <= 0 {
			if got != 0 {
				t.Errorf("QuotaPollDelay(%v) = %v, want 0", d, got)
			}
			continue
		}
		if got < d {
			t.Errorf("QuotaPollDelay(%v) = %v, want at least %v", d, got, d)
		}
		if got > d+d/2 {
			t.Errorf("QuotaPollDelay(%v) = %v, want at most %v", d, got, d+d/2)
		}
	}
}

// silentRefuse refuses every Take without saying when to retry, which
// Grant.RetryAfter documents as legal ("zero means the backend is not saying")
// and which pacetest currently accepts as conformant.
type silentRefuse struct {
	mu    sync.Mutex
	calls int
}

func (q *silentRefuse) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	return pace.Grant{}, nil // OK false, RetryAfter zero
}

func (q *silentRefuse) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

// TestWaitDoesNotSpinWhenTheBackendGivesNoSchedule is the CPU-burn guard.
//
// The refusal path cancels the shadow reservation, which by design puts the
// token back — so the local-estimate fallback (u.bucket.DelayAt) is
// structurally guaranteed to return zero on exactly this path, because the only
// way to reach it is that the shadow has tokens and the shared bucket does not.
// Zero then flowed through quotaPollDelay(0) into sleep(ctx, 0), which returned
// immediately without even checking ctx. The result was a tight loop issuing
// one Take per iteration until the deadline.
func TestWaitDoesNotSpinWhenTheBackendGivesNoSchedule(t *testing.T) {
	backend := &silentRefuse{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerSecond(10) // one token per 100ms
		c.Burst = 100               // a shadow that will not refuse on its own
	})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := lim.Client("alice").Wait(ctx)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Wait returned nil against a backend that refuses everything")
	}
	// It must give up with the context, not hang.
	if elapsed > 5*time.Second {
		t.Fatalf("Wait took %v against a 300ms deadline", elapsed)
	}
	// The real assertion. At one token per 100ms over ~300ms, a sane poller
	// makes a handful of calls. The spinning version made tens of thousands.
	if n := backend.callCount(); n > 20 {
		t.Errorf("the backend was called %d times in %v; the poll loop is spinning "+
			"rather than backing off", n, elapsed)
	}
	if backend.callCount() == 0 {
		t.Error("the backend was never called")
	}
}

// TestSleepHonoursCancellationAtZeroDelay: a polling loop that computes a zero
// delay must still be cancellable. sleep returned nil immediately for d <= 0
// without consulting ctx, so the only thing that could end the loop was a
// backend that eventually granted.
func TestSleepHonoursCancellationAtZeroDelay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := pace.SleepFor(ctx, 0); !errors.Is(err, context.Canceled) {
		t.Errorf("sleep(cancelled ctx, 0) = %v, want context.Canceled", err)
	}
	if err := pace.SleepFor(context.Background(), 0); err != nil {
		t.Errorf("sleep(live ctx, 0) = %v, want nil", err)
	}
}

// TestWaitDoesNotReportSuccessOnAnExpiredContext: takeShared had no ctx.Err()
// guard. A conformant backend must honour the context — pacetest requires it —
// so a caller whose deadline expires mid-Take produces context.DeadlineExceeded,
// which pace then (1) recorded as a *backend* failure in the shared circuit
// breaker and (2) converted, under the default policy, into "proceed". Wait
// therefore returned nil on an expired context, where the non-shared path
// returns a LimitError.
func TestWaitDoesNotReportSuccessOnAnExpiredContext(t *testing.T) {
	backend := &ctxRespectingQuota{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 100 })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := lim.Client("alice").Wait(ctx)
	if err == nil {
		t.Fatal("Wait returned nil for an already-cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("Wait = %v, want it to wrap context.Canceled", err)
	}
}

// TestCallerCancellationIsNotChargedToTheBreaker: a service with short
// per-request deadlines would otherwise open the breaker on its own
// cancellations — five in a row and every user falls back to local buckets for
// five seconds, with a log line blaming the backend.
func TestCallerCancellationIsNotChargedToTheBreaker(t *testing.T) {
	backend := &ctxRespectingQuota{}
	lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 100 })

	for range breaker.Threshold {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = lim.Client("alice").Wait(ctx)
	}

	// The breaker must still be closed, so a live request reaches the backend.
	before := backend.callCount()
	if !lim.Client("alice").Allow(context.Background()) {
		t.Fatal("a live request was refused after the caller cancelled earlier ones")
	}
	if backend.callCount() == before {
		t.Error("the backend was not called; the breaker opened on caller cancellations")
	}
}

// ctxRespectingQuota grants unless the context is done, which is what pacetest
// requires of a conformant backend.
type ctxRespectingQuota struct {
	mu    sync.Mutex
	calls int
}

func (q *ctxRespectingQuota) Take(ctx context.Context, _ pace.TakeRequest) (pace.Grant, error) {
	q.mu.Lock()
	q.calls++
	q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return pace.Grant{}, err
	}
	return pace.Grant{OK: true}, nil
}

func (q *ctxRespectingQuota) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

// TestWaitingSharedQuotaDoesNotReportEveryRequestAsThrottled: waitShared fired
// report(0) before delegating, so Observer.Throttled and Stats.Throttled counted
// every request on this path — Stats.Throttled == Stats.Requests identically,
// even when the backend granted instantly. Both are documented as counting
// requests that "had to wait for a token".
func TestWaitingSharedQuotaDoesNotReportEveryRequestAsThrottled(t *testing.T) {
	var throttles int
	var mu sync.Mutex

	backend := &waitingQuota{} // grants immediately
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Burst = 100
		c.Observer = &pace.Observer{
			Throttled: func(context.Context, pace.ThrottleInfo) {
				mu.Lock()
				defer mu.Unlock()
				throttles++
			},
		}
	})

	for range 5 {
		if err := lim.Client("alice").Wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if throttles != 0 {
		t.Errorf("Throttled fired %d times for 5 immediately-granted requests, want 0", throttles)
	}
	if got := lim.Stats().Throttled; got != 0 {
		t.Errorf("Stats.Throttled = %d, want 0", got)
	}
	if got := lim.Stats().Requests; got != 5 {
		t.Errorf("Stats.Requests = %d, want 5", got)
	}
}

// TestReserveConsultsTheSharedBackend: Reserve went straight to the local
// bucket and returned OK on its say-so alone — no sharedEnabled branch, where
// both allow and acquire have one. That makes the shadow authoritative, which
// ADR 0004 states it can never be ("the shadow only refuses"), and it means
// zero Takes for an admitted request, which the same ADR states is exactly one.
//
// It is worse than a per-replica leak: persistsState is false under a shared
// quota, so every replica boots with a *full* shadow, and N replicas x full
// burst were admitted with the backend never consulted.
func TestReserveConsultsTheSharedBackend(t *testing.T) {
	backend := newGCRAQuota(time.Now)
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 10
	})

	r := lim.Client("alice").Reserve(context.Background())
	if !r.OK() {
		t.Fatal("Reserve was refused by a backend with a full bucket")
	}
	if got := backend.takeCount(); got != 1 {
		t.Errorf("the backend saw %d takes for one admitted reservation, want exactly 1", got)
	}
}

// TestReserveIsRefusedWhenTheBackendRefuses: the whole point of consulting the
// backend is that its answer binds.
func TestReserveIsRefusedWhenTheBackendRefuses(t *testing.T) {
	lim := sharedLimiter(t, &alwaysRefuse{}, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 10
	})
	alice := lim.Client("alice")

	r := alice.Reserve(context.Background())
	if r.OK() {
		t.Error("Reserve succeeded against a backend that refuses everything")
	}
	// And the shadow must be untouched, for the reason allowShared documents:
	// consuming locally for a request the backend refused ratchets this replica
	// below its own share.
	if got := tokensOf(alice); got != 10 {
		t.Errorf("shadow tokens = %v after a refused reservation, want the full 10", got)
	}
}

// TestReserveSkipsTheBackendWhenTheShadowAlreadyRefuses is the optimisation
// that makes the shared path affordable, and it must hold for Reserve too.
func TestReserveSkipsTheBackendWhenTheShadowAlreadyRefuses(t *testing.T) {
	backend := newGCRAQuota(time.Now)
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Rate = pace.PerHour(1)
		c.Burst = 1
	})
	alice := lim.Client("alice")

	if r := alice.Reserve(context.Background()); !r.OK() {
		t.Fatal("the first reservation was refused")
	}
	spent := backend.takeCount()

	// The shadow now holds nothing, which proves the shared bucket holds
	// nothing either. No round-trip should be spent finding that out.
	r := alice.Reserve(context.Background())
	if r.Delay() == 0 {
		t.Error("a reservation against an empty shadow reported no delay")
	}
	if got := backend.takeCount(); got != spent {
		t.Errorf("the backend saw %d more takes after the shadow was exhausted, want 0", got-spent)
	}
}

// switchableQuota fails or succeeds on demand, and counts calls.
type switchableQuota struct {
	mu      sync.Mutex
	failing bool
	calls   int
}

func (q *switchableQuota) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.calls++
	if q.failing {
		return pace.Grant{}, errors.New("connection refused")
	}
	return pace.Grant{OK: true}, nil
}

func (q *switchableQuota) callCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.calls
}

func (q *switchableQuota) setFailing(v bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failing = v
}

// TestCircuitBreakerRecoversThroughASingleProbe covers the half of the breaker
// nothing tested: what happens after the cooldown.
//
// The doc promised "one request is let through to test the backend" and the
// code delivered no such thing — once openTill passed, every caller went
// through, so an unbounded number of them paid a full QuotaTimeout against a
// backend still known to be down. And because opening reset the failure count,
// re-opening then needed five *more* failures rather than one.
//
// Driven by a fake clock, so crossing the cooldown is exact rather than a
// five-second sleep.
func TestCircuitBreakerRecoversThroughASingleProbe(t *testing.T) {
	clk := newFakeClock()
	backend := &switchableQuota{failing: true}
	lim := sharedLimiter(t, backend, func(c *pace.Config) {
		c.Burst = 1000
		c.Clock = clk
	})
	alice := lim.Client("alice")

	// Trip it.
	for range breaker.Threshold {
		alice.Allow(context.Background())
	}
	tripped := backend.callCount()
	if tripped != breaker.Threshold {
		t.Fatalf("the backend saw %d calls before the breaker opened, want %d", tripped, breaker.Threshold)
	}

	// While open, nothing reaches the backend.
	for range 20 {
		alice.Allow(context.Background())
	}
	if got := backend.callCount(); got != tripped {
		t.Fatalf("the backend saw %d calls while the breaker was open, want 0", got-tripped)
	}

	// Past the cooldown: exactly one probe, however many callers arrive.
	clk.advance(6 * time.Second)
	for range 20 {
		alice.Allow(context.Background())
	}
	probes := backend.callCount() - tripped
	if probes != 1 {
		t.Errorf("the backend saw %d calls after the cooldown, want exactly 1 probe", probes)
	}

	// That probe failed, so it must re-open immediately rather than waiting for
	// another five failures.
	for range 20 {
		alice.Allow(context.Background())
	}
	if got := backend.callCount() - tripped; got != probes {
		t.Errorf("the backend saw %d more calls after a failed probe, want 0: a failed probe "+
			"must re-open the breaker on its own", got-probes)
	}

	// Once the backend is healthy again, the next probe closes it.
	backend.setFailing(false)
	clk.advance(6 * time.Second)
	if !alice.Allow(context.Background()) {
		t.Fatal("the probe was refused after the backend recovered")
	}
	before := backend.callCount()
	for range 10 {
		alice.Allow(context.Background())
	}
	if got := backend.callCount() - before; got != 10 {
		t.Errorf("the backend saw %d of 10 calls after recovery, want all: the breaker "+
			"should be closed", got)
	}
}

// TestStatsReportTheSharedBackend: with a shared quota configured, nothing in
// Stats said whether the backend was being reached at all — so an operator
// whose Redis was down saw a healthy-looking snapshot while every replica
// quietly fell back to enforcing the rate per process.
func TestStatsReportTheSharedBackend(t *testing.T) {
	t.Run("grants and refusals", func(t *testing.T) {
		backend := newGCRAQuota(time.Now)
		lim := sharedLimiter(t, backend, func(c *pace.Config) {
			c.Rate = pace.PerHour(1)
			c.Burst = 2
		})
		alice := lim.Client("alice")

		for range 5 {
			alice.Allow(context.Background())
		}

		got := lim.Stats()
		// Two granted, then the shadow is empty and short-circuits the rest —
		// which is the optimisation working, and it is visible here.
		if got.QuotaTakes != 2 {
			t.Errorf("QuotaTakes = %d, want 2", got.QuotaTakes)
		}
		if got.QuotaRefused != 0 {
			t.Errorf("QuotaRefused = %d, want 0", got.QuotaRefused)
		}
		if got.QuotaErrors != 0 {
			t.Errorf("QuotaErrors = %d, want 0", got.QuotaErrors)
		}
	})

	t.Run("a refusing backend is visible", func(t *testing.T) {
		lim := sharedLimiter(t, &alwaysRefuse{}, func(c *pace.Config) {
			c.Rate = pace.PerHour(1)
			c.Burst = 10
		})
		for range 3 {
			lim.Client("alice").Allow(context.Background())
		}
		if got := lim.Stats().QuotaRefused; got != 3 {
			t.Errorf("QuotaRefused = %d, want 3", got)
		}
	})

	t.Run("a dead backend is visible", func(t *testing.T) {
		backend := &failingQuota{err: errors.New("connection refused")}
		lim := sharedLimiter(t, backend, func(c *pace.Config) { c.Burst = 1000 })
		for range 20 {
			lim.Client("alice").Allow(context.Background())
		}

		got := lim.Stats()
		if got.QuotaErrors < breaker.Threshold {
			t.Errorf("QuotaErrors = %d, want at least %d: this is the number an "+
				"operator alerts on", got.QuotaErrors, breaker.Threshold)
		}
		// The breaker suppresses most of the calls, so the errors must exceed
		// the takes — that difference is what says "we stopped even trying".
		if got.QuotaErrors <= got.QuotaTakes {
			t.Errorf("QuotaErrors = %d, QuotaTakes = %d; short-circuited calls must be "+
				"counted as errors too", got.QuotaErrors, got.QuotaTakes)
		}
	})

	t.Run("zero without a shared quota", func(t *testing.T) {
		lim, _ := newTestLimiter(t)
		lim.Client("alice").Allow(context.Background())
		got := lim.Stats()
		if got.QuotaTakes != 0 || got.QuotaRefused != 0 || got.QuotaErrors != 0 {
			t.Errorf("quota counters = %d/%d/%d with no shared quota, want all zero",
				got.QuotaTakes, got.QuotaRefused, got.QuotaErrors)
		}
	})
}

// refusingQuota refuses every request and reports how many shared tokens remain.
type refusingQuota struct{ tokens float64 }

func (q refusingQuota) Take(context.Context, pace.TakeRequest) (pace.Grant, error) {
	t := q.tokens
	return pace.Grant{OK: false, RetryAfter: time.Second, Tokens: &t}, nil
}

// TestThrottleReportsTheBackendsTokensNotTheShadows pins which number reaches
// the operator when a shared backend refuses.
//
// The shadow bucket is not authoritative — ADR 0004 says so, and it is why the
// shadow may only refuse. On a refusal it holds this replica's fraction of the
// quota, which here is a full burst of 100, while the backend is reporting that
// the shared budget is down to 3. Reporting 100 to a dashboard while the shared
// quota is nearly spent is not a rounding error, it is the wrong quantity.
//
// Grant.Tokens existed and was read by nothing before v0.5.0; this is what
// makes it a field pace acts on rather than one it merely asks backends to fill.
func TestThrottleReportsTheBackendsTokensNotTheShadows(t *testing.T) {
	const backendTokens = 3

	var got []float64
	var mu sync.Mutex
	lim := sharedLimiter(t, refusingQuota{tokens: backendTokens}, func(c *pace.Config) {
		c.Observer = &pace.Observer{
			Throttled: func(_ context.Context, info pace.ThrottleInfo) {
				mu.Lock()
				defer mu.Unlock()
				got = append(got, info.Tokens)
			},
		}
	})

	if lim.Client("alice").Allow(context.Background()) {
		t.Fatal("Allow succeeded against a backend that refuses everything")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("Throttled fired %d times, want 1", len(got))
	}
	if got[0] != backendTokens {
		t.Errorf("ThrottleInfo.Tokens = %v, want %v (the backend's count). "+
			"A value near the local burst means the shadow bucket was reported instead, "+
			"and the shadow is never authoritative for a shared quota.", got[0], float64(backendTokens))
	}
}
