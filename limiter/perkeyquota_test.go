package limiter_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/store/memory"
)

// tierLimiter builds a Limiter whose keys are graded by a QuotaFor closure.
func tierLimiter(t *testing.T, url string, tiers map[string]bucket.Quota) *client.Pool {
	t.Helper()
	return buildWith(t, config.Config{
		BaseURL: url,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2},
		Clock:   newFakeClock(),
	}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
		if q, ok := tiers[key]; ok {
			return q
		}
		return def
	}))
}

// TestQuotaForGradesKeysIndependently is the feature the package name implies
// and did not have: the rate was global, so pace could isolate keys from each
// other but not tell a paying one from a free one.
func TestQuotaForGradesKeysIndependently(t *testing.T) {
	lim := tierLimiter(t, "http://example.invalid", map[string]bucket.Quota{
		"paid": {Rate: bucket.PerMinute(600), Burst: 50},
		// "free" is absent, so tierLimiter's closure hands back its own default.
	})

	paid, free := lim.Client("paid"), lim.Client("free")

	if got, want := paid.Quota(), (bucket.Quota{Rate: bucket.PerMinute(600), Burst: 50}); got != want {
		t.Errorf("paid quota = %+v, want %+v", got, want)
	}
	if got, want := free.Quota(), (bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2}); got != want {
		t.Errorf("free quota = %+v, want %+v (the Config defaults)", got, want)
	}

	// And the buckets actually enforce them.
	var paidAllowed, freeAllowed int
	for range 50 {
		if paid.Allow(context.Background()) {
			paidAllowed++
		}
		if free.Allow(context.Background()) {
			freeAllowed++
		}
	}
	if paidAllowed != 50 {
		t.Errorf("the paid key got %d of 50 requests, want all: burst is 50", paidAllowed)
	}
	if freeAllowed != 2 {
		t.Errorf("the free key got %d requests, want 2: burst is 2", freeAllowed)
	}
}

// TestThrottleReportsTheKeysOwnQuota: LimitError and ThrottleInfo have always
// documented their Limit and Burst as "the configuration in force for that
// key". When there was one Config-wide rate, reading that happened to be
// right. It has not been since QuotaFor could grade keys, and there is no
// Config-wide rate left to read even if it were.
func TestThrottleReportsTheKeysOwnQuota(t *testing.T) {
	var infos []observe.ThrottleInfo
	var mu sync.Mutex

	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1},
		Clock:   newFakeClock(),
		Observer: &observe.Observer{
			Throttled: func(_ context.Context, info observe.ThrottleInfo) {
				mu.Lock()
				defer mu.Unlock()
				infos = append(infos, info)
			},
		},
	}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
		if key == "paid" {
			return bucket.Quota{Rate: bucket.PerMinute(600), Burst: 3}
		}
		return def
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	paid := lim.Client("paid")
	for range 4 { // three allowed, the fourth throttles
		paid.Allow(context.Background())
	}

	mu.Lock()
	defer mu.Unlock()
	if len(infos) != 1 {
		t.Fatalf("Throttled fired %d times, want 1", len(infos))
	}
	if infos[0].Limit != float64(bucket.PerMinute(600)) || infos[0].Burst != 3 {
		t.Errorf("ThrottleInfo reported limit %v burst %d, want the paid key's 600/min burst 3",
			infos[0].Limit, infos[0].Burst)
	}
}

func TestLimitErrorReportsTheKeysOwnQuota(t *testing.T) {
	// A real clock here: limiter.Limiter.Wait schedules against the wall clock
	// regardless of Config.Clock, so a frozen fake would leave the bucket
	// looking full to Wait and empty to everything else. One token per hour
	// makes the refill far too slow to race the 10ms deadline.
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerHour(1), Burst: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	alice := lim.Client("alice")
	alice.Allow(context.Background()) // spend the single token

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	waitErr := alice.Wait(ctx)

	var le *limiter.LimitError
	if !errors.As(waitErr, &le) {
		t.Fatalf("Wait = %v, want a *LimitError", waitErr)
	}
	if le.Limit != bucket.PerHour(1) || le.Burst != 1 {
		t.Errorf("LimitError reported %v burst %d, want the key's own 1/hour burst 1", le.Limit, le.Burst)
	}
}

// TestReloadQuotasAppliesToLiveBuckets: before this, changing a tier meant
// building a new Limiter, which dropped every in-memory bucket in the process.
func TestReloadQuotasAppliesToLiveBuckets(t *testing.T) {
	var mu sync.Mutex
	tiers := map[string]bucket.Quota{"alice": {Rate: bucket.PerMinute(60), Burst: 2}}

	clk := newFakeClock()
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2},
		Clock:   clk,
	}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
		mu.Lock()
		defer mu.Unlock()
		if q, ok := tiers[key]; ok {
			return q
		}
		return def
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	alice := lim.Client("alice")
	if !alice.Allow(context.Background()) {
		t.Fatal("the first request was refused")
	}
	before := tokensOf(alice) // 1 of 2

	mu.Lock()
	tiers["alice"] = bucket.Quota{Rate: bucket.PerMinute(600), Burst: 20}
	mu.Unlock()

	// Nothing changes until the reload: the bucket is what enforces the quota.
	if got := alice.Quota().Burst; got != 2 {
		t.Errorf("burst = %d before ReloadQuotas, want the old 2", got)
	}

	lim.ReloadQuotas()

	if got, want := alice.Quota(), (bucket.Quota{Rate: bucket.PerMinute(600), Burst: 20}); got != want {
		t.Errorf("quota after reload = %+v, want %+v", got, want)
	}
	// The upgrade must not hand out a full new bucket, or a key could farm
	// tokens by triggering reloads.
	if after := tokensOf(alice); after != before {
		t.Errorf("tokens went from %v to %v across the reload; accrued tokens must be kept as they are",
			before, after)
	}
}

// TestReloadQuotasIgnoresKeysNotInMemory: they need nothing, because their
// bucket is built from QuotaFor when they next appear.
func TestReloadQuotasIgnoresKeysNotInMemory(t *testing.T) {
	lim := tierLimiter(t, "http://example.invalid", map[string]bucket.Quota{
		"ghost": {Rate: bucket.PerMinute(600), Burst: 50},
	})
	lim.ReloadQuotas() // nobody is in memory; must not panic

	if got := lim.Stats().Keys; got != 0 {
		t.Errorf("Keys = %d, want 0: ReloadQuotas must not create anyone", got)
	}
	if got := lim.Client("ghost").Quota().Burst; got != 50 {
		t.Errorf("burst = %d on first sight, want 50 from QuotaFor", got)
	}
}

// TestQuotaForRunsOutsideTheShardLock is the regression guard for the one
// constraint this feature has. QuotaFor is caller code; running it under a
// shard write lock would let a slow implementation stall every key who hashes
// to that shard, which is the same mistake entryFor's loadState call already
// documents avoiding.
func TestQuotaForRunsOutsideTheShardLock(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)

	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 10},
		Shards:  1, // one shard, so every key provably collides
	}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
		if key == "slow" {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-release
		}
		return def
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	go func() { lim.Client("slow").Allow(context.Background()) }()
	<-entered

	// A different key on the same shard must not be stuck behind it.
	done := make(chan struct{})
	go func() {
		defer close(done)
		lim.Client("other").Allow(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("a second key blocked behind a slow QuotaFor: it is being called under the shard lock")
	}
	close(release)
}

// TestReloadQuotasDoesNotHoldTheShardLock: same constraint on the other call
// site. A ReloadQuotas whose QuotaFor blocks must not freeze live traffic.
func TestReloadQuotasDoesNotHoldTheShardLock(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)
	var blocking sync.Once

	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 10},
		Shards:  1,
	}, config.WithQuotaFor(func(_ string, def bucket.Quota) bucket.Quota {
		blocking.Do(func() {}) // first call during New-time traffic is free
		select {
		case <-release:
		default:
			select {
			case entered <- struct{}{}:
				<-release
			default:
			}
		}
		return def
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	// Put someone in memory first, draining the one blocking call.
	go lim.Client("alice").Allow(context.Background())
	<-entered

	reloaded := make(chan struct{})
	go func() {
		defer close(reloaded)
		lim.ReloadQuotas()
	}()

	// Reads must stay serviceable while the reload is in progress.
	done := make(chan struct{})
	go func() {
		defer close(done)
		lim.Stats()
		lim.Client("alice").Tokens()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("reads blocked during ReloadQuotas: QuotaFor is being called under the shard lock")
	}
	close(release)
	<-reloaded
}

// TestRestoredKeyIsClampedToTheCurrentBurst: persisted state carries no quota,
// so a demoted key must not be handed back a ceiling they no longer have.
func TestRestoredKeyIsClampedToTheCurrentBurst(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := memory.New()
	burst := 50
	newLim := func() *client.Pool {
		t.Helper()
		lim, err := client.New(config.Config{
			BaseURL: srv.URL,
			Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: burst},
			Store:   st,
			Clock:   newFakeClock(),
		})
		if err != nil {
			t.Fatal(err)
		}
		return lim
	}

	// Generous tier: one request creates the bucket, and the rest of its large
	// balance is persisted on Close.
	lim := newLim()
	if !lim.Client("alice").Allow(context.Background()) {
		t.Fatal("the first request was refused")
	}
	if got := lim.Client("alice").Quota().Burst; got != 50 {
		t.Fatalf("burst = %d, want 50", got)
	}
	if got := tokensOf(lim.Client("alice")); got != 49 {
		t.Fatalf("tokens = %v, want 49 of 50 after one request", got)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	// Demoted, then restored from the same saved state.
	burst = 3
	lim = newLim()
	defer lim.Close()

	lim.Client("alice").Allow(context.Background()) // bring the key back into memory
	if got := lim.Client("alice").Quota().Burst; got != 3 {
		t.Fatalf("burst = %d after demotion, want 3", got)
	}
	if got := tokensOf(lim.Client("alice")); got > 3 {
		t.Errorf("tokens = %v after a demotion to burst 3, want at most 3: "+
			"saved state must not resurrect an old ceiling", got)
	}
}

// TestNonFiniteRateIsNotAcceptedSilently: bucket.Limit is a float64, so a caller
// can write Limit(math.Inf(1)) or a NaN. Both passed validate — its only check
// was Rate <= 0, which neither trips — and produced a bucket whose token count
// was NaN, refusing every request forever. Found by fuzzing RestoreBucket.
// TestReloadQuotasReadsTheClockPerKey: ReloadQuotas captured one `now` and
// passed it to every key across all 256 shards, after however long QuotaFor
// took for each. SetQuotaAt writes that instant as the bucket's last-updated
// time, so any key whose bucket had already advanced past it was rewound — and
// a rewound interval is refilled a second time, which is a silent quota grant
// on a maintenance call.
//
// The reachable case is a key who makes a request while the walk is in
// progress, which is inherently racy to stage. This asserts the fix directly
// instead: the clock is read where it is used, once per key, rather than once
// for the whole walk.
func TestReloadQuotasReadsTheClockPerKey(t *testing.T) {
	clk := newFakeClock()
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(600), Burst: 10},
		Clock:   clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, u := range keys {
		lim.Client(u).Allow(context.Background())
	}

	before := clk.callCount()
	lim.ReloadQuotas()
	got := clk.callCount() - before

	if got < len(keys) {
		t.Errorf("ReloadQuotas read the clock %d times for %d keys in memory; it must read it "+
			"where it stamps each bucket, not once for the whole walk", got, len(keys))
	}
}

// TestQuotaForIsCalledConcurrently is the guard the example cannot be.
//
// QuotaFor runs on request goroutines — one per key whose bucket is being
// created — and on whatever goroutine calls ReloadQuotas. Nothing said so at
// first, and ExamplePool_ReloadQuotas demonstrated the racy shape: a plain map
// written by the caller while the closure read it. An Example cannot catch that,
// because `// Output:` forces it to be single-goroutine and -race sees nothing
// in a program with one goroutine. So the guard lives here.
//
// The hook parks every cold-path entrant until all of them have arrived, so they
// call QuotaFor at the same instant rather than whenever the scheduler feels
// like it. Swap the atomic.Pointer below for a plain map and this fails under
// -race; that is the mutation that proves it works.
func TestQuotaForIsCalledConcurrently(t *testing.T) {
	const keys = 8

	srv := newEchoServer(t)
	defer srv.Close()

	slow := map[string]bucket.Quota{}
	fast := map[string]bucket.Quota{}
	for i := range keys {
		id := fmt.Sprintf("u%d", i)
		slow[id] = bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1}
		fast[id] = bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 50}
	}

	var tiers atomic.Pointer[map[string]bucket.Quota]
	tiers.Store(&slow)

	pool := buildWith(t, config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(600), Burst: 10},
		Shards:  1, // one shard, so every key contends for the same lock
	}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
		if q, ok := (*tiers.Load())[key]; ok {
			return q
		}
		return def
	}))

	// Release the parked goroutines only once all of them have arrived.
	var (
		arrive = make(chan struct{}, keys)
		start  = make(chan struct{})
	)
	limiter.SetGetOrCreateHook(pool.Limiter(), func() {
		arrive <- struct{}{}
		<-start
	})

	var wg sync.WaitGroup
	for i := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			pool.Client(fmt.Sprintf("u%d", i)).Allow(context.Background())
		}()
	}
	for range keys {
		<-arrive // every goroutine is inside the cold path
	}

	// Swap the table and reload while all eight are about to read it.
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for range 50 {
			tiers.Store(&fast)
			pool.ReloadQuotas()
			tiers.Store(&slow)
			pool.ReloadQuotas()
		}
	}()

	close(start)
	wg.Wait()
	<-writerDone

	// Whatever they raced to, every key must have a bucket enforcing one of the
	// two configured quotas — never a zero one, which is what a lost read gives.
	for i := range keys {
		q := pool.Client(fmt.Sprintf("u%d", i)).Quota()
		if q.Rate <= 0 || q.Burst <= 0 {
			t.Fatalf("u%d ended with quota %+v; a concurrent QuotaFor read was lost", i, q)
		}
	}
}

// TestQuotaForChangeAppliesExplicitly is the contract: new keys at once,
// existing buckets when you ask.
//
// Applying is a separate step on purpose: a key with no bucket yet is about to
// call the hook, so it picks the change up at once, and a key already in memory
// keeps the tokens it has accrued until a reload asks for it by name.
func TestQuotaForChangeAppliesExplicitly(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	var live atomic.Pointer[bucket.Quota]
	live.Store(&bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2})

	pool := buildWith(t, config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2},
	}, config.WithQuotaFor(func(string, bucket.Quota) bucket.Quota { return *live.Load() }))

	old := pool.Client("old")
	old.Allow(context.Background()) // brings a bucket into memory at the old quota

	next := bucket.Quota{Rate: bucket.PerMinute(600), Burst: 50}
	live.Store(&next)

	// A key who has never been seen gets it immediately: they are about to
	// call QuotaFor for the first time.
	if got := pool.Client("fresh").Quota(); got != next {
		t.Errorf("a new key's quota = %+v, want %+v", got, next)
	}

	// The one already in memory does not, until asked.
	if got := old.Quota().Burst; got != 2 {
		t.Errorf("an existing bucket's burst = %d, want the old 2 until a reload", got)
	}
	if !old.ReloadQuota() {
		t.Fatal("ReloadQuota reported no bucket for a key that has one")
	}
	if got := old.Quota().Burst; got != 50 {
		t.Errorf("burst = %d after ReloadQuota, want 50", got)
	}
}

// TestReportedQuotaIsTheEnforcedQuota is the invariant the whole arrangement
// rests on: what pace says a key's quota is, before that key has a bucket, is
// what its bucket enforces once it does.
//
// The two answers come from different code — Limiter.Quota reads quotaFor for a
// key it has never seen, and reads the bucket for one it has — so they can
// disagree. They did while this was being written: quotaFor applied
// bucket.Finite but not the floor NewBucket applies, so a NaN rate was reported
// as NaN and enforced as zero. One normalisation now, and this is what holds it
// to one.
//
// The values go in through the option, because Config.Quota is checked by
// Resolve and half of these would not get past it — which is the point of the
// split: the value a caller writes down is validated, and the one a hook
// produces at run time is clamped.
func TestReportedQuotaIsTheEnforcedQuota(t *testing.T) {
	for _, give := range []bucket.Quota{
		{Rate: bucket.PerMinute(60), Burst: 10},
		{Rate: bucket.PerMinute(60)},                // burst raised to one
		{Rate: bucket.Limit(math.Inf(1)), Burst: 1}, // infinity made finite
		{Rate: bucket.Limit(math.NaN()), Burst: 3},  // unusable, clamped
		{Rate: -1, Burst: 3},                        // unusable, clamped
		{Rate: bucket.Inf, Burst: 2},
	} {
		t.Run(give.Rate.String(), func(t *testing.T) {
			pool := buildWith(t, config.Config{
				BaseURL: "http://example.invalid",
				Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1},
			}, config.WithQuotaFor(func(string, bucket.Quota) bucket.Quota { return give }))
			alice := pool.Client("alice")

			before := alice.Quota() // no bucket yet: answered from quotaFor
			alice.Allow(context.Background())
			after := alice.Quota() // now answered from the bucket

			if before != after {
				t.Errorf("quota before a bucket exists = %+v, after = %+v; one value, two answers", before, after)
			}
		})
	}
}

// TestOptionQuotaIsNormalised: what a WithQuotaFor hook returns arrives one key
// at a time, on the goroutine building that key's bucket, long after Resolve
// has run. Nothing downstream re-checks it, so this is what stands between a
// caller and a bucket holding NaN tokens.
//
// There is no error to return — quotaFor is called from inside the registry —
// so an unusable rate fails closed and is logged. That is the price of the
// hook, and the reason Config.Quota is a value: the common case is checked at
// construction instead.
func TestOptionQuotaIsNormalised(t *testing.T) {
	for _, tt := range []struct {
		name string
		give bucket.Quota
		want bucket.Quota
	}{
		{"a non-positive Burst is raised to one",
			bucket.Quota{Rate: bucket.PerMinute(60)},
			bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1}},
		{"an infinite Rate is made finite",
			bucket.Quota{Rate: bucket.Limit(math.Inf(1)), Burst: 1},
			bucket.Quota{Rate: bucket.Inf, Burst: 1}},
		{"a NaN Rate cannot reach the arithmetic",
			bucket.Quota{Rate: bucket.Limit(math.NaN()), Burst: 3},
			bucket.Quota{Rate: 0, Burst: 3}},
		{"a negative Rate cannot either",
			bucket.Quota{Rate: -1, Burst: 3},
			bucket.Quota{Rate: 0, Burst: 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pool := buildWith(t, config.Config{
				BaseURL: "http://example.invalid",
				Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1},
			}, config.WithQuotaFor(func(string, bucket.Quota) bucket.Quota { return tt.give }))
			if got := pool.Client("alice").Quota(); got != tt.want {
				t.Errorf("quota = %+v, want %+v", got, tt.want)
			}
		})
	}
}

// TestOptionWithoutAHookIsTheConfiguredQuota: no option means the field, with
// nothing in between. quotaFor returns Config.Quota unchanged because Resolve
// already normalised it, and a second normalisation is a second place for the
// rule to drift.
func TestOptionWithoutAHookIsTheConfiguredQuota(t *testing.T) {
	want := bucket.Quota{Rate: bucket.PerMinute(600), Burst: 7}
	pool := build(t, config.Config{BaseURL: "http://example.invalid", Quota: want})
	if got := pool.Client("alice").Quota(); got != want {
		t.Errorf("quota = %+v, want the configured %+v", got, want)
	}
}

// TestUnusableRateFailsClosedAndSaysSo: the two halves of "there is no error to
// return". The key is throttled to a standstill rather than served, and the
// operator is told which key and why.
func TestUnusableRateFailsClosedAndSaysSo(t *testing.T) {
	var buf bytes.Buffer
	pool := buildWith(t, config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1},
		Logger:  slog.New(slog.NewTextHandler(&buf, nil)),
	}, config.WithQuotaFor(func(string, bucket.Quota) bucket.Quota {
		return bucket.Quota{Rate: 0, Burst: 1}
	}))

	alice := pool.Client("alice")
	if !alice.Allow(context.Background()) {
		t.Fatal("the initial burst was refused; a zero rate should still hand out the tokens it starts with")
	}
	if alice.Allow(context.Background()) {
		t.Error("a second request was allowed; a zero rate never refills")
	}
	if got := buf.String(); !strings.Contains(got, "unusable rate") || !strings.Contains(got, "alice") {
		t.Errorf("log = %q, want it to name the key and the problem", got)
	}
}

// TestQuotaForSwapWhileRequestsInFlight: changing a rate is a control-plane act
// an operator performs at any instant, so it has to be safe against live
// traffic — both the swap and the reload that applies it.
func TestQuotaForSwapWhileRequestsInFlight(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	var live atomic.Pointer[bucket.Quota]
	live.Store(&bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 50})

	pool := buildWith(t, config.Config{
		BaseURL: srv.URL,
		Quota:   bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 50},
	}, config.WithQuotaFor(func(string, bucket.Quota) bucket.Quota { return *live.Load() }))

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c := pool.Client(fmt.Sprintf("u%d", i))
			for {
				select {
				case <-stop:
					return
				default:
					c.Allow(context.Background())
					_ = c.Quota()
				}
			}
		}()
	}

	slow := bucket.Quota{Rate: bucket.PerMinute(600), Burst: 10}
	fast := bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 50}
	for range 200 {
		live.Store(&slow)
		pool.ReloadQuotas()
		live.Store(&fast)
	}
	close(stop)
	wg.Wait()
}
