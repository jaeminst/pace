package limiter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/store/memory"
)

// tierLimiter builds a Limiter whose users are graded by a QuotaFor closure.
func tierLimiter(t *testing.T, url string, tiers map[string]config.Quota) *client.Pool {
	t.Helper()
	return build(t, config.Config{
		BaseURL:  url,
		Rate:     config.PerMinute(60), // the default tier
		Burst:    2,
		Clock:    newFakeClock(),
		QuotaFor: func(userID string) config.Quota { return tiers[userID] },
	})
}

// TestQuotaForGradesUsersIndependently is the feature the package name implies
// and did not have: Rate and Burst were global, so pace could isolate users
// from each other but not tell a paying one from a free one.
func TestQuotaForGradesUsersIndependently(t *testing.T) {
	lim := tierLimiter(t, "http://example.invalid", map[string]config.Quota{
		"paid": {Rate: config.PerMinute(600), Burst: 50},
		// "free" is absent, so it gets the zero Quota and thus the defaults.
	})

	paid, free := lim.Client("paid"), lim.Client("free")

	if got, want := paid.Quota(), (config.Quota{Rate: config.PerMinute(600), Burst: 50}); got != want {
		t.Errorf("paid quota = %+v, want %+v", got, want)
	}
	if got, want := free.Quota(), (config.Quota{Rate: config.PerMinute(60), Burst: 2}); got != want {
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
		t.Errorf("the paid user got %d of 50 requests, want all: burst is 50", paidAllowed)
	}
	if freeAllowed != 2 {
		t.Errorf("the free user got %d requests, want 2: burst is 2", freeAllowed)
	}
}

// TestQuotaPartialOverrideFallsBackPerField: each field falls back on its own,
// so a tier that only raises the rate keeps the default ceiling.
// TestThrottleReportsTheUsersOwnQuota: LimitError and ThrottleInfo have always
// documented their Limit and Burst as "the configuration in force for that
// user". Until QuotaFor existed there was only one configuration, so reading
// Config.Rate happened to be right. It is not any more.
func TestThrottleReportsTheUsersOwnQuota(t *testing.T) {
	var infos []observe.ThrottleInfo
	var mu sync.Mutex

	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Rate:    config.PerMinute(60),
		Burst:   1,
		Clock:   newFakeClock(),
		QuotaFor: func(userID string) config.Quota {
			if userID == "paid" {
				return config.Quota{Rate: config.PerMinute(600), Burst: 3}
			}
			return config.Quota{}
		},
		Observer: &observe.Observer{
			Throttled: func(_ context.Context, info observe.ThrottleInfo) {
				mu.Lock()
				defer mu.Unlock()
				infos = append(infos, info)
			},
		},
	})
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
	if infos[0].Limit != float64(config.PerMinute(600)) || infos[0].Burst != 3 {
		t.Errorf("ThrottleInfo reported limit %v burst %d, want the paid user's 600/min burst 3",
			infos[0].Limit, infos[0].Burst)
	}
}

func TestLimitErrorReportsTheUsersOwnQuota(t *testing.T) {
	// A real clock here: limiter.Limiter.Wait schedules against the wall clock
	// regardless of Config.Clock, so a frozen fake would leave the bucket
	// looking full to Wait and empty to everything else. One token per hour
	// makes the refill far too slow to race the 10ms deadline.
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Rate:    config.PerMinute(60),
		Burst:   1,
		QuotaFor: func(string) config.Quota {
			return config.Quota{Rate: config.PerHour(1), Burst: 1}
		},
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
	if le.Limit != config.PerHour(1) || le.Burst != 1 {
		t.Errorf("LimitError reported %v burst %d, want the user's own 1/hour burst 1", le.Limit, le.Burst)
	}
}

// TestReloadQuotasAppliesToLiveBuckets: before this, changing a tier meant
// building a new Limiter, which dropped every in-memory bucket in the process.
func TestReloadQuotasAppliesToLiveBuckets(t *testing.T) {
	var mu sync.Mutex
	tiers := map[string]config.Quota{"alice": {Rate: config.PerMinute(60), Burst: 2}}

	clk := newFakeClock()
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Rate:    config.PerMinute(60),
		Burst:   2,
		Clock:   clk,
		QuotaFor: func(userID string) config.Quota {
			mu.Lock()
			defer mu.Unlock()
			return tiers[userID]
		},
	})
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
	tiers["alice"] = config.Quota{Rate: config.PerMinute(600), Burst: 20}
	mu.Unlock()

	// Nothing changes until the reload: the bucket is what enforces the quota.
	if got := alice.Quota().Burst; got != 2 {
		t.Errorf("burst = %d before ReloadQuotas, want the old 2", got)
	}

	lim.ReloadQuotas()

	if got, want := alice.Quota(), (config.Quota{Rate: config.PerMinute(600), Burst: 20}); got != want {
		t.Errorf("quota after reload = %+v, want %+v", got, want)
	}
	// The upgrade must not hand out a full new bucket, or a user could farm
	// tokens by triggering reloads.
	if after := tokensOf(alice); after != before {
		t.Errorf("tokens went from %v to %v across the reload; accrued tokens must be kept as they are",
			before, after)
	}
}

// TestReloadQuotasIgnoresUsersNotInMemory: they need nothing, because their
// bucket is built from QuotaFor when they next appear.
func TestReloadQuotasIgnoresUsersNotInMemory(t *testing.T) {
	lim := tierLimiter(t, "http://example.invalid", map[string]config.Quota{
		"ghost": {Rate: config.PerMinute(600), Burst: 50},
	})
	lim.ReloadQuotas() // nobody is in memory; must not panic

	if got := lim.Stats().Users; got != 0 {
		t.Errorf("Users = %d, want 0: ReloadQuotas must not create anyone", got)
	}
	if got := lim.Client("ghost").Quota().Burst; got != 50 {
		t.Errorf("burst = %d on first sight, want 50 from QuotaFor", got)
	}
}

// TestQuotaForRunsOutsideTheShardLock is the regression guard for the one
// constraint this feature has. QuotaFor is caller code; running it under a
// shard write lock would let a slow implementation stall every user who hashes
// to that shard, which is the same mistake userFor's loadState call already
// documents avoiding.
func TestQuotaForRunsOutsideTheShardLock(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 1)

	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Rate:    config.PerMinute(6000),
		Burst:   10,
		Shards:  1, // one shard, so every user provably collides
		QuotaFor: func(userID string) config.Quota {
			if userID == "slow" {
				select {
				case entered <- struct{}{}:
				default:
				}
				<-release
			}
			return config.Quota{}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	go func() { lim.Client("slow").Allow(context.Background()) }()
	<-entered

	// A different user on the same shard must not be stuck behind it.
	done := make(chan struct{})
	go func() {
		defer close(done)
		lim.Client("other").Allow(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("a second user blocked behind a slow QuotaFor: it is being called under the shard lock")
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
		Rate:    config.PerMinute(6000),
		Burst:   10,
		Shards:  1,
		QuotaFor: func(string) config.Quota {
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
			return config.Quota{}
		},
	})
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

// TestRestoredUserIsClampedToTheCurrentBurst: persisted state carries no quota,
// so a demoted user must not be handed back a ceiling they no longer have.
func TestRestoredUserIsClampedToTheCurrentBurst(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	st := memory.New()
	burst := 50
	newLim := func() *client.Pool {
		t.Helper()
		lim, err := client.New(config.Config{
			BaseURL:  srv.URL,
			Rate:     config.PerMinute(60),
			Burst:    2,
			Store:    st,
			Clock:    newFakeClock(),
			QuotaFor: func(string) config.Quota { return config.Quota{Burst: burst} },
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

	lim.Client("alice").Allow(context.Background()) // bring the user back into memory
	if got := lim.Client("alice").Quota().Burst; got != 3 {
		t.Fatalf("burst = %d after demotion, want 3", got)
	}
	if got := tokensOf(lim.Client("alice")); got > 3 {
		t.Errorf("tokens = %v after a demotion to burst 3, want at most 3: "+
			"saved state must not resurrect an old ceiling", got)
	}
}

// TestNonFiniteRateIsNotAcceptedSilently: config.Limit is a float64, so a caller
// can write Limit(math.Inf(1)) or a NaN. Both passed validate — its only check
// was Rate <= 0, which neither trips — and produced a bucket whose token count
// was NaN, refusing every request forever. Found by fuzzing RestoreBucket.
// TestReloadQuotasReadsTheClockPerUser: ReloadQuotas captured one `now` and
// passed it to every user across all 256 shards, after however long QuotaFor
// took for each. SetQuotaAt writes that instant as the bucket's last-updated
// time, so any user whose bucket had already advanced past it was rewound — and
// a rewound interval is refilled a second time, which is a silent quota grant
// on a maintenance call.
//
// The reachable case is a user who makes a request while the walk is in
// progress, which is inherently racy to stage. This asserts the fix directly
// instead: the clock is read where it is used, once per user, rather than once
// for the whole walk.
func TestReloadQuotasReadsTheClockPerUser(t *testing.T) {
	clk := newFakeClock()
	lim, err := client.New(config.Config{
		BaseURL: "http://example.invalid",
		Rate:    config.PerMinute(600),
		Burst:   10,
		Clock:   clk,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	users := []string{"a", "b", "c", "d", "e", "f", "g", "h"}
	for _, u := range users {
		lim.Client(u).Allow(context.Background())
	}

	before := clk.callCount()
	lim.ReloadQuotas()
	got := clk.callCount() - before

	if got < len(users) {
		t.Errorf("ReloadQuotas read the clock %d times for %d users in memory; it must read it "+
			"where it stamps each bucket, not once for the whole walk", got, len(users))
	}
}
