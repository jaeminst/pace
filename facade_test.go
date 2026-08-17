// facade_test.go is the only test of the repository root, because the root is
// only a facade: pace.go declares aliases and six one-line functions, and
// everything they name is tested under internal/pace.
//
// What is left to check is the facade itself, which has two ways to be wrong
// and no way to notice at runtime. It can be incomplete — a name that used to
// exist and now does not — or it can be subtly untrue, declaring defined types
// where it means aliases. Both are compile-time properties, so most of this
// file is declarations rather than tests.
package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	impl "github.com/jaeminst/pace/internal/pace"
)

// zero is the uniform way to name a type's zero value: T{} does not work for
// an interface or a named float64, and this file has all three.
func zero[T any]() T {
	var z T
	return z
}

// Each pair assigns the zero value in both directions with no conversion. That
// is what distinguishes an alias from a defined type: had pace.go written
// `type Config impl.Config` instead of `type Config = impl.Config`, a caller's
// value would stop crossing the boundary, errors.As would stop matching, and a
// StateStore they implement would stop satisfying the interface the
// implementation asks for — all silently. These lines fail to compile first.
var (
	_ impl.AmbiguousPolicy    = zero[pace.AmbiguousPolicy]()
	_ pace.AmbiguousPolicy    = zero[impl.AmbiguousPolicy]()
	_ impl.BatchStateStore    = zero[pace.BatchStateStore]()
	_ pace.BatchStateStore    = zero[impl.BatchStateStore]()
	_ impl.Client             = zero[pace.Client]()
	_ pace.Client             = zero[impl.Client]()
	_ impl.Clock              = zero[pace.Clock]()
	_ pace.Clock              = zero[impl.Clock]()
	_ impl.Config             = zero[pace.Config]()
	_ pace.Config             = zero[impl.Config]()
	_ impl.ConfigError        = zero[pace.ConfigError]()
	_ pace.ConfigError        = zero[impl.ConfigError]()
	_ impl.DeadJob            = zero[pace.DeadJob]()
	_ pace.DeadJob            = zero[impl.DeadJob]()
	_ impl.DeadJobQuery       = zero[pace.DeadJobQuery]()
	_ pace.DeadJobQuery       = zero[impl.DeadJobQuery]()
	_ impl.EvictInfo          = zero[pace.EvictInfo]()
	_ pace.EvictInfo          = zero[impl.EvictInfo]()
	_ impl.EvictReason        = zero[pace.EvictReason]()
	_ pace.EvictReason        = zero[impl.EvictReason]()
	_ impl.Grant              = zero[pace.Grant]()
	_ pace.Grant              = zero[impl.Grant]()
	_ impl.JobInfo            = zero[pace.JobInfo]()
	_ pace.JobInfo            = zero[impl.JobInfo]()
	_ impl.JobPhase           = zero[pace.JobPhase]()
	_ pace.JobPhase           = zero[impl.JobPhase]()
	_ impl.Limit              = zero[pace.Limit]()
	_ pace.Limit              = zero[impl.Limit]()
	_ impl.LimitError         = zero[pace.LimitError]()
	_ pace.LimitError         = zero[impl.LimitError]()
	_ impl.Limiter            = zero[pace.Limiter]()
	_ pace.Limiter            = zero[impl.Limiter]()
	_ impl.Observer           = zero[pace.Observer]()
	_ pace.Observer           = zero[impl.Observer]()
	_ impl.QueueConfig        = zero[pace.QueueConfig]()
	_ pace.QueueConfig        = zero[impl.QueueConfig]()
	_ impl.Quota              = zero[pace.Quota]()
	_ pace.Quota              = zero[impl.Quota]()
	_ impl.QuotaErrorPolicy   = zero[pace.QuotaErrorPolicy]()
	_ pace.QuotaErrorPolicy   = zero[impl.QuotaErrorPolicy]()
	_ impl.Request            = zero[pace.Request]()
	_ pace.Request            = zero[impl.Request]()
	_ impl.RequestInfo        = zero[pace.RequestInfo]()
	_ pace.RequestInfo        = zero[impl.RequestInfo]()
	_ impl.Reservation        = zero[pace.Reservation]()
	_ pace.Reservation        = zero[impl.Reservation]()
	_ impl.Response           = zero[pace.Response]()
	_ pace.Response           = zero[impl.Response]()
	_ impl.RetryDecision      = zero[pace.RetryDecision]()
	_ pace.RetryDecision      = zero[impl.RetryDecision]()
	_ impl.RetryPolicy        = zero[pace.RetryPolicy]()
	_ pace.RetryPolicy        = zero[impl.RetryPolicy]()
	_ impl.SharedConfig       = zero[pace.SharedConfig]()
	_ pace.SharedConfig       = zero[impl.SharedConfig]()
	_ impl.SharedQuota        = zero[pace.SharedQuota]()
	_ pace.SharedQuota        = zero[impl.SharedQuota]()
	_ impl.State              = zero[pace.State]()
	_ pace.State              = zero[impl.State]()
	_ impl.StateStore         = zero[pace.StateStore]()
	_ pace.StateStore         = zero[impl.StateStore]()
	_ impl.Stats              = zero[pace.Stats]()
	_ pace.Stats              = zero[impl.Stats]()
	_ impl.TakeRequest        = zero[pace.TakeRequest]()
	_ pace.TakeRequest        = zero[impl.TakeRequest]()
	_ impl.ThrottleInfo       = zero[pace.ThrottleInfo]()
	_ pace.ThrottleInfo       = zero[impl.ThrottleInfo]()
	_ impl.TransportConfig    = zero[pace.TransportConfig]()
	_ pace.TransportConfig    = zero[impl.TransportConfig]()
	_ impl.UserState          = zero[pace.UserState]()
	_ pace.UserState          = zero[impl.UserState]()
	_ impl.WaitingSharedQuota = zero[pace.WaitingSharedQuota]()
	_ pace.WaitingSharedQuota = zero[impl.WaitingSharedQuota]()
)

// Every constant and sentinel error the package exported before the facade. A
// name dropped from pace.go fails to compile here rather than at a caller.
var _ = []any{
	pace.Inf,
	pace.AmbiguousAuto,
	pace.AmbiguousRetry,
	pace.AmbiguousPark,
	pace.EvictIdle,
	pace.EvictExplicit,
	pace.EvictShutdown,
	pace.JobClaimed,
	pace.JobCompleted,
	pace.JobRetrying,
	pace.JobDead,
	pace.QuotaFallbackLocal,
	pace.QuotaDeny,
	pace.QuotaAllow,
	pace.ErrBodyTooLarge,
	pace.ErrClosed,
	pace.ErrInvalidID,
	pace.ErrJobClaimed,
	pace.ErrNoQueue,
	pace.ErrQuotaUnavailable,
	pace.ErrStreamDurable,
}

// The six package-level functions, pinned by signature.
var (
	_ func(pace.Config) (*pace.Limiter, error)   = pace.New
	_ func(float64) pace.Limit                   = pace.PerSecond
	_ func(float64) pace.Limit                   = pace.PerMinute
	_ func(float64) pace.Limit                   = pace.PerHour
	_ func(time.Duration) pace.Limit             = pace.Every
	_ func(pace.TransportConfig) *http.Transport = pace.NewTransport
)

// TestTheFacadeCarriesRealTraffic is the one end-to-end assertion. The
// declarations above prove the types line up; this proves a Limiter built
// through pace.New actually rate-limits, reports through the alias types, and
// returns errors a caller can match.
func TestTheFacadeCarriesRealTraffic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	var throttled pace.ThrottleInfo
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerHour(1),
		Burst:   1,
		Observer: &pace.Observer{
			Throttled: func(_ context.Context, info pace.ThrottleInfo) { throttled = info },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lim.Close() }()

	ctx := context.Background()
	alice := lim.Client("alice")

	if _, err := alice.Get(ctx, "/"); err != nil {
		t.Fatalf("the first request through the facade failed: %v", err)
	}

	// The burst is spent, so the second must be refused rather than served.
	deadlined, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = alice.Get(deadlined, "/")

	// errors.As against the alias is the property most at risk: a defined type
	// here would compile and then never match what the implementation returns.
	var le *pace.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("errors.As(*pace.LimitError) did not match %#v", err)
	}
	if le.UserID != "alice" {
		t.Errorf("LimitError.UserID = %q, want alice", le.UserID)
	}
	if throttled.UserID != "alice" {
		t.Errorf("Observer.Throttled saw %q, want alice", throttled.UserID)
	}
	if got := lim.Stats().Requests; got != 2 {
		t.Errorf("Stats().Requests = %d, want 2", got)
	}
}

// facadeStore is written the way a caller writes one: against the names in the
// root package, with no knowledge that internal/pace exists.
type facadeStore struct{ saves int }

func (s *facadeStore) Save(context.Context, string, pace.State) error { s.saves++; return nil }
func (s *facadeStore) Load(context.Context, string) (pace.State, bool, error) {
	return pace.State{}, false, nil
}

// TestACallersStateStoreSatisfiesTheImplementation covers the direction that
// only breaks for third parties: pace.StateStore is what they compile against,
// impl.StateStore is what the Limiter stores it in. Nothing in the repository
// would notice if those came apart, because every other implementation of the
// interface is in the same package as the code that consumes it.
func TestACallersStateStoreSatisfiesTheImplementation(t *testing.T) {
	var _ impl.StateStore = (*facadeStore)(nil)

	st := &facadeStore{}
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(600),
		Burst:   10,
		Store:   st,
	})
	if err != nil {
		t.Fatal(err)
	}
	lim.Client("alice").Allow(context.Background())
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}
	if st.saves == 0 {
		t.Error("the caller's store was never written to")
	}
}
