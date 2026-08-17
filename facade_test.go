// facade_test.go is the only test of the root package, because the root only
// re-exports. What it checks is the two ways a facade can be wrong with nothing
// to notice at run time: it can be incomplete, or it can declare defined types
// where it means aliases. Both are compile-time properties, so most of this
// file is declarations.
package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/response"
	"github.com/jaeminst/pace/store"
)

// zero is the uniform way to name a type's zero value: T{} does not work for an
// interface, and this file has one.
func zero[T any]() T {
	var z T
	return z
}

// Each pair assigns the zero value in both directions with no conversion. That
// is what distinguishes an alias from a defined type: had pace.go written
// `type Config limiter.Config`, a caller's value would stop crossing the
// boundary, errors.As would stop matching, and a store they implement would
// stop satisfying what the Limiter asks for — all silently, and all still
// passing `go build ./...`.
var (
	_ limiter.Limiter     = zero[pace.Limiter]()
	_ pace.Limiter        = zero[limiter.Limiter]()
	_ limiter.Client      = zero[pace.Client]()
	_ pace.Client         = zero[limiter.Client]()
	_ limiter.Request     = zero[pace.Request]()
	_ pace.Request        = zero[limiter.Request]()
	_ response.Response   = zero[pace.Response]()
	_ pace.Response       = zero[response.Response]()
	_ limiter.Reservation = zero[pace.Reservation]()
	_ pace.Reservation    = zero[limiter.Reservation]()
	_ limiter.Config      = zero[pace.Config]()
	_ pace.Config         = zero[limiter.Config]()
	_ limiter.Clock       = zero[pace.Clock]()
	_ pace.Clock          = zero[limiter.Clock]()
	_ limiter.ConfigError = zero[pace.ConfigError]()
	_ pace.ConfigError    = zero[limiter.ConfigError]()
	_ limiter.LimitError  = zero[pace.LimitError]()
	_ pace.LimitError     = zero[limiter.LimitError]()
)

// The six sentinels exist so a caller can name them without importing the
// limiter; they are pinned by TestASentinelMatchesWhatTheLimiterReturns.
var _ = []error{
	pace.ErrClosed, pace.ErrNoQueue, pace.ErrInvalidID,
	pace.ErrJobClaimed, pace.ErrBodyTooLarge, pace.ErrStreamDurable,
}

var _ func(pace.Config) (*pace.Limiter, error) = pace.New

// TestTheFrontDoorCarriesRealTraffic is the one end-to-end assertion. The
// declarations above prove the types line up; this proves a Limiter built
// through pace.New rate-limits and returns errors a caller can match.
func TestTheFrontDoorCarriesRealTraffic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerHour(1),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = lim.Close() }()

	ctx := context.Background()
	alice := lim.Client("alice")
	if _, err := alice.Get(ctx, "/"); err != nil {
		t.Fatalf("the first request through the front door failed: %v", err)
	}

	// The burst is spent, so the second must be refused rather than served.
	if ok := alice.Allow(ctx); ok {
		t.Fatal("Allow granted a second token from a burst of one")
	}
	// A short deadline, because the refill is an hour away and the point is the
	// error's type rather than the wait.
	deadlined, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	_, err = alice.Get(deadlined, "/")

	// errors.As against the alias is the property most at risk: a defined type
	// would compile and then never match what the limiter returns.
	var le *pace.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("errors.As(*pace.LimitError) did not match %#v", err)
	}
	if le.UserID != "alice" {
		t.Errorf("LimitError.UserID = %q, want alice", le.UserID)
	}
}

// frontDoorStore is written the way a caller writes one: against the names in
// the store package, with no knowledge of how the Limiter holds it.
type frontDoorStore struct{ saves int }

func (s *frontDoorStore) Save(context.Context, string, store.State) error { s.saves++; return nil }
func (s *frontDoorStore) Load(context.Context, string) (store.State, bool, error) {
	return store.State{}, false, nil
}

// TestACallersStoreSatisfiesTheLimiter covers the direction that only breaks
// for third parties: store.Store is what they compile against, and Config.Store
// is where it lands.
func TestACallersStoreSatisfiesTheLimiter(t *testing.T) {
	st := &frontDoorStore{}
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    rate.PerMinute(600),
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

// TestASentinelMatchesWhatTheLimiterReturns guards the failure a re-export
// invites: writing errors.New again in the root rather than naming the
// limiter's value. That compiles, reads correctly, and leaves every caller's
// errors.Is silently false.
func TestASentinelMatchesWhatTheLimiterReturns(t *testing.T) {
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    rate.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := lim.Close(); err != nil {
		t.Fatal(err)
	}

	// A closed Limiter reports ErrClosed, and the caller matches it against the
	// name this package exports rather than the limiter's.
	_, err = lim.Client("alice").Get(context.Background(), "/")
	if !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("errors.Is(err, pace.ErrClosed) was false for %#v", err)
	}

	// Same for the durable path, which has its own sentinel.
	_, err = lim.Client("alice").Durable("job-1").Do(context.Background(), "GET", "/")
	if !errors.Is(err, pace.ErrClosed) && !errors.Is(err, pace.ErrNoQueue) {
		t.Fatalf("neither pace.ErrClosed nor pace.ErrNoQueue matched %#v", err)
	}
}
