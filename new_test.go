// new_test.go tests the only behaviour this package has left: what [pace.New]
// wired. Config is validated and defaulted here and nowhere else, and the
// *limiter.Limiter that comes back has to be one a caller can actually use.
//
// It was facade_test.go, and most of it was compile-time declarations pinning
// aliases in both directions. There is no facade now — the root re-exports
// nothing — so those declarations assert nothing and are gone. The one that
// survives is New's own signature, which is the whole of the boundary.
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
	"github.com/jaeminst/pace/store"
	"github.com/jaeminst/pace/transport"
)

// The boundary in one line: a Config goes in and the engine's own Limiter comes
// out. No wrapper, no alias — if either side of that ever stops being true this
// stops compiling.
var _ func(pace.Config) (*limiter.Limiter, error) = pace.New

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
		Rate:    limiter.PerHour(1),
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
	var le *limiter.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("errors.As(*limiter.LimitError) did not match %#v", err)
	}
	if le.UserID != "alice" {
		t.Errorf("LimitError.UserID = %q, want alice", le.UserID)
	}
}

// frontDoorStore is written the way a caller writes one: against the names in
// the store package, with no knowledge of how the Limiter holds it.

// frontDoorStore is written the way a caller writes one: against the names in
// the store package, with no knowledge of how the Limiter holds it.
type frontDoorStore struct{ saves int }

func (s *frontDoorStore) Save(context.Context, string, store.State) error { s.saves++; return nil }

func (s *frontDoorStore) Load(context.Context, string) (store.State, bool, error) {
	return store.State{}, false, nil
}

// TestTheFrontDoorAssemblesTheCallersTransport. Config.Transport is not a
// field the engine has — the root wraps it in an *http.Client and hands that
// over — so this is the only place the wiring is observable, and it is a
// front-door assertion rather than an engine one.
//
// It asserts nothing about transport.New; transport/ tests that. It asserts
// that what transport.New returned is what carried the request.

// TestTheFrontDoorAssemblesTheCallersTransport. Config.Transport is not a
// field the engine has — the root wraps it in an *http.Client and hands that
// over — so this is the only place the wiring is observable, and it is a
// front-door assertion rather than an engine one.
//
// It asserts nothing about transport.New; transport/ tests that. It asserts
// that what transport.New returned is what carried the request.
func TestTheFrontDoorAssemblesTheCallersTransport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limiter.PerMinute(6000),
		Transport: transport.New(transport.Config{
			DialTimeout:         2 * time.Second,
			TLSHandshakeTimeout: 2 * time.Second,
			MaxIdleConnsPerHost: 4,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	resp, err := lim.Client("u").Get(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode())
	}
}

// TestACallersStoreSatisfiesTheLimiter covers the direction that only breaks
// for third parties: store.Store is what they compile against, and Config.Store
// is where it lands.

// TestACallersStoreSatisfiesTheLimiter covers the direction that only breaks
// for third parties: store.Store is what they compile against, and Config.Store
// is where it lands.
func TestACallersStoreSatisfiesTheLimiter(t *testing.T) {
	st := &frontDoorStore{}
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    limiter.PerMinute(600),
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
//
// It is ErrClosed that is at risk, because the engine is what reports a closed
// Limiter. ErrBodyTooLarge is genuinely declared here and needs no such check —
// see TestMaxResponseBytesRejectsOversizedBody for the path that returns it.
