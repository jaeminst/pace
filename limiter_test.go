package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/jaeminst/pace"
)

func newTestLimiter(t *testing.T, opts ...func(*pace.Config)) (*pace.Limiter, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := pace.Config{BaseURL: srv.URL, Rate: pace.PerMinute(60), Burst: 1}
	for _, o := range opts {
		o(&cfg)
	}
	lim, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	return lim, srv
}

// TestClientsShareLimiterState is the property the Limiter/Client split makes
// structural: two handles for the same user are two views of one bucket, and
// handles for different users are independent.
func TestClientsShareLimiterState(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *pace.Config) { c.Burst = 1 })
	ctx := context.Background()

	// Two independently derived handles for the same identity.
	alice1 := lim.Client("alice")
	alice2 := lim.Client("alice")

	if _, err := alice1.Request(ctx); err != nil {
		t.Fatalf("alice1: %v", err)
	}
	// alice2 must see the token alice1 spent, not a fresh bucket.
	if got := alice2.Tokens(); got >= 1 {
		t.Errorf("alice2.Tokens() = %v after alice1 spent the burst, want < 1", got)
	}

	// A different identity is untouched.
	if got := lim.Client("bob").Tokens(); got != -1 {
		t.Errorf("bob.Tokens() = %v before first use, want -1 (no state)", got)
	}
	if _, err := lim.Client("bob").Request(ctx); err != nil {
		t.Errorf("bob was throttled by alice's traffic: %v", err)
	}
}

// TestClientCarriesUserID guards against the empty-identity footgun the old
// Config.Name field allowed, where an unset name silently rate-limited every
// caller as the "" user.
func TestClientCarriesUserID(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if got := lim.Client("alice").UserID(); got != "alice" {
		t.Errorf("UserID() = %q, want %q", got, "alice")
	}
}

// TestCloseIsIdempotentAndReportsOnce pins that repeated Close calls agree,
// which callers rely on when both a defer and an explicit shutdown path run.
func TestCloseIsIdempotentAndReportsOnce(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if err := lim.Close(); err != nil {
		t.Errorf("first Close = %v, want nil", err)
	}
	if err := lim.Close(); err != nil {
		t.Errorf("second Close = %v, want nil like the first", err)
	}
}

func TestCloseConcurrent(t *testing.T) {
	lim, _ := newTestLimiter(t)
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Go(func() { errs[i] = lim.Close() })
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("Close #%d = %v, want nil", i, err)
		}
	}
}

// TestClosingOneLimiterDoesNotAffectAnother is the counterpart to the bug the
// split removed: lifecycle used to hang off a per-user handle, so closing a
// derived handle tore down the limiter every other user shared.
func TestClosingOneLimiterDoesNotAffectAnother(t *testing.T) {
	limA, _ := newTestLimiter(t)
	limB, _ := newTestLimiter(t)
	ctx := context.Background()

	if err := limA.Close(); err != nil {
		t.Fatalf("close limA: %v", err)
	}
	if _, err := limA.Client("alice").Request(ctx); !errors.Is(err, pace.ErrClosed) {
		t.Errorf("limA after Close = %v, want ErrClosed", err)
	}
	if _, err := limB.Client("alice").Request(ctx); err != nil {
		t.Errorf("limB was affected by closing limA: %v", err)
	}
}

func TestShutdownReportsCloseErrorWhenNoDeadlineExceeded(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if err := lim.Shutdown(context.Background()); err != nil {
		t.Errorf("Shutdown = %v, want nil", err)
	}
	// Shutdown routes through the same closeOnce, so a later Close agrees.
	if err := lim.Close(); err != nil {
		t.Errorf("Close after Shutdown = %v, want nil", err)
	}
}
