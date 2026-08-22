package limiter_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

func newTestLimiter(t *testing.T, opts ...func(*config.Config)) (*client.Pool, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	return build(t, config.Config{BaseURL: srv.URL, Quota: bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1}}, opts...), srv
}

// TestClientsShareLimiterState is the property the Limiter/Client split makes
// structural: two handles for the same key are two views of one bucket, and
// handles for different keys are independent.
func TestClientsShareLimiterState(t *testing.T) {
	lim, _ := newTestLimiter(t, func(c *config.Config) { c.Quota.Burst = 1 })
	ctx := context.Background()

	// Two independently derived handles for the same identity.
	alice1 := lim.Client("alice")
	alice2 := lim.Client("alice")

	if err := alice1.Wait(ctx); err != nil {
		t.Fatalf("alice1: %v", err)
	}
	// alice2 must see the token alice1 spent, not a fresh bucket.
	if got := tokensOf(alice2); got >= 1 {
		t.Errorf("alice2 sees %v tokens after alice1 spent the burst, want < 1", got)
	}

	// A different identity is untouched, and reports having no state at all.
	if n, ok := lim.Client("bob").Tokens(); ok || n != 0 {
		t.Errorf("bob.Tokens() = (%v, %v) before first use, want (0, false)", n, ok)
	}
	if err := lim.Client("bob").Wait(ctx); err != nil {
		t.Errorf("bob was throttled by alice's traffic: %v", err)
	}
}

// TestClientCarriesKey guards against the empty-identity footgun the old
// Config.Name field allowed, where an unset name silently rate-limited every
// caller as the "" key.
func TestClientCarriesKey(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if got := lim.Client("alice").Key(); got != "alice" {
		t.Errorf("Key() = %q, want %q", got, "alice")
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
// split removed: lifecycle used to hang off a per-key handle, so closing a
// derived handle tore down the limiter every other key shared.
func TestClosingOneLimiterDoesNotAffectAnother(t *testing.T) {
	limA, _ := newTestLimiter(t)
	limB, _ := newTestLimiter(t)
	ctx := context.Background()

	if err := limA.Close(); err != nil {
		t.Fatalf("close limA: %v", err)
	}
	if err := limA.Client("alice").Wait(ctx); !errors.Is(err, limiter.ErrClosed) {
		t.Errorf("limA after Close = %v, want ErrClosed", err)
	}
	if err := limB.Client("alice").Wait(ctx); err != nil {
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

func TestConfigShards(t *testing.T) {
	// A non-power-of-two count is rounded up rather than rejected, and key
	// isolation must hold whatever the shard count is.
	for _, shards := range []int{0, 1, 3, 64} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			lim, _ := newTestLimiter(t, func(c *config.Config) {
				c.Shards = shards
				c.Quota.Burst = 1
				c.Quota.Rate = bucket.PerMinute(6)
			})
			if !lim.Client("alice").Allow(context.Background()) {
				t.Fatal("alice could not take her first token")
			}
			if lim.Client("alice").Allow(context.Background()) {
				t.Error("alice took a second token from a burst of 1")
			}
			if !lim.Client("bob").Allow(context.Background()) {
				t.Error("bob was blocked by alice's traffic")
			}
		})
	}
}

func newTestLimiterOn(t *testing.T, baseURL string, opts ...func(*config.Config)) (*client.Pool, string) {
	t.Helper()
	return build(t, config.Config{BaseURL: baseURL, Quota: bucket.Quota{Rate: bucket.PerMinute(6000), Burst: 100}}, opts...), baseURL
}
