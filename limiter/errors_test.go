package limiter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaeminst/pace/limit"
	pace "github.com/jaeminst/pace/limiter"
)

func TestConfigErrorFromNew(t *testing.T) {
	tests := []struct {
		name      string
		cfg       pace.Config
		wantField string
	}{
		{"missing BaseURL", pace.Config{Rate: limit.PerMinute(60)}, "BaseURL"},
		{"zero Rate", pace.Config{BaseURL: "http://x"}, "Rate"},
		{"negative Rate", pace.Config{BaseURL: "http://x", Rate: -1}, "Rate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pace.New(tt.cfg)
			if err == nil {
				t.Fatal("New = nil error, want ConfigError")
			}
			var ce *pace.ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("New = %v, want *ConfigError", err)
			}
			if ce.Field != tt.wantField {
				t.Errorf("ConfigError.Field = %q, want %q", ce.Field, tt.wantField)
			}
		})
	}
}

// TestLimitErrorNotErrClosed pins the distinction that a caller acts on. The
// limiter reports "would exceed context deadline" without waiting, leaving the
// caller's ctx.Err() nil; inferring "the client must have closed" from that
// told callers the Client was shut down when it was very much open.
func TestLimitErrorNotErrClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Client("alice").Wait(ctx); err != nil {
		t.Fatalf("first request: %v", err)
	}

	// The bucket is empty and refills in ten seconds; this deadline cannot be met.
	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	err = client.Client("alice").Wait(deadlined)
	if err == nil {
		t.Fatal("second request succeeded, want a rate-limit error")
	}
	if errors.Is(err, pace.ErrClosed) {
		t.Fatalf("got ErrClosed, but the client is open: %v", err)
	}

	var le *pace.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("got %T (%v), want *pace.LimitError", err, err)
	}
	if le.UserID != "alice" {
		t.Errorf("LimitError.UserID = %q, want %q", le.UserID, "alice")
	}
	if le.Limit != limit.PerMinute(6) {
		t.Errorf("LimitError.Limit = %v, want %v", le.Limit, limit.PerMinute(6))
	}
	if le.Burst != 1 {
		t.Errorf("LimitError.Burst = %d, want 1", le.Burst)
	}
}

// TestErrClosedStillReportedWhenClosed guards the other side of the same
// branch: a genuinely closed Client must still say so.
func TestErrClosedStillReportedWhenClosed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer srv.Close()

	client, err := pace.New(pace.Config{BaseURL: srv.URL, Rate: limit.PerMinute(60), Burst: 1})
	if err != nil {
		t.Fatal(err)
	}
	client.Close()

	if err := client.Client("alice").Wait(context.Background()); !errors.Is(err, pace.ErrClosed) {
		t.Fatalf("Request after Close = %v, want ErrClosed", err)
	}
}

func TestLimitErrorMessageAndUnwrap(t *testing.T) {
	base := errors.New("boom")
	e := &pace.LimitError{UserID: "bob", Limit: limit.PerMinute(30), Burst: 5, Err: base}
	if !errors.Is(e, base) {
		t.Error("LimitError does not unwrap to its cause")
	}
	if got, want := e.Error(), `pace: rate limit for "bob" (30/min, burst 5): boom`; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}

	withDelay := &pace.LimitError{UserID: "bob", Limit: limit.PerMinute(30), Burst: 5, Delay: 2 * time.Second, Err: base}
	if got, want := withDelay.Error(), `pace: rate limit for "bob" (30/min, burst 5): boom; retry in 2s`; got != want {
		t.Errorf("Error() with delay = %q, want %q", got, want)
	}
}

func TestConfigErrorMessage(t *testing.T) {
	cause := errors.New("required")
	tests := []struct {
		err  *pace.ConfigError
		want string
	}{
		{&pace.ConfigError{Field: "BaseURL", Err: cause}, "pace: invalid Config.BaseURL: required"},
		{&pace.ConfigError{Field: "Rate", Value: limit.Limit(0), Err: cause}, "pace: invalid Config.Rate (0): required"},
		{&pace.ConfigError{Field: "Burst", Value: -3}, "pace: invalid Config.Burst: -3"},
		{&pace.ConfigError{Field: "Shards"}, "pace: invalid Config.Shards"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
	if !errors.Is(&pace.ConfigError{Field: "X", Err: cause}, cause) {
		t.Error("ConfigError does not unwrap to its cause")
	}
}

// TestLimitErrorCarriesDelay: the field callers branch on has to be populated.
// It was documented as "how long the caller would have had to wait" and left at
// zero, which a godoc example exposed by printing it.
func TestLimitErrorCarriesDelay(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6), // one token every 10s
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	ctx := context.Background()
	alice := lim.Client("alice")
	if _, err := alice.Get(ctx, "/"); err != nil {
		t.Fatal(err)
	}

	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = alice.Get(deadlined, "/")

	var le *pace.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("got %T (%v), want *pace.LimitError", err, err)
	}
	if le.Delay < 5*time.Second || le.Delay > 11*time.Second {
		t.Errorf("LimitError.Delay = %v, want roughly 10s", le.Delay)
	}
}
