package limiter_test

import (
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/jaeminst/pace/limiter"
)

// good is a Config that New accepts, so each case below can be one field wrong
// rather than a fresh literal whose other fields might be doing the work.
func good() limiter.Config {
	return limiter.Config{
		BaseURL:      "http://example.invalid",
		HTTPClient:   &http.Client{},
		Quota:        func(string) limiter.Quota { return limiter.Quota{Rate: limiter.PerMinute(60), Burst: 1} },
		Now:          time.Now,
		Logger:       slog.New(slog.DiscardHandler),
		Shards:       8,
		IdleExpiry:   time.Minute,
		GCInterval:   time.Minute,
		StoreTimeout: time.Second,
	}
}

// TestNewPanicsOnConfigItCannotUse is the vtable rule: a value this package
// cannot work with fails where it is written, naming the field, rather than as
// a nil call on the first request or on a background goroutine three calls
// later.
//
// A nil Store is deliberately absent from this table. It is the one meaningful
// zero here — no persistence is how pace runs unless a caller configures some —
// and TestNewAcceptsNoStore below pins that.
func TestNewPanicsOnConfigItCannotUse(t *testing.T) {
	tests := []struct {
		name string
		bend func(*limiter.Config)
		want string
	}{
		{"no BaseURL", func(c *limiter.Config) { c.BaseURL = "" }, "BaseURL is required"},
		{"no HTTPClient", func(c *limiter.Config) { c.HTTPClient = nil }, "HTTPClient is required"},
		{"no Quota", func(c *limiter.Config) { c.Quota = nil }, "Quota, Now and Logger are required"},
		{"no Now", func(c *limiter.Config) { c.Now = nil }, "Quota, Now and Logger are required"},
		{"no Logger", func(c *limiter.Config) { c.Logger = nil }, "Quota, Now and Logger are required"},
		{"zero Shards", func(c *limiter.Config) { c.Shards = 0 }, "Shards must be a positive power of two"},
		{"Shards not a power of two", func(c *limiter.Config) { c.Shards = 6 }, "Shards must be a positive power of two"},
		{"zero IdleExpiry", func(c *limiter.Config) { c.IdleExpiry = 0 }, "IdleExpiry, GCInterval and StoreTimeout must be positive"},
		{"zero GCInterval", func(c *limiter.Config) { c.GCInterval = 0 }, "IdleExpiry, GCInterval and StoreTimeout must be positive"},
		{"zero StoreTimeout", func(c *limiter.Config) { c.StoreTimeout = 0 }, "IdleExpiry, GCInterval and StoreTimeout must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				got, ok := recover().(string)
				switch {
				case !ok:
					t.Errorf("panicked with %v, want a string naming the field", got)
				case !strings.HasPrefix(got, "limiter: "):
					t.Errorf("panic = %q, want it prefixed with the package name", got)
				case !strings.Contains(got, tt.want):
					t.Errorf("panic = %q, want it to mention %q", got, tt.want)
				}
			}()
			cfg := good()
			tt.bend(&cfg)
			limiter.New(cfg)
			t.Error("New did not panic")
		})
	}
}

// TestNewAcceptsNoStore: StoreTimeout is still required without one, because it
// bounds the load that happens when a user is first seen whether or not
// anything is written back.
func TestNewAcceptsNoStore(t *testing.T) {
	lim := limiter.New(good())
	t.Cleanup(func() { _ = lim.Close() })
	if lim == nil {
		t.Fatal("New returned nil for a Config with no Store")
	}
}
