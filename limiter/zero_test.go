// zero_test.go is the engine stating what it needs, checked at the door.
//
// There is no vtable any more: New takes the caller's own config.Config, so
// these are the six fields of it this package actually reads and the panics it
// raises when one is unusable. A zero field here is a nil call or a division on
// the first request rather than a default.
//
// Config.Quota is in the table; what a config.WithQuotaFor option returns is
// not. That value arrives on a request goroutine long after New, so it is
// quotaFor's to normalise. The field is checkable here because it is a value,
// which is the whole argument for it being one.

package limiter_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

// good is a resolved Config New accepts, so each case below can be one field
// wrong rather than a fresh literal whose other fields might be doing the work.
//
// It is built by hand rather than by Resolve so that the table can reach states
// Resolve would have fixed — which is the point: these panics exist for the
// caller who assembles the pieces themselves.
func good() config.Config {
	return config.Config{
		BaseURL:      "http://example.invalid",
		Quota:        bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1},
		Clock:        stdClock{},
		Logger:       slog.New(slog.DiscardHandler),
		Shards:       8,
		IdleExpiry:   time.Minute,
		GCInterval:   time.Minute,
		StoreTimeout: time.Second,
	}
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }

// TestNewPanicsOnAConfigItCannotUse: a value the engine cannot work with fails
// where it is written, naming the field, rather than on a background goroutine
// three calls later.
//
// A nil Store is deliberately absent. It is the one meaningful zero here — no
// persistence is how pace runs unless a caller configures some — and
// TestNewAcceptsNoStore below pins that.
func TestNewPanicsOnAConfigItCannotUse(t *testing.T) {
	tests := []struct {
		name string
		bend func(*config.Config)
		want string
	}{
		{"zero Quota", func(c *config.Config) { c.Quota = bucket.Quota{} }, "Config.Quota needs a rate above zero"},
		{"zero Burst", func(c *config.Config) { c.Quota.Burst = 0 }, "Config.Quota needs a rate above zero"},
		{"no Clock", func(c *config.Config) { c.Clock = nil }, "Config.Clock and Config.Logger are required"},
		{"no Logger", func(c *config.Config) { c.Logger = nil }, "Config.Clock and Config.Logger are required"},
		{"zero Shards", func(c *config.Config) { c.Shards = 0 }, "Config.Shards must be a positive power of two"},
		{"Shards not a power of two", func(c *config.Config) { c.Shards = 6 }, "Config.Shards must be a positive power of two"},
		{"zero IdleExpiry", func(c *config.Config) { c.IdleExpiry = 0 }, "Config.IdleExpiry, Config.GCInterval and Config.StoreTimeout must be positive"},
		{"zero GCInterval", func(c *config.Config) { c.GCInterval = 0 }, "Config.IdleExpiry, Config.GCInterval and Config.StoreTimeout must be positive"},
		{"zero StoreTimeout", func(c *config.Config) { c.StoreTimeout = 0 }, "Config.IdleExpiry, Config.GCInterval and Config.StoreTimeout must be positive"},
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
// bounds the load that happens when a key is first seen whether or not
// anything is written back.
func TestNewAcceptsNoStore(t *testing.T) {
	lim := limiter.New(good())
	t.Cleanup(func() { _ = lim.Close() })
	if lim == nil {
		t.Fatal("New returned nil for a Config with no Store")
	}
}

// TestAResolvedConfigIsOneNewAccepts is the property that makes the table above
// worth having: the Config a caller actually ends up with, from Resolve, is one
// the engine takes. Without it the table could be pinning requirements that
// Resolve quietly fails to satisfy.
func TestAResolvedConfigIsOneNewAccepts(t *testing.T) {
	cfg, err := config.Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60)},
	}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	lim := limiter.New(cfg)
	t.Cleanup(func() { _ = lim.Close() })
}
