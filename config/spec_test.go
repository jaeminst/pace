// spec_test.go is the vtable rule, checked without an engine.
//
// Spec.Validate is exported and declared here, so the panic table needs no
// Limiter at all — the same reason Config.Resolve's tests need no Pool. That
// limiter.New actually calls Validate is a separate, one-line property, and it
// is pinned in limiter/zero_test.go where the call is.

package config_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jaeminst/pace/config"
)

// good is a Spec Validate accepts, so each case below can be one field wrong
// rather than a fresh literal whose other fields might be doing the work.
func good() config.Spec {
	return config.Spec{
		Quota:        func(string) config.Quota { return config.Quota{Rate: config.PerMinute(60), Burst: 1} },
		Now:          time.Now,
		Logger:       slog.New(slog.DiscardHandler),
		Shards:       8,
		IdleExpiry:   time.Minute,
		GCInterval:   time.Minute,
		StoreTimeout: time.Second,
	}
}

// TestValidatePanicsOnASpecTheEngineCannotUse is the vtable rule: a value the
// engine cannot work with fails where it is written, naming the field, rather
// than as a nil call on the first request or on a background goroutine three
// calls later.
//
// A nil Store is deliberately absent from this table. It is the one meaningful
// zero here — no persistence is how pace runs unless a caller configures some —
// and TestValidateAcceptsNoStore below pins that.
func TestValidatePanicsOnASpecTheEngineCannotUse(t *testing.T) {
	tests := []struct {
		name string
		bend func(*config.Spec)
		want string
	}{
		{"no Quota", func(c *config.Spec) { c.Quota = nil }, "Spec.Quota, Spec.Now and Spec.Logger are required"},
		{"no Now", func(c *config.Spec) { c.Now = nil }, "Spec.Quota, Spec.Now and Spec.Logger are required"},
		{"no Logger", func(c *config.Spec) { c.Logger = nil }, "Spec.Quota, Spec.Now and Spec.Logger are required"},
		{"zero Shards", func(c *config.Spec) { c.Shards = 0 }, "Spec.Shards must be a positive power of two"},
		{"Shards not a power of two", func(c *config.Spec) { c.Shards = 6 }, "Spec.Shards must be a positive power of two"},
		{"zero IdleExpiry", func(c *config.Spec) { c.IdleExpiry = 0 }, "Spec.IdleExpiry, Spec.GCInterval and Spec.StoreTimeout must be positive"},
		{"zero GCInterval", func(c *config.Spec) { c.GCInterval = 0 }, "Spec.IdleExpiry, Spec.GCInterval and Spec.StoreTimeout must be positive"},
		{"zero StoreTimeout", func(c *config.Spec) { c.StoreTimeout = 0 }, "Spec.IdleExpiry, Spec.GCInterval and Spec.StoreTimeout must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				got, ok := recover().(string)
				switch {
				case !ok:
					t.Errorf("panicked with %v, want a string naming the field", got)
				case !strings.HasPrefix(got, "config: "):
					t.Errorf("panic = %q, want it prefixed with the package name", got)
				case !strings.Contains(got, tt.want):
					t.Errorf("panic = %q, want it to mention %q", got, tt.want)
				}
			}()
			spec := good()
			tt.bend(&spec)
			spec.Validate()
			t.Error("Validate did not panic")
		})
	}
}

// TestValidateAcceptsNoStore: StoreTimeout is still required without one,
// because it bounds the load that happens when a user is first seen whether or
// not anything is written back.
func TestValidateAcceptsNoStore(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Validate panicked on a Spec with no Store: %v", r)
		}
	}()
	spec := good()
	spec.Store = nil
	spec.Validate()
}

// TestConfigSpecPassesValidate is the property that makes the table above worth
// having: the one Spec a caller actually gets, from a resolved Config, is a Spec
// the engine accepts. Without it the table could be pinning invariants that
// Config.Spec quietly violates.
func TestConfigSpecPassesValidate(t *testing.T) {
	cfg, err := config.Config{
		BaseURL: "http://example.invalid",
		Rate:    config.PerMinute(60),
	}.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a Spec from a resolved Config failed Validate: %v", r)
		}
	}()
	cfg.Spec().Validate()
}
