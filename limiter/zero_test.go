// zero_test.go pins the one thing about the vtable that is this package's
// rather than config's: that New consults it at all.
//
// The Spec's own invariants moved to config with the type — config/spec_test.go
// has the eight-case table, and it needs no engine to run it. What is left here
// is the wiring, and it is worth a test of its own precisely because it is one
// line inside New that nothing else would notice the loss of.

package limiter_test

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

// good is a Spec New accepts.
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

// TestNewValidatesItsSpec: a bad Spec has to fail at New, where it was written,
// rather than as a nil call on the first request or on a background goroutine
// three calls later. Delete the Validate call in New and this is the only test
// that fails.
func TestNewValidatesItsSpec(t *testing.T) {
	defer func() {
		got, ok := recover().(string)
		switch {
		case !ok:
			t.Errorf("panicked with %v, want the string config.Spec.Validate raises", got)
		case !strings.Contains(got, "Spec.Quota"):
			t.Errorf("panic = %q, want it to name the missing field", got)
		}
	}()
	spec := good()
	spec.Quota = nil
	limiter.New(spec)
	t.Error("New accepted a Spec with no Quota")
}

// TestNewAcceptsNoStore: StoreTimeout is still required without one, because it
// bounds the load that happens when a user is first seen whether or not
// anything is written back. This is the whole-engine counterpart to
// config's TestValidateAcceptsNoStore — it builds a Limiter and closes it.
func TestNewAcceptsNoStore(t *testing.T) {
	lim := limiter.New(good())
	t.Cleanup(func() { _ = lim.Close() })
	if lim == nil {
		t.Fatal("New returned nil for a Spec with no Store")
	}
}
