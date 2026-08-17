package registry

import (
	"strings"
	"testing"
)

// TestTheZeroConfigPanicsWithItsReason covers what publishing this package
// changed. Config is a vtable, not a set of options: before New validated it,
// New(Config{}) built a zero-length shard slice and a mask of 0xFFFFFFFF, and
// the first lookup indexed out of range — a panic with nothing in it to say
// which field was missing.
func TestTheZeroConfigPanicsWithItsReason(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"zero", Config{}, "Shards"},
		{"not a power of two", func() Config { c := testConfig(); c.Shards = 3; return c }(), "power of two"},
		{"no clock", func() Config { c := testConfig(); c.Now = nil; return c }(), "Now"},
		{"no store hooks", func() Config { c := testConfig(); c.Load = nil; return c }(), "Load"},
		{"no seams", func() Config { c := testConfig(); c.AfterSweep = nil; return c }(), "AfterSweep"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("New accepted a Config it cannot work with")
				}
				if msg, _ := r.(string); !strings.Contains(msg, tt.want) {
					t.Errorf("panic said %q, want it to name %q", r, tt.want)
				}
			}()
			New(tt.cfg)
		})
	}
}
