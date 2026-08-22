package registry

import (
	"strings"
	"testing"
)

// TestTheZeroSpecPanicsWithItsReason covers what publishing this package
// changed. Spec is a vtable, not a set of options: before New validated it,
// New(Spec{}) built a zero-length shard slice and a mask of 0xFFFFFFFF, and
// the first lookup indexed out of range — a panic with nothing in it to say
// which field was missing.
func TestTheZeroSpecPanicsWithItsReason(t *testing.T) {
	for _, tt := range []struct {
		name string
		cfg  Spec
		want string
	}{
		{"zero", Spec{}, "Shards"},
		{"not a power of two", func() Spec { c := testConfig(); c.Shards = 3; return c }(), "power of two"},
		{"no clock", func() Spec { c := testConfig(); c.Now = nil; return c }(), "Now"},
		{"no store hooks", func() Spec { c := testConfig(); c.Load = nil; return c }(), "Load"},
		{"no seams", func() Spec { c := testConfig(); c.AfterSweep = nil; return c }(), "AfterSweep"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("New accepted a Spec it cannot work with")
				}
				if msg, _ := r.(string); !strings.Contains(msg, tt.want) {
					t.Errorf("panic said %q, want it to name %q", r, tt.want)
				}
			}()
			New(tt.cfg)
		})
	}
}
