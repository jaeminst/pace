package observe

import "testing"

func TestEvictReasonStrings(t *testing.T) {
	reasons := map[EvictReason]string{
		EvictIdle: "idle", EvictExplicit: "explicit",
		EvictShutdown: "shutdown", EvictReason(9): "unknown",
	}
	for r, want := range reasons {
		if got := r.String(); got != want {
			t.Errorf("EvictReason(%d) = %q, want %q", r, got, want)
		}
	}
}
