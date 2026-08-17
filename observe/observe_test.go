package observe

import "testing"

func TestEvictReasonAndJobPhaseStrings(t *testing.T) {
	reasons := map[EvictReason]string{
		EvictIdle: "idle", EvictExplicit: "explicit",
		EvictShutdown: "shutdown", EvictReason(9): "unknown",
	}
	for r, want := range reasons {
		if got := r.String(); got != want {
			t.Errorf("EvictReason(%d) = %q, want %q", r, got, want)
		}
	}
	phases := map[JobPhase]string{
		JobClaimed: "claimed", JobCompleted: "completed",
		JobRetrying: "retrying", JobDead: "dead", JobPhase(9): "unknown",
	}
	for p, want := range phases {
		if got := p.String(); got != want {
			t.Errorf("JobPhase(%d) = %q, want %q", p, got, want)
		}
	}
}
