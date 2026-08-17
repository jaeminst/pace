package runner

import (
	"context"
	"net/http"
	"testing"
)

// TestQueueReadsSeeCommittedJobs covers the same for the queue tables, which
// are read on the request path by Get and by the poller.
func TestQueueReadsSeeCommittedJobs(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()

	enqueue(t, s, "job-1", http.MethodGet)
	if jobs, err := s.Due(ctx, 1<<62, 10); err != nil || len(jobs) != 1 {
		t.Fatalf("Due after Enqueue = (%d jobs, %v), want 1", len(jobs), err)
	}
	if ok, err := s.Claim(ctx, "job-1", "w", 1, 1<<62); err != nil || !ok {
		t.Fatalf("Claim = (%v, %v)", ok, err)
	}
	// The claim is committed, so the job is no longer due.
	if jobs, err := s.Due(ctx, 1, 10); err != nil || len(jobs) != 0 {
		t.Fatalf("Due after Claim = (%d jobs, %v), want 0", len(jobs), err)
	}
}
