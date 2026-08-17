package response

import (
	"net/http"
	"testing"
	"time"
)

// TestAClocklessResponseStillAnswers covers the zero value. A Response built
// without a clock — which is what a caller constructing one for a test gets —
// must still answer RetryAfter rather than panic.
func TestAClocklessResponseStillAnswers(t *testing.T) {
	r := New(0, "", nil, http.Header{"Retry-After": []string{"30"}}, nil)
	if got, ok := r.RetryAfter(); !ok || got != 30*time.Second {
		t.Errorf("RetryAfter on a clockless Response = (%v, %v), want (30s, true)", got, ok)
	}
	if r.clock().IsZero() {
		t.Error("clock() on a clockless Response returned the zero time")
	}
}
