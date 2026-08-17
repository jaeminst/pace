package limiter

import (
	"testing"
)

// TestResponseHelpersHandleTheirZeroCases: both are one-line helpers whose only
// interesting branch is the nil one, which is exactly the branch a caller hits
// when a request failed.
func TestResponseHelpersHandleTheirZeroCases(t *testing.T) {
	if got := httpStatusOf(nil); got != 0 {
		t.Errorf("httpStatusOf(nil) = %d, want 0", got)
	}
	if got := statusOf(nil); got != 0 {
		t.Errorf("statusOf(nil) = %d, want 0", got)
	}
}
