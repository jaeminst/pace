package limiter

import (
	"net/http"
	"testing"
	"time"
)

// FuzzRetryAfter covers the one header pace interprets, parsed two different
// ways, from a server that may be hostile. Whatever it says, the answer has to
// be a duration a caller can act on: never negative, never absurd.
func FuzzRetryAfter(f *testing.F) {
	f.Add("30")
	f.Add("0")
	f.Add("-1")
	f.Add("")
	f.Add("Wed, 21 Oct 2015 07:28:00 GMT")
	f.Add("9223372036854775807")
	f.Add("not a number")

	f.Fuzz(func(t *testing.T, header string) {
		r := &Response{
			header: http.Header{"Retry-After": []string{header}},
			clock:  stdClock{},
		}
		got, ok := r.RetryAfter()
		if !ok {
			if got != 0 {
				t.Errorf("RetryAfter = (%v, false) for %q, want a zero duration when not ok", got, header)
			}
			return
		}
		if got < 0 {
			t.Errorf("RetryAfter = %v for %q, want a non-negative duration", got, header)
		}
	})
}

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

	// A Response built outside a Limiter — which is what a zero value is — must
	// still be able to answer RetryAfter rather than panic on a nil clock.
	r := &Response{header: http.Header{"Retry-After": []string{"30"}}}
	if got, ok := r.RetryAfter(); !ok || got != 30*time.Second {
		t.Errorf("RetryAfter on a clockless Response = (%v, %v), want (30s, true)", got, ok)
	}
	if r.now().IsZero() {
		t.Error("now() on a clockless Response returned the zero time")
	}
}
