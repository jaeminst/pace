package limiter

import (
	"math"
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

// FuzzLimitString: String is what a rate looks like in an error message and in
// every log line, so it must not panic and must not produce nothing.
func FuzzLimitString(f *testing.F) {
	f.Add(1.0)
	f.Add(0.0)
	f.Add(-1.0)
	f.Add(1.0 / 3600.0)
	f.Add(float64(Inf))

	f.Fuzz(func(t *testing.T, v float64) {
		if got := Limit(v).String(); got == "" {
			t.Errorf("Limit(%v).String() is empty", v)
		}
	})
}

// TestFiniteRateMapsWhatTheBucketCannotHold covers the branches the fuzzer
// reached but no example test names. pace.Limit is a float64, so every one of
// these is something a caller can write.
func TestFiniteRateMapsWhatTheBucketCannotHold(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   Limit
		want Limit
	}{
		{"positive infinity becomes Inf", Limit(math.Inf(1)), Inf},
		{"negative infinity becomes Inf", Limit(math.Inf(-1)), Inf},
		{"a finite rate is untouched", PerMinute(60), PerMinute(60)},
		{"Inf is already Inf", Inf, Inf},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := finiteRate(tt.in); got != tt.want {
				t.Errorf("finiteRate(%v) = %v, want %v", float64(tt.in), float64(got), float64(tt.want))
			}
		})
	}
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
