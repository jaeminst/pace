package limiter

import (
	"math"
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
// reached but no example test names. Limit is a float64, so every one of
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
			if got := Finite(tt.in); got != tt.want {
				t.Errorf("Finite(%v) = %v, want %v", float64(tt.in), float64(got), float64(tt.want))
			}
		})
	}
}
