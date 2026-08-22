package bucket_test

import (
	"math"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"
)

const limitEpsilon = 1e-12

func TestLimitConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  bucket.Limit
		want float64 // requests per second
	}{
		{"bucket.PerSecond", bucket.PerSecond(5), 5},
		{"bucket.PerMinute", bucket.PerMinute(60), 1},
		{"bucket.PerMinute fractional", bucket.PerMinute(30), 0.5},
		// 7/min does not divide 60s evenly. Routing the rate through a
		// time.Duration interval truncated it; dividing in float64 does not.
		{"bucket.PerMinute indivisible", bucket.PerMinute(7), 7.0 / 60.0},
		{"bucket.PerHour", bucket.PerHour(3600), 1},
		{"bucket.Every second", bucket.Every(time.Second), 1},
		{"bucket.Every 100ms", bucket.Every(100 * time.Millisecond), 10},
		{"bucket.Every minute", bucket.Every(time.Minute), 1.0 / 60.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if math.Abs(float64(tt.got)-tt.want) > limitEpsilon {
				t.Errorf("= %v, want %v", float64(tt.got), tt.want)
			}
		})
	}
}

func TestLimitEveryNonPositiveIsInf(t *testing.T) {
	for _, d := range []time.Duration{0, -time.Second} {
		if got := bucket.Every(d); got != bucket.Inf {
			t.Errorf("bucket.Every(%v) = %v, want bucket.Inf", d, got)
		}
	}
}

func TestLimitString(t *testing.T) {
	tests := []struct {
		limit bucket.Limit
		want  string
	}{
		{bucket.Inf, "Inf"},
		{bucket.Limit(0), "0"},
		{bucket.Limit(-1), "0"},
		{bucket.PerSecond(5), "5/s"},
		{bucket.PerMinute(60), "1/s"},
		{bucket.PerMinute(30), "30/min"},
		{bucket.PerMinute(6), "6/min"},
		{bucket.PerHour(30), "30/hour"},
	}
	for _, tt := range tests {
		if got := tt.limit.String(); got != tt.want {
			t.Errorf("bucket.Limit(%v).String() = %q, want %q", float64(tt.limit), got, tt.want)
		}
	}
}
