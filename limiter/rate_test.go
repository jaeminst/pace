package limiter_test

import (
	"math"
	"testing"
	"time"

	"github.com/jaeminst/pace/limiter"
)

const limitEpsilon = 1e-12

func TestLimitConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  limiter.Limit
		want float64 // requests per second
	}{
		{"limiter.PerSecond", limiter.PerSecond(5), 5},
		{"limiter.PerMinute", limiter.PerMinute(60), 1},
		{"limiter.PerMinute fractional", limiter.PerMinute(30), 0.5},
		// 7/min does not divide 60s evenly. Routing the rate through a
		// time.Duration interval truncated it; dividing in float64 does not.
		{"limiter.PerMinute indivisible", limiter.PerMinute(7), 7.0 / 60.0},
		{"limiter.PerHour", limiter.PerHour(3600), 1},
		{"limiter.Every second", limiter.Every(time.Second), 1},
		{"limiter.Every 100ms", limiter.Every(100 * time.Millisecond), 10},
		{"limiter.Every minute", limiter.Every(time.Minute), 1.0 / 60.0},
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
		if got := limiter.Every(d); got != limiter.Inf {
			t.Errorf("limiter.Every(%v) = %v, want limiter.Inf", d, got)
		}
	}
}

func TestLimitString(t *testing.T) {
	tests := []struct {
		limit limiter.Limit
		want  string
	}{
		{limiter.Inf, "Inf"},
		{limiter.Limit(0), "0"},
		{limiter.Limit(-1), "0"},
		{limiter.PerSecond(5), "5/s"},
		{limiter.PerMinute(60), "1/s"},
		{limiter.PerMinute(30), "30/min"},
		{limiter.PerMinute(6), "6/min"},
		{limiter.PerHour(30), "30/hour"},
	}
	for _, tt := range tests {
		if got := tt.limit.String(); got != tt.want {
			t.Errorf("limiter.Limit(%v).String() = %q, want %q", float64(tt.limit), got, tt.want)
		}
	}
}
