package rate_test

import (
	"math"
	"testing"
	"time"

	"github.com/jaeminst/pace/rate"
)

const limitEpsilon = 1e-12

func TestLimitConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  rate.Limit
		want float64 // requests per second
	}{
		{"rate.PerSecond", rate.PerSecond(5), 5},
		{"rate.PerMinute", rate.PerMinute(60), 1},
		{"rate.PerMinute fractional", rate.PerMinute(30), 0.5},
		// 7/min does not divide 60s evenly. Routing the rate through a
		// time.Duration interval truncated it; dividing in float64 does not.
		{"rate.PerMinute indivisible", rate.PerMinute(7), 7.0 / 60.0},
		{"rate.PerHour", rate.PerHour(3600), 1},
		{"rate.Every second", rate.Every(time.Second), 1},
		{"rate.Every 100ms", rate.Every(100 * time.Millisecond), 10},
		{"rate.Every minute", rate.Every(time.Minute), 1.0 / 60.0},
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
		if got := rate.Every(d); got != rate.Inf {
			t.Errorf("rate.Every(%v) = %v, want rate.Inf", d, got)
		}
	}
}

func TestLimitString(t *testing.T) {
	tests := []struct {
		limit rate.Limit
		want  string
	}{
		{rate.Inf, "Inf"},
		{rate.Limit(0), "0"},
		{rate.Limit(-1), "0"},
		{rate.PerSecond(5), "5/s"},
		{rate.PerMinute(60), "1/s"},
		{rate.PerMinute(30), "30/min"},
		{rate.PerMinute(6), "6/min"},
		{rate.PerHour(30), "30/hour"},
	}
	for _, tt := range tests {
		if got := tt.limit.String(); got != tt.want {
			t.Errorf("rate.Limit(%v).String() = %q, want %q", float64(tt.limit), got, tt.want)
		}
	}
}
