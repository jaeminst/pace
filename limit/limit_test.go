package limit_test

import (
	"math"
	"testing"
	"time"

	"github.com/jaeminst/pace/limit"
)

const limitEpsilon = 1e-12

func TestLimitConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  limit.Limit
		want float64 // requests per second
	}{
		{"limit.PerSecond", limit.PerSecond(5), 5},
		{"limit.PerMinute", limit.PerMinute(60), 1},
		{"limit.PerMinute fractional", limit.PerMinute(30), 0.5},
		// 7/min does not divide 60s evenly. Routing the rate through a
		// time.Duration interval truncated it; dividing in float64 does not.
		{"limit.PerMinute indivisible", limit.PerMinute(7), 7.0 / 60.0},
		{"limit.PerHour", limit.PerHour(3600), 1},
		{"limit.Every second", limit.Every(time.Second), 1},
		{"limit.Every 100ms", limit.Every(100 * time.Millisecond), 10},
		{"limit.Every minute", limit.Every(time.Minute), 1.0 / 60.0},
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
		if got := limit.Every(d); got != limit.Inf {
			t.Errorf("limit.Every(%v) = %v, want limit.Inf", d, got)
		}
	}
}

func TestLimitString(t *testing.T) {
	tests := []struct {
		limit limit.Limit
		want  string
	}{
		{limit.Inf, "Inf"},
		{limit.Limit(0), "0"},
		{limit.Limit(-1), "0"},
		{limit.PerSecond(5), "5/s"},
		{limit.PerMinute(60), "1/s"},
		{limit.PerMinute(30), "30/min"},
		{limit.PerMinute(6), "6/min"},
		{limit.PerHour(30), "30/hour"},
	}
	for _, tt := range tests {
		if got := tt.limit.String(); got != tt.want {
			t.Errorf("limit.Limit(%v).String() = %q, want %q", float64(tt.limit), got, tt.want)
		}
	}
}
