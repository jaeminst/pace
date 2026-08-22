package config_test

import (
	"math"
	"testing"
	"time"

	"github.com/jaeminst/pace/config"
)

const limitEpsilon = 1e-12

func TestLimitConstructors(t *testing.T) {
	tests := []struct {
		name string
		got  config.Limit
		want float64 // requests per second
	}{
		{"config.PerSecond", config.PerSecond(5), 5},
		{"config.PerMinute", config.PerMinute(60), 1},
		{"config.PerMinute fractional", config.PerMinute(30), 0.5},
		// 7/min does not divide 60s evenly. Routing the rate through a
		// time.Duration interval truncated it; dividing in float64 does not.
		{"config.PerMinute indivisible", config.PerMinute(7), 7.0 / 60.0},
		{"config.PerHour", config.PerHour(3600), 1},
		{"config.Every second", config.Every(time.Second), 1},
		{"config.Every 100ms", config.Every(100 * time.Millisecond), 10},
		{"config.Every minute", config.Every(time.Minute), 1.0 / 60.0},
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
		if got := config.Every(d); got != config.Inf {
			t.Errorf("config.Every(%v) = %v, want config.Inf", d, got)
		}
	}
}

func TestLimitString(t *testing.T) {
	tests := []struct {
		limit config.Limit
		want  string
	}{
		{config.Inf, "Inf"},
		{config.Limit(0), "0"},
		{config.Limit(-1), "0"},
		{config.PerSecond(5), "5/s"},
		{config.PerMinute(60), "1/s"},
		{config.PerMinute(30), "30/min"},
		{config.PerMinute(6), "6/min"},
		{config.PerHour(30), "30/hour"},
	}
	for _, tt := range tests {
		if got := tt.limit.String(); got != tt.want {
			t.Errorf("config.Limit(%v).String() = %q, want %q", float64(tt.limit), got, tt.want)
		}
	}
}
