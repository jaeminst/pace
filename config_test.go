// config_test.go tests the front door's own job: turning what a caller wrote
// into something the engine can be handed. Validation, defaulting and the
// shard rounding all live here now, so their tests do too.
package pace

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/registry"
)

// TestRoundUpPowerOfTwo pins the shard-count rounding. shardIndex masks rather
// than divides, so a count that is not a power of two would silently address
// only part of the map.
func TestRoundUpPowerOfTwo(t *testing.T) {
	const def = registry.DefaultShards
	tests := []struct{ in, want int }{
		{0, def},
		{-1, def},
		{1, 1},
		{2, 2},
		{3, 4},
		{5, 8},
		{256, 256},
		{257, 512},
	}
	for _, tt := range tests {
		if got := roundUpPowerOfTwo(tt.in); got != tt.want {
			t.Errorf("RoundUpPowerOfTwo(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

// The rest of this file is what moved out of limiter/ when Config did. Every
// test below asserts something about turning a written Config into an engine
// one: which values are refused, which are defaulted, and how Rate, Burst and
// QuotaFor fold into a single answer. None of it is engine behaviour, and most
// of it needs no Limiter at all.

func TestConfigErrorFromNew(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantField string
	}{
		{"missing BaseURL", Config{Rate: limiter.PerMinute(60)}, "BaseURL"},
		{"zero Rate", Config{BaseURL: "http://x"}, "Rate"},
		{"negative Rate", Config{BaseURL: "http://x", Rate: -1}, "Rate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(tt.cfg)
			if err == nil {
				t.Fatal("New = nil error, want ConfigError")
			}
			var ce *ConfigError
			if !errors.As(err, &ce) {
				t.Fatalf("New = %v, want *ConfigError", err)
			}
			if ce.Field != tt.wantField {
				t.Errorf("ConfigError.Field = %q, want %q", ce.Field, tt.wantField)
			}
		})
	}
}

// TestLimitErrorNotErrClosed pins the distinction that a caller acts on. The
// limiter reports "would exceed context deadline" without waiting, leaving the
// caller's ctx.Err() nil; inferring "the client must have closed" from that
// told callers the Client was shut down when it was very much open.
func TestConfigErrorMessage(t *testing.T) {
	cause := errors.New("required")
	tests := []struct {
		err  *ConfigError
		want string
	}{
		{&ConfigError{Field: "BaseURL", Err: cause}, "pace: invalid Config.BaseURL: required"},
		{&ConfigError{Field: "Rate", Value: limiter.Limit(0), Err: cause}, "pace: invalid Config.Rate (0): required"},
		{&ConfigError{Field: "Burst", Value: -3}, "pace: invalid Config.Burst: -3"},
		{&ConfigError{Field: "Shards"}, "pace: invalid Config.Shards"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
	if !errors.Is(&ConfigError{Field: "X", Err: cause}, cause) {
		t.Error("ConfigError does not unwrap to its cause")
	}
}

// TestLimitErrorCarriesDelay: the field callers branch on has to be populated.
// It was documented as "how long the caller would have had to wait" and left at
// zero, which a godoc example exposed by printing it.
func TestConfigShardsUpperBound(t *testing.T) {
	_, err := New(Config{
		BaseURL: "http://example.invalid",
		Rate:    limiter.PerMinute(60),
		Shards:  1 << 21,
	})
	var ce *ConfigError
	if !errors.As(err, &ce) || ce.Field != "Shards" {
		t.Fatalf("New with an absurd Shards = %v, want ConfigError on Shards", err)
	}
}

func TestBaseURLIsValidatedAtNew(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{"plain http", "http://api.example.com", false},
		{"https with path", "https://api.example.com/v1", false},
		{"with port", "http://127.0.0.1:8080", false},
		{"relative", "/api/v1", true},
		{"no scheme", "api.example.com", true},
		{"unsupported scheme", "ftp://files.example.com", true},
		{"no host", "http://", true},
		{"unparsable", "http://%zz", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := New(Config{BaseURL: tt.baseURL, Rate: limiter.PerMinute(60)})
			if tt.wantErr {
				var ce *ConfigError
				if !errors.As(err, &ce) || ce.Field != "BaseURL" {
					t.Fatalf("New(%q) = %v, want a ConfigError on BaseURL", tt.baseURL, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("New(%q) = %v, want nil", tt.baseURL, err)
			}
		})
	}
}

// TestBaseURLWithoutAHostnameIsRejected: "http://:" and "http://:8080" both
// have a non-empty url.URL.Host and no hostname at all, so the original check
// let them through and produced a Limiter whose every request went nowhere.
func TestBaseURLWithoutAHostnameIsRejected(t *testing.T) {
	for _, base := range []string{"http://:", "http://:8080"} {
		_, err := New(Config{BaseURL: base, Rate: limiter.PerMinute(60)})
		var ce *ConfigError
		if !errors.As(err, &ce) || ce.Field != "BaseURL" {
			t.Errorf("New(%q) = %v, want a ConfigError on BaseURL", base, err)
		}
	}
}

// TestQuotaPartialOverrideFallsBackPerField: each field of a Quota falls back
// on its own, so a QuotaFor backed by a map needs no "if missing" branch — the
// zero Quota it returns for an unlisted user selects both defaults, and a
// partial override changes only what it names.
//
// This calls quotaFor directly. Reached through a Limiter it needed a tiered
// fixture and three live buckets to observe three struct fields.
func TestQuotaPartialOverrideFallsBackPerField(t *testing.T) {
	tiers := map[string]limiter.Quota{
		"fast":  {Rate: limiter.PerMinute(600)}, // Burst unset
		"deep":  {Burst: 50},                    // Rate unset
		"zeros": {},
	}
	cfg := Config{
		BaseURL:  "http://example.invalid",
		Rate:     limiter.PerMinute(60),
		Burst:    2,
		QuotaFor: func(userID string) limiter.Quota { return tiers[userID] },
	}.withDefaults()

	for _, tt := range []struct {
		user string
		want limiter.Quota
	}{
		{"fast", limiter.Quota{Rate: limiter.PerMinute(600), Burst: 2}},
		{"deep", limiter.Quota{Rate: limiter.PerMinute(60), Burst: 50}},
		{"zeros", limiter.Quota{Rate: limiter.PerMinute(60), Burst: 2}},
		{"never-mentioned", limiter.Quota{Rate: limiter.PerMinute(60), Burst: 2}},
	} {
		if got := cfg.quotaFor(tt.user); got != tt.want {
			t.Errorf("quotaFor(%q) = %+v, want %+v", tt.user, got, tt.want)
		}
	}
}

func TestNonFiniteRateIsNotAcceptedSilently(t *testing.T) {
	t.Run("NaN is rejected", func(t *testing.T) {
		_, err := New(Config{BaseURL: "http://x", Rate: limiter.Limit(math.NaN())})
		var ce *ConfigError
		if !errors.As(err, &ce) || ce.Field != "Rate" {
			t.Errorf("New with a NaN Rate = %v, want a ConfigError on Rate", err)
		}
	})

	t.Run("infinity means Inf", func(t *testing.T) {
		lim, err := New(Config{
			BaseURL: "http://example.invalid",
			Rate:    limiter.Limit(math.Inf(1)),
			Burst:   1,
		})
		if err != nil {
			t.Fatalf("New with an infinite Rate = %v, want it treated as Inf", err)
		}
		defer lim.Close()

		alice := lim.Client("alice")
		for i := range 100 {
			if !alice.Allow(context.Background()) {
				t.Fatalf("request %d was refused at an infinite rate", i)
			}
		}
		if got := tokensAvailable(alice); math.IsNaN(got) {
			t.Error("Tokens() = NaN; the bucket was built with a non-finite rate")
		}
	})

	t.Run("QuotaFor cannot smuggle one in", func(t *testing.T) {
		lim, err := New(Config{
			BaseURL: "http://example.invalid",
			Rate:    limiter.PerMinute(60),
			Burst:   2,
			QuotaFor: func(string) limiter.Quota {
				return limiter.Quota{Rate: limiter.Limit(math.NaN()), Burst: 2}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer lim.Close()

		alice := lim.Client("alice")
		if !alice.Allow(context.Background()) {
			t.Error("a request was refused by a bucket built from a NaN quota")
		}
		if got := tokensAvailable(alice); math.IsNaN(got) {
			t.Error("Tokens() = NaN; QuotaFor is not validated at New and must be clamped")
		}
	})
}

// tokensAvailable is Client.Tokens with the comma-ok dropped, for the two
// assertions above that only care whether the number is a NaN.
func tokensAvailable(c *limiter.Client) float64 {
	n, _ := c.Tokens()
	return n
}
