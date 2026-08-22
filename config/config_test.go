// config_test.go tests this package's own job: turning what a caller wrote
// into something the engine can be handed. Validation, defaulting, the shard
// rounding and the per-user quota fallback all live here, so their tests do.
//
// Every one of them goes through [Config.Resolve] or [Config.Quota] rather than
// through client.New. That is the point of exporting those two: checking a
// configuration no longer requires building an engine, and this package's tests
// no longer import the one that would make them a cycle.
//
// What each test asserts is which written values are refused, which are
// defaulted, and how Rate, Burst and QuotaFor fold into a single answer. None
// of it is engine behaviour.

package config

import (
	"errors"
	"testing"

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

func TestErrorFromResolve(t *testing.T) {
	tests := []struct {
		name      string
		cfg       Config
		wantField string
	}{
		{"missing BaseURL", Config{Rate: PerMinute(60)}, "BaseURL"},
		{"zero Rate", Config{BaseURL: "http://x"}, "Rate"},
		{"negative Rate", Config{BaseURL: "http://x", Rate: -1}, "Rate"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := tt.cfg.Resolve()
			if err == nil {
				t.Fatal("Resolve = nil error, want an *Error")
			}
			var ce *Error
			if !errors.As(err, &ce) {
				t.Fatalf("Resolve = %v, want *Error", err)
			}
			if ce.Field != tt.wantField {
				t.Errorf("Error.Field = %q, want %q", ce.Field, tt.wantField)
			}
		})
	}
}

// TestLimitErrorNotErrClosed pins the distinction that a caller acts on. The
// limiter reports "would exceed context deadline" without waiting, leaving the
// caller's ctx.Err() nil; inferring "the client must have closed" from that
// told callers the Client was shut down when it was very much open.
func TestErrorMessage(t *testing.T) {
	cause := errors.New("required")
	tests := []struct {
		err  *Error
		want string
	}{
		{&Error{Field: "BaseURL", Err: cause}, "pace: invalid Config.BaseURL: required"},
		{&Error{Field: "Rate", Value: Limit(0), Err: cause}, "pace: invalid Config.Rate (0): required"},
		{&Error{Field: "Burst", Value: -3}, "pace: invalid Config.Burst: -3"},
		{&Error{Field: "Shards"}, "pace: invalid Config.Shards"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
	if !errors.Is(&Error{Field: "X", Err: cause}, cause) {
		t.Error("ConfigError does not unwrap to its cause")
	}
}

// TestLimitErrorCarriesDelay: the field callers branch on has to be populated.
// It was documented as "how long the caller would have had to wait" and left at
// zero, which a godoc example exposed by printing it.
func TestConfigShardsUpperBound(t *testing.T) {
	_, err := (Config{
		BaseURL: "http://example.invalid",
		Rate:    PerMinute(60),
		Shards:  1 << 21,
	}).Resolve()
	var ce *Error
	if !errors.As(err, &ce) || ce.Field != "Shards" {
		t.Fatalf("Resolve with an absurd Shards = %v, want ConfigError on Shards", err)
	}
}

func TestBaseURLIsValidated(t *testing.T) {
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
			_, err := (Config{BaseURL: tt.baseURL, Rate: PerMinute(60)}).Resolve()
			if tt.wantErr {
				var ce *Error
				if !errors.As(err, &ce) || ce.Field != "BaseURL" {
					t.Fatalf("Resolve(%q) = %v, want an *Error on BaseURL", tt.baseURL, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resolve(%q) = %v, want nil", tt.baseURL, err)
			}
		})
	}
}

// TestBaseURLWithoutAHostnameIsRejected: "http://:" and "http://:8080" both
// have a non-empty url.URL.Host and no hostname at all, so the original check
// let them through and produced a Limiter whose every request went nowhere.
func TestBaseURLWithoutAHostnameIsRejected(t *testing.T) {
	for _, base := range []string{"http://:", "http://:8080"} {
		_, err := (Config{BaseURL: base, Rate: PerMinute(60)}).Resolve()
		var ce *Error
		if !errors.As(err, &ce) || ce.Field != "BaseURL" {
			t.Errorf("Resolve(%q) = %v, want an *Error on BaseURL", base, err)
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
	tiers := map[string]Quota{
		"fast":  {Rate: PerMinute(600)}, // Burst unset
		"deep":  {Burst: 50},            // Rate unset
		"zeros": {},
	}
	cfg := Config{
		BaseURL:  "http://example.invalid",
		Rate:     PerMinute(60),
		Burst:    2,
		QuotaFor: func(userID string) Quota { return tiers[userID] },
	}.withDefaults()

	for _, tt := range []struct {
		user string
		want Quota
	}{
		{"fast", Quota{Rate: PerMinute(600), Burst: 2}},
		{"deep", Quota{Rate: PerMinute(60), Burst: 50}},
		{"zeros", Quota{Rate: PerMinute(60), Burst: 2}},
		{"never-mentioned", Quota{Rate: PerMinute(60), Burst: 2}},
	} {
		if got := cfg.Quota(tt.user); got != tt.want {
			t.Errorf("Quota(%q) = %+v, want %+v", tt.user, got, tt.want)
		}
	}
}
