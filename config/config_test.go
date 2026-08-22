// config_test.go tests this package's own job: turning what a caller wrote
// into something the engine can be handed. Validation, defaulting, the shard
// rounding and the per-key quota fallback all live here, so their tests do.
//
// Every one of them goes through [Config.Resolve] rather than
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
	"math"
	"testing"

	"github.com/jaeminst/pace/bucket"
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
		{"missing BaseURL", Config{Quota: bucket.Quota{Rate: bucket.PerMinute(60)}}, "BaseURL"},
		{"unparseable BaseURL", Config{BaseURL: "://x", Quota: bucket.Quota{Rate: 1}}, "BaseURL"},
		{"zero Quota", Config{BaseURL: "http://x"}, "Quota"},
		{"NaN rate", Config{BaseURL: "http://x", Quota: bucket.Quota{Rate: bucket.Limit(math.NaN())}}, "Quota"},
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

// TestErrorMessage: the message names the field, because that is the whole
// point of the type — a caller who mistypes one field should not have to guess
// which. The Value is included when there is one and omitted when there is not,
// so a required-but-absent field does not read as "invalid Config.Shards (0)".
func TestErrorMessage(t *testing.T) {
	cause := errors.New("required")
	tests := []struct {
		err  *Error
		want string
	}{
		{&Error{Field: "BaseURL", Err: cause}, "pace: invalid Config.BaseURL: required"},
		{&Error{Field: "Quota", Err: cause}, "pace: invalid Config.Quota: required"},
		{&Error{Field: "Shards", Value: -3}, "pace: invalid Config.Shards: -3"},
		{&Error{Field: "Shards"}, "pace: invalid Config.Shards"},
	}
	for _, tt := range tests {
		if got := tt.err.Error(); got != tt.want {
			t.Errorf("Error() = %q, want %q", got, tt.want)
		}
	}
	if !errors.Is(&Error{Field: "X", Err: cause}, cause) {
		t.Error("Error does not unwrap to its cause")
	}
}

// TestConfigShardsUpperBound: Shards is rounded up to a power of two, so an
// absurd value is refused rather than rounded into an allocation nobody meant.
// The ceiling is also what keeps roundUpPowerOfTwo from overflowing and what
// makes the shard count small enough to mask with a uint32.
func TestConfigShardsUpperBound(t *testing.T) {
	_, err := (Config{
		BaseURL: "http://example.invalid",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60)},
		Shards:  1 << 21,
	}).Resolve()
	var ce *Error
	if !errors.As(err, &ce) || ce.Field != "Shards" {
		t.Fatalf("Resolve with an absurd Shards = %v, want an *Error on Shards", err)
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
			_, err := (Config{BaseURL: tt.baseURL, Quota: bucket.Quota{Rate: bucket.PerMinute(60)}}).Resolve()
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
		_, err := (Config{BaseURL: base, Quota: bucket.Quota{Rate: bucket.PerMinute(60)}}).Resolve()
		var ce *Error
		if !errors.As(err, &ce) || ce.Field != "BaseURL" {
			t.Errorf("Resolve(%q) = %v, want an *Error on BaseURL", base, err)
		}
	}
}

// TestResolveNormalisesTheQuota: the quota is a value, so Resolve can put it
// right before anything runs — which is the whole argument for it being one.
// An option's answer arrives too late for this and is normalised by the engine.
func TestResolveNormalisesTheQuota(t *testing.T) {
	for _, tt := range []struct {
		name string
		give bucket.Quota
		want bucket.Quota
	}{
		{"a non-positive burst is raised to one",
			bucket.Quota{Rate: bucket.PerMinute(60)},
			bucket.Quota{Rate: bucket.PerMinute(60), Burst: 1}},
		{"an infinite rate is made finite",
			bucket.Quota{Rate: bucket.Limit(math.Inf(1)), Burst: 3},
			bucket.Quota{Rate: bucket.Inf, Burst: 3}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := Config{BaseURL: "http://example.invalid", Quota: tt.give}.Resolve()
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Quota != tt.want {
				t.Errorf("Quota = %+v, want %+v", cfg.Quota, tt.want)
			}
		})
	}
}

// TestApplyFoldsOptions: Apply is the only thing that builds an Options, and a
// nil in the list is not a caller error worth panicking over.
func TestApplyFoldsOptions(t *testing.T) {
	if got := Apply(nil); got.QuotaFor != nil {
		t.Error("Apply(nil) produced a non-zero Options")
	}
	if got := Apply([]Option{nil}); got.QuotaFor != nil {
		t.Error("a nil Option was not skipped")
	}
	q := bucket.Quota{Rate: bucket.PerMinute(600), Burst: 5}
	got := Apply([]Option{WithQuotaFor(func(string, bucket.Quota) bucket.Quota { return q })})
	if got.QuotaFor == nil {
		t.Fatal("WithQuotaFor did not set QuotaFor")
	}
	if v := got.QuotaFor("alice", bucket.Quota{}); v != q {
		t.Errorf("QuotaFor = %+v, want %+v", v, q)
	}
}

// TestDefaultConfigIsUsableAsIs: the point of it is that a caller needs to
// invent nothing, so what it returns has to pass the same Resolve everything
// else does — and stay a plain Config, so a caller can still change a field.
func TestDefaultConfigIsUsableAsIs(t *testing.T) {
	cfg := DefaultConfig("http://example.invalid")
	if got, want := cfg.Quota, (bucket.Quota{Rate: bucket.PerMinute(100), Burst: 10}); got != want {
		t.Errorf("Quota = %+v, want %+v", got, want)
	}
	resolved, err := cfg.Resolve()
	if err != nil {
		t.Fatalf("DefaultConfig does not survive Resolve: %v", err)
	}
	if resolved.Shards == 0 || resolved.Clock == nil {
		t.Error("Resolve did not fill the optional fields in")
	}
	// Unresolved on the way out, so overriding a field is still just an
	// assignment rather than a rebuild.
	if cfg.Shards != 0 {
		t.Error("DefaultConfig returned a resolved Config; it should compose")
	}
}
