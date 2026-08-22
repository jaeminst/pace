// config_test.go tests this package's own job: turning what a caller wrote
// into something the engine can be handed. Validation, defaulting, the shard
// rounding and the per-user quota fallback all live here, so their tests do.
//
// Every one of them goes through [Config.Resolve] or [Config.QuotaFor] rather than
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
		{"missing BaseURL", Config{QuotaFor: Fixed(bucket.Quota{Rate: bucket.PerMinute(60)})}, "BaseURL"},
		{"unparseable BaseURL", Config{BaseURL: "://x", QuotaFor: Fixed(bucket.Quota{Rate: 1})}, "BaseURL"},
		{"missing QuotaFor", Config{BaseURL: "http://x"}, "QuotaFor"},
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
		{&Error{Field: "QuotaFor", Err: cause}, "pace: invalid Config.QuotaFor: required"},
		{&Error{Field: "Shards", Value: -3}, "pace: invalid Config.Shards: -3"},
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
		BaseURL:  "http://example.invalid",
		QuotaFor: Fixed(bucket.Quota{Rate: bucket.PerMinute(60)}),
		Shards:   1 << 21,
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
			_, err := (Config{BaseURL: tt.baseURL, QuotaFor: Fixed(bucket.Quota{Rate: bucket.PerMinute(60)})}).Resolve()
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
		_, err := (Config{BaseURL: base, QuotaFor: Fixed(bucket.Quota{Rate: bucket.PerMinute(60)})}).Resolve()
		var ce *Error
		if !errors.As(err, &ce) || ce.Field != "BaseURL" {
			t.Errorf("Resolve(%q) = %v, want an *Error on BaseURL", base, err)
		}
	}
}

// TestQuotaForIsUsedAsWritten: there is no defaulting left in this package. A
// Quota that comes back from QuotaFor is the answer, including its zero fields.
//
// This is the rule that replaced "each field falls back on its own". That rule
// existed because Config carried a Rate and a Burst beside QuotaFor and the two
// had to be reconciled; with one source there is nothing to reconcile, and a
// caller who wants a default writes it in their own function.
func TestQuotaForIsUsedAsWritten(t *testing.T) {
	free := bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2}
	tiers := map[string]bucket.Quota{
		"fast": {Rate: bucket.PerMinute(600), Burst: 20},
	}
	cfg := Config{
		BaseURL: "http://example.invalid",
		QuotaFor: func(userID string) bucket.Quota {
			if q, ok := tiers[userID]; ok {
				return q
			}
			return free
		},
	}.withDefaults()

	for _, tt := range []struct {
		user string
		want bucket.Quota
	}{
		{"fast", bucket.Quota{Rate: bucket.PerMinute(600), Burst: 20}},
		{"never-mentioned", free},
	} {
		if got := cfg.QuotaFor(tt.user); got != tt.want {
			t.Errorf("QuotaFor(%q) = %+v, want %+v", tt.user, got, tt.want)
		}
	}
}

// TestFixedAnswersTheSameForEveryone pins the convenience: Fixed is the flat
// case of the one hook, not a second place a rate can be configured.
func TestFixedAnswersTheSameForEveryone(t *testing.T) {
	q := bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}
	f := Fixed(q)
	for _, user := range []string{"", "alice", "bob"} {
		if got := f(user); got != q {
			t.Errorf("Fixed(%+v)(%q) = %+v, want %+v", q, user, got, q)
		}
	}
}
