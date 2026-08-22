// parse_test.go covers the spellings a person actually writes, the ones they
// mistype, and the property that ties the two halves of the vocabulary
// together: what Limit.String prints, ParseLimit reads.

package bucket_test

import (
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/jaeminst/pace/bucket"
)

// TestParseLimitSpellings: every accepted way of saying the same three rates.
// Grouped by the rate they produce, because that is the actual claim — these
// are spellings, not variants.
func TestParseLimitSpellings(t *testing.T) {
	for _, tt := range []struct {
		want  bucket.Limit
		forms []string
	}{
		{bucket.PerMinute(6), []string{"6/m", "6/min", "6/mins", "6/minute", "6/minutes", "6rpm", "6RPM", "6Rpm", " 6 / min "}},
		{bucket.PerSecond(1), []string{"1/s", "1/sec", "1/secs", "1/second", "1/seconds", "1rps", "1RPS", "60/min", "3600/hour"}},
		{bucket.PerHour(100), []string{"100/h", "100/hr", "100/hrs", "100/hour", "100/hours", "100rph", "100RPH"}},
		{bucket.PerSecond(2.5), []string{"2.5/s", "2.5rps", "150/min"}},
		{bucket.Inf, []string{"inf", "Inf", "INF", "infinite", "unlimited"}},
	} {
		for _, form := range tt.forms {
			got, err := bucket.ParseLimit(form)
			if err != nil {
				t.Errorf("ParseLimit(%q) = %v", form, err)
				continue
			}
			if math.Abs(float64(got-tt.want)) > 1e-12 {
				t.Errorf("ParseLimit(%q) = %v, want %v", form, got, tt.want)
			}
		}
	}
}

// TestParseLimitRefusals: what has to fail, and why each one is here.
//
// The zero and negative cases matter most. A rate at or below zero builds a
// bucket that never refills, which is a key throttled to a standstill for the
// life of the process — a failure that looks like a hang, not like a typo.
func TestParseLimitRefusals(t *testing.T) {
	for _, tt := range []struct{ in, why string }{
		{"", "empty"},
		{"   ", "blank"},
		{"6", "no period"},
		{"/min", "no number"},
		{"abc/s", "not a number"},
		{"6/mm", "not a period"},
		{"6/ms", "milliseconds are not a rate anyone writes"},
		{"6/day", "unsupported period"},
		{"0/s", "a bucket built from it never refills"},
		{"-1/s", "same, and negative"},
		{"0rpm", "the rpm spelling of the same"},
		{"nan/s", "NaN passes every comparison a guard makes"},
		{"6rp", "a truncated suffix"},
		{"6r/s", "rps is one token, not r plus /s"},
		{"6//s", "empty number, empty period"},
	} {
		got, err := bucket.ParseLimit(tt.in)
		if err == nil {
			t.Errorf("ParseLimit(%q) = %v, want an error: %s", tt.in, got, tt.why)
			continue
		}
		if !errors.Is(err, bucket.ErrBadLimit) {
			t.Errorf("ParseLimit(%q) error does not wrap ErrBadLimit: %v", tt.in, err)
		}
		if !strings.Contains(err.Error(), strings.TrimSpace(tt.in)) && tt.in != "" && strings.TrimSpace(tt.in) != "" {
			t.Errorf("ParseLimit(%q) = %q, want the input quoted back", tt.in, err)
		}
	}
}

// TestParseLimitAcceptsWhatStringPrints is the round-trip. String picks the
// largest unit that keeps the number at or above one, so it emits all three
// periods and "Inf"; if it ever emits something this cannot read, the two
// halves of the vocabulary have drifted.
func TestParseLimitAcceptsWhatStringPrints(t *testing.T) {
	for _, l := range []bucket.Limit{
		bucket.PerSecond(1), bucket.PerSecond(2.5), bucket.PerSecond(1000),
		bucket.PerMinute(1), bucket.PerMinute(6), bucket.PerMinute(59),
		bucket.PerHour(1), bucket.PerHour(59), bucket.PerHour(100),
		bucket.Inf,
	} {
		s := l.String()
		got, err := bucket.ParseLimit(s)
		if err != nil {
			t.Errorf("%v prints as %q, which ParseLimit refuses: %v", float64(l), s, err)
			continue
		}
		if got != l {
			t.Errorf("%q round-tripped to %v, want %v", s, float64(got), float64(l))
		}
	}
}

// TestNewLimitPanicsOnATypo: NewLimit is for a literal in the source, so a bad
// one is a programming error and fails where it is written — and a good one is
// just ParseLimit with the error already checked.
func TestNewLimitPanicsOnATypo(t *testing.T) {
	if got, want := bucket.NewLimit("6/m"), bucket.PerMinute(6); got != want {
		t.Errorf("NewLimit(\"6/m\") = %v, want %v", got, want)
	}
	defer func() {
		got, ok := recover().(string)
		if !ok || !strings.Contains(got, "6/mm") {
			t.Errorf("panicked with %v, want a message quoting the input", got)
		}
	}()
	bucket.NewLimit("6/mm")
	t.Error("NewLimit did not panic")
}

// TestNewQuotaPairsRateAndBurst, including the burst floor that the rest of the
// library applies everywhere else.
func TestNewQuotaPairsRateAndBurst(t *testing.T) {
	if got, want := bucket.NewQuota("6/m", 10), (bucket.Quota{Rate: bucket.PerMinute(6), Burst: 10}); got != want {
		t.Errorf("NewQuota = %+v, want %+v", got, want)
	}
	for _, burst := range []int{0, -1} {
		if got := bucket.NewQuota("6/m", burst).Burst; got != 1 {
			t.Errorf("NewQuota(_, %d).Burst = %d, want 1", burst, got)
		}
	}
	if _, err := bucket.ParseQuota("6/mm", 1); !errors.Is(err, bucket.ErrBadLimit) {
		t.Errorf("ParseQuota with a bad rate = %v, want ErrBadLimit", err)
	}

	t.Run("a bad rate panics", func(t *testing.T) {
		defer func() {
			if got, ok := recover().(string); !ok || !strings.Contains(got, "6/mm") {
				t.Errorf("panicked with %v, want a message quoting the input", got)
			}
		}()
		bucket.NewQuota("6/mm", 1)
		t.Error("NewQuota did not panic")
	})
}

// TestParseLimitInfinities: the two ways a number can be infinite, and the
// different answers they get.
//
// A written-out infinity is someone saying "no limit" with a period attached,
// and lands on bucket.Inf — MaxFloat64, not a true infinity, which is the value
// that makes a bucket hold NaN tokens. An overflow is a typo, and is refused.
func TestParseLimitInfinities(t *testing.T) {
	for _, s := range []string{"inf/s", "Inf/min", "+inf/hour", "infrps"} {
		got, err := bucket.ParseLimit(s)
		if err != nil {
			t.Errorf("ParseLimit(%q) = %v, want Inf", s, err)
			continue
		}
		if got != bucket.Inf {
			t.Errorf("ParseLimit(%q) = %v, want Inf", s, float64(got))
		}
	}
	for _, s := range []string{"1e400/s", "1e400rpm"} {
		if got, err := bucket.ParseLimit(s); !errors.Is(err, bucket.ErrBadLimit) {
			t.Errorf("ParseLimit(%q) = %v, %v; want it refused as too large", s, float64(got), err)
		}
	}
}

// FuzzParseLimit: nothing it accepts may be a rate the bucket cannot use. A
// NaN or a non-positive rate reaching a bucket is the defect this whole guard
// exists for, and the parser is now a second door into it.
func FuzzParseLimit(f *testing.F) {
	for _, s := range []string{"6/m", "1rps", "100/hour", "inf", "2.5/s", "", "0/s", "nan/s", "1e309/s"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		l, err := bucket.ParseLimit(s)
		if err != nil {
			if l != 0 {
				t.Errorf("ParseLimit(%q) failed but returned %v, want the zero Limit", s, float64(l))
			}
			return
		}
		if math.IsNaN(float64(l)) {
			t.Fatalf("ParseLimit(%q) accepted a NaN rate", s)
		}
		if l <= 0 {
			t.Fatalf("ParseLimit(%q) = %v, which is a bucket that never refills", s, float64(l))
		}
		if math.IsInf(float64(l), 0) {
			t.Fatalf("ParseLimit(%q) returned a true infinity; Inf is MaxFloat64", s)
		}
		// Whatever came back has to be usable, which is what the bucket needs
		// and what String has to be able to print.
		if _, err := bucket.ParseLimit(l.String()); err != nil {
			t.Fatalf("ParseLimit(%q) = %v, which prints as %q and does not parse back: %v", s, float64(l), l, err)
		}
	})
}
