package gate

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/shared"
)

// nopQuota is a backend that grants everything. New only checks that the field
// is non-nil, so nothing here needs to be a real decision.
type nopQuota struct{}

func (nopQuota) Take(context.Context, shared.TakeRequest) (shared.Grant, error) {
	return shared.Grant{OK: true}, nil
}

// good is a Config New accepts, so each case below can be one field wrong
// rather than a fresh literal whose other fields might be doing the work.
func good() Config {
	return Config{
		Quota:   nopQuota{},
		Timeout: time.Second,
		Logger:  slog.New(slog.DiscardHandler),
		Now:     time.Now,
		Closed:  errors.New("closed"),
		Throttled: func(context.Context, string, *bucket.Bucket, time.Duration, time.Time, *float64) {
		},
		BeforeWait:      func() {},
		BeforeQuotaTake: func() {},
	}
}

// TestNewPanicsOnConfigItCannotUse is the vtable rule, which this package was
// the only one of the four not to check. Every field here is required, nothing
// is defaulted, and a zero one is a nil call on the first request rather than a
// default — so it has to fail where it is written, naming the field.
//
// Note that none of these panics is reachable through pace.New: the front door
// validates and defaults every value before limiter.New builds a gate. They are
// the contract for a caller assembling the pieces directly, which is the only
// way to reach them and the reason they need a test of their own.
func TestNewPanicsOnConfigItCannotUse(t *testing.T) {
	tests := []struct {
		name string
		bend func(*Config)
		want string
	}{
		{"no Quota", func(c *Config) { c.Quota = nil }, "Quota is required"},
		{"no Logger", func(c *Config) { c.Logger = nil }, "Logger, Now and Closed are required"},
		{"no Now", func(c *Config) { c.Now = nil }, "Logger, Now and Closed are required"},
		{"no Closed", func(c *Config) { c.Closed = nil }, "Logger, Now and Closed are required"},
		{"no Throttled", func(c *Config) { c.Throttled = nil }, "Throttled, BeforeWait and BeforeQuotaTake are required"},
		{"no BeforeWait", func(c *Config) { c.BeforeWait = nil }, "Throttled, BeforeWait and BeforeQuotaTake are required"},
		{"no BeforeQuotaTake", func(c *Config) { c.BeforeQuotaTake = nil }, "Throttled, BeforeWait and BeforeQuotaTake are required"},
		{"zero Timeout", func(c *Config) { c.Timeout = 0 }, "Timeout must be positive"},
		{"negative Timeout", func(c *Config) { c.Timeout = -time.Second }, "Timeout must be positive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				got, ok := recover().(string)
				switch {
				case !ok:
					t.Errorf("panicked with %v, want a string naming the field", got)
				case !strings.HasPrefix(got, "gate: "):
					t.Errorf("panic = %q, want it prefixed with the package name", got)
				case !strings.Contains(got, tt.want):
					t.Errorf("panic = %q, want it to mention %q", got, tt.want)
				}
			}()
			cfg := good()
			tt.bend(&cfg)
			New(context.Background(), cfg)
			t.Error("New did not panic")
		})
	}
}

// TestNewAcceptsAWholeConfig pins the other half: the table above is only
// meaningful if the unmodified Config is one New takes.
func TestNewAcceptsAWholeConfig(t *testing.T) {
	if g := New(context.Background(), good()); g == nil {
		t.Fatal("New returned nil for a Config it accepts")
	}
}

// TestTheCountersStartAtZero. They are the three numbers Stats reports about
// the backend, and a Gate that has asked nothing must say so — a non-zero start
// would read as traffic that never happened.
func TestTheCountersStartAtZero(t *testing.T) {
	g := New(context.Background(), good())
	if takes, refused, errs := g.Takes(), g.Refused(), g.Errors(); takes|refused|errs != 0 {
		t.Errorf("a fresh Gate reports takes=%d refused=%d errors=%d, want all zero", takes, refused, errs)
	}
}
