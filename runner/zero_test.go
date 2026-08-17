package runner

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jaeminst/pace/sqlite"
)

// TestTheZeroConfigPanicsWithItsReason covers what publishing this package
// changed. Config is everything the owner must supply, with nothing defaulted;
// before New validated it, New(ctx, Config{}) returned a Queue whose Start
// dereferenced a nil *sqlite.Store on the replay goroutine — a panic in another
// goroutine, with nothing to say which field was missing.
func TestTheZeroConfigPanicsWithItsReason(t *testing.T) {
	full := func() Config {
		return Config{
			Store: &sqlite.Store{}, Owner: "o", Logger: slog.New(slog.DiscardHandler),
			Now: time.Now, StoreTimeout: time.Second, PollInterval: time.Second,
			Workers: 1, ResultTTL: time.Hour, MaxAttempts: 1,
			Backoff:    func(int) time.Duration { return 0 },
			RepeatSafe: func(string) bool { return true },
			Dispatch:   func(context.Context, sqlite.Job) {},
			OnDead:     func(sqlite.Job, string) {},
			OnRetry:    func(string, string, int, time.Duration, error) {},
			AfterPoll:  func() {},
		}
	}
	for _, tt := range []struct {
		name string
		cfg  Config
		want string
	}{
		{"zero", Config{}, "Store"},
		{"no clock", func() Config { c := full(); c.Now = nil; return c }(), "Now"},
		{"no dispatcher", func() Config { c := full(); c.Dispatch = nil; return c }(), "Dispatch"},
		{"no seam", func() Config { c := full(); c.AfterPoll = nil; return c }(), "AfterPoll"},
		{"no workers", func() Config { c := full(); c.Workers = 0; return c }(), "Workers"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				r := recover()
				if r == nil {
					t.Fatal("New accepted a Config it cannot work with")
				}
				if msg, _ := r.(string); !strings.Contains(msg, tt.want) {
					t.Errorf("panic said %q, want it to name %q", r, tt.want)
				}
			}()
			New(context.Background(), tt.cfg)
		})
	}
}
