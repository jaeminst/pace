package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/jaeminst/pace/internal/registry"
	"github.com/jaeminst/pace/limit"
	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/store"
)

func TestNew_ZeroConfig(t *testing.T) {
	_, err := pace.New(pace.Config{})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
	}
}

func TestNew_ZeroRate(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    limit.PerMinute(0),
	})
	if err == nil {
		t.Fatal("want error for zero Rate")
	}
}

func TestNew_EmptyBaseURL(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "",
		Rate:    limit.PerMinute(60),
	})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
	}
}

func TestNew_StoreOpenFailure(t *testing.T) {
	// Point DBPath at a directory that doesn't exist to make sqlite.OpenStore fail.
	_, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    limit.PerMinute(60),
		DBPath:  "/nonexistent/directory/pace.db",
	})
	if err == nil {
		t.Fatal("expected error when store cannot be opened")
	}
}

func TestNew_CustomStore_NoopLoad(t *testing.T) {
	// Config.Store with a no-op backend: userFor calls Load on it.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   5,
		Store:   &noopStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestNew_CustomStore_WithSavedState(t *testing.T) {
	// Config.Store returns saved state so the wrapper.Load conversion path runs.
	srv := newEchoServer(t)
	defer srv.Close()

	now := time.Now()
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(60),
		Burst:   3,
		Store: &loadStateStore{state: store.State{
			Tokens: 1.5, LastUsed: now,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// User is loaded from the custom store — should have tokens available.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

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
		if got := pace.RoundUpPowerOfTwo(tt.in); got != tt.want {
			t.Errorf("RoundUpPowerOfTwo(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
