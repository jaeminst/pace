// config_test.go tests the front door's own job: turning what a caller wrote
// into something the engine can be handed. Validation, defaulting and the
// shard rounding all live here now, so their tests do too.
package pace

import (
	"testing"

	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/registry"
)

func TestNew_ZeroConfig(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
	}
}

func TestNew_ZeroRate(t *testing.T) {
	_, err := New(Config{
		BaseURL: "http://x",
		Rate:    rate.PerMinute(0),
	})
	if err == nil {
		t.Fatal("want error for zero Rate")
	}
}

func TestNew_EmptyBaseURL(t *testing.T) {
	_, err := New(Config{
		BaseURL: "",
		Rate:    rate.PerMinute(60),
	})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
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
		if got := roundUpPowerOfTwo(tt.in); got != tt.want {
			t.Errorf("RoundUpPowerOfTwo(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
