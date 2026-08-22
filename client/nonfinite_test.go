// nonfinite_test.go is the half of the old root config_test.go that needs a
// live engine: a rate that is NaN or infinite has to be clamped before a bucket
// is built from it, and the only way to see that is to build one and ask.
//
// config/ tests the clamping itself against Config.Resolve. This tests that the
// clamped value is what actually reaches the bucket — the two halves of one
// property, in the two packages that own them.

package client_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
)

func TestNonFiniteRateIsNotAcceptedSilently(t *testing.T) {
	t.Run("NaN in the configured quota is rejected", func(t *testing.T) {
		_, err := client.New(config.Config{
			BaseURL: "http://example.invalid",
			Quota:   bucket.Quota{Rate: bucket.Limit(math.NaN()), Burst: 1},
		})
		var ce *config.Error
		if !errors.As(err, &ce) || ce.Field != "Quota" {
			t.Errorf("client.New with a NaN rate = %v, want a config.Error on Quota", err)
		}
	})

	t.Run("infinity means Inf", func(t *testing.T) {
		pool, err := client.New(config.Config{
			BaseURL: "http://example.invalid",
			Quota:   bucket.Quota{Rate: bucket.Limit(math.Inf(1)), Burst: 1},
		})
		if err != nil {
			t.Fatalf("client.New with an infinite Rate = %v, want it treated as Inf", err)
		}
		defer pool.Close()

		alice := pool.Client("alice")
		for i := range 100 {
			if !alice.Allow(context.Background()) {
				t.Fatalf("request %d was refused at an infinite rate", i)
			}
		}
		if got := tokensAvailable(alice); math.IsNaN(got) {
			t.Error("Tokens() = NaN; the bucket was built with a non-finite rate")
		}
	})

	t.Run("WithQuotaFor cannot smuggle one in", func(t *testing.T) {
		pool, err := client.New(config.Config{
			BaseURL: "http://example.invalid",
			Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 2},
		}, config.WithQuotaFor(func(string, bucket.Quota) bucket.Quota {
			return bucket.Quota{Rate: bucket.Limit(math.NaN()), Burst: 2}
		}))
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()

		alice := pool.Client("alice")
		if !alice.Allow(context.Background()) {
			t.Error("a request was refused by a bucket built from a NaN quota")
		}
		if got := tokensAvailable(alice); math.IsNaN(got) {
			t.Error("Tokens() = NaN; an option's answer is not validated at New and must be clamped")
		}
	})
}

// tokensAvailable is Client.Tokens with the comma-ok dropped, for the two
// assertions above that only care whether the number is a NaN.
func tokensAvailable(c *client.Client) float64 {
	n, _ := c.Tokens()
	return n
}
