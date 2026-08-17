package bucket_test

import (
	"testing"
	"time"

	"github.com/jaeminst/pace/internal/bucket"
)

// BenchmarkBucket_TokensAt measures the token-count read behind Client.Tokens,
// the throttle report, and every sweep snapshot.
func BenchmarkBucket_TokensAt(b *testing.B) {
	bk := bucket.NewBucket(1_000_000, 1_000)
	now := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		_ = bk.TokensAt(now)
	}
}
