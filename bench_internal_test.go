// bench_internal_test.go holds white-box benchmarks that isolate pace's own
// machinery — shard lookup, bucket accounting, GC sweep — from the cost of an
// HTTP round-trip. The black-box benchmarks in bench_test.go measure the full
// end-to-end path and are dominated by loopback TCP.
package pace

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/jaeminst/pace/internal/bucket"
	"github.com/jaeminst/pace/internal/store"
)

// benchLimiter builds a Limiter directly, bypassing New so no GC goroutine or
// store is started. dbPath enables SQLite persistence when non-empty.
// benchCtx stands in for a caller's context in white-box benchmarks.
var benchCtx = context.Background()

func benchLimiter(b *testing.B, dbPath string) *Limiter {
	b.Helper()
	e := &Limiter{cfg: Config{
		BaseURL:      "http://example.invalid",
		Rate:         PerMinute(1_000_000),
		Burst:        1_000_000,
		IdleExpiry:   time.Hour,
		StoreTimeout: 5 * time.Second,
		Clock:        stdClock{},
		Logger:       slog.New(slog.DiscardHandler),
	}}
	e.shards = newShards(numShards)
	e.shardMask = uint32(len(e.shards) - 1)
	if dbPath != "" {
		s, err := store.OpenStore(dbPath)
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = s.Close() })
		e.store = sqliteStateStore{s: s}
	}
	return e
}

// BenchmarkShardIndex measures the per-call cost of mapping a user ID to its
// shard. This runs on every request and every Tokens() call.
//
// The fnv.New32a()/[]byte(userID) pair does not allocate on Go 1.25 — the
// compiler devirtualises the hash.Hash32 result and keeps the byte slice on
// the stack — so what is left to win here is interface-dispatch overhead, not
// GC pressure. Track ns/op, not allocs/op.
func BenchmarkShardIndex(b *testing.B) {
	e := benchLimiter(b, "")
	for _, n := range []int{8, 32, 128} {
		id := fmt.Sprintf("%0*d", n, 1)
		b.Run(fmt.Sprintf("len=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = e.shardFor(id)
			}
		})
	}
}

// BenchmarkUserFor_Hot measures the steady-state lookup of an existing user:
// shard hash + read-locked map hit.
func BenchmarkUserFor_Hot(b *testing.B) {
	e := benchLimiter(b, "")
	const id = "user-hot"
	_ = e.userFor(benchCtx, id)
	b.ReportAllocs()
	for b.Loop() {
		_ = e.userFor(benchCtx, id)
	}
}

// BenchmarkUserFor_Cold measures first-ever lookup for a user: read-lock miss,
// then bucket creation under the shard write lock.
func BenchmarkUserFor_Cold(b *testing.B) {
	e := benchLimiter(b, "")
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = e.userFor(benchCtx, fmt.Sprintf("cold-user-%d", i))
		i++
	}
}

// BenchmarkBucket_TokensAt measures the token-count read used by Tokens(),
// the OnThrottle check, and every sweep snapshot.
func BenchmarkBucket_TokensAt(b *testing.B) {
	bk := bucket.NewBucket(1_000_000, 1_000)
	b.ReportAllocs()
	for b.Loop() {
		_ = bk.Tokens()
	}
}

// BenchmarkSweep measures a full GC sweep over expired users, with and
// without persistence.
//
// Today sweep holds each shard's write lock across the store.Save call, so the
// reported ns/op IS the aggregate lock-hold time — the number that blocks live
// requests. Once sweep is restructured to persist outside the locks, ns/op and
// lock-hold time diverge and the latter must be measured separately.
//
// The user count is deliberately modest: every user costs one serialised
// SQLite transaction in the store variant, so a larger population makes the
// benchmark take minutes rather than making the point any better.
func BenchmarkSweep(b *testing.B) {
	const users = 2_000
	for _, withStore := range []bool{false, true} {
		name := "store=none"
		if withStore {
			name = "store=sqlite"
		}
		b.Run(name, func(b *testing.B) {
			dbPath := ""
			if withStore {
				dbPath = filepath.Join(b.TempDir(), "bench.db")
			}
			e := benchLimiter(b, dbPath)
			e.cfg.IdleExpiry = 0 // every user is immediately expired
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				for i := range users {
					_ = e.userFor(benchCtx, fmt.Sprintf("sweep-user-%d", i))
				}
				b.StartTimer()
				e.sweep()
			}
		})
	}
}
