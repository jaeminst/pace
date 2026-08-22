// bench_test.go holds white-box benchmarks that isolate the key registry —
// shard lookup, key creation, GC sweep — from the cost of an HTTP round-trip.
// The black-box benchmarks in the parent measure the full end-to-end path and
// are dominated by loopback TCP.
package registry

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jaeminst/pace/bucket"
)

// benchCtx stands in for a caller's context in white-box benchmarks.
var benchCtx = context.Background()

// benchConfig is testConfig with the two values a benchmark needs different: a
// quota nothing can exhaust, and an expiry no run reaches.
func benchConfig() Spec {
	cfg := testConfig()
	cfg.IdleExpiry = time.Hour
	cfg.QuotaFor = func(string) bucket.Quota { return bucket.Quota{Rate: 1_000_000, Burst: 1_000_000} }
	return cfg
}

// benchRegistry builds a registry with no background work.
func benchRegistry(b *testing.B) *Registry {
	b.Helper()
	return New(benchConfig())
}

// BenchmarkShardIndex measures the per-call cost of mapping a key to its
// shard. This runs on every request and every token read.
//
// The fnv.New32a()/[]byte(key) pair does not allocate on Go 1.25 — the
// compiler devirtualises the hash.Hash32 result and keeps the byte slice on
// the stack — so what is left to win here is interface-dispatch overhead, not
// GC pressure. Track ns/op, not allocs/op.
func BenchmarkShardIndex(b *testing.B) {
	r := benchRegistry(b)
	for _, n := range []int{8, 32, 128} {
		id := fmt.Sprintf("%0*d", n, 1)
		b.Run(fmt.Sprintf("len=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				_ = r.shardFor(id)
			}
		})
	}
}

// BenchmarkEntryFor_Hot measures the steady-state lookup of an existing key:
// shard hash + read-locked map hit.
func BenchmarkEntryFor_Hot(b *testing.B) {
	r := benchRegistry(b)
	const id = "user-hot"
	_ = r.GetOrCreate(benchCtx, id)
	b.ReportAllocs()
	for b.Loop() {
		_ = r.GetOrCreate(benchCtx, id)
	}
}

// BenchmarkEntryFor_Cold measures first-ever lookup for a key: read-lock miss,
// then bucket creation under the shard write lock.
func BenchmarkEntryFor_Cold(b *testing.B) {
	r := benchRegistry(b)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = r.GetOrCreate(benchCtx, fmt.Sprintf("cold-user-%d", i))
		i++
	}
}

// BenchmarkSweep measures a full GC sweep over expired keys.
//
// There is deliberately no persisting variant here. The registry does not own
// persistence — the owner supplies Flush — so a store-backed benchmark in this
// package would be measuring an adapter that exists only in this file. The real
// path is measured end to end by BenchmarkSweepWithStore in the parent.
func BenchmarkSweep(b *testing.B) {
	const keys = 2_000
	r := benchRegistry(b)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		// Backdate rather than set IdleExpiry to zero. With a cutoff of exactly
		// now, a key created inside the same clock tick as the sweep compares
		// equal rather than less and is not collected — so on a coarse clock the
		// benchmark silently sweeps a varying fraction of the population.
		stale := time.Now().Add(-time.Hour)
		for i := range keys {
			r.GetOrCreate(benchCtx, fmt.Sprintf("sweep-user-%d", i)).Touch(stale)
		}
		b.StartTimer()
		r.Sweep()
	}
}
