# Benchmark baselines

`baseline-v0.1.0.txt` is the recorded starting point for the v0.2.0 work, taken
before any refactoring. Compare against it with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
make benchstat
```

Numbers are machine-specific — regenerate your own baseline before trusting a
comparison made on different hardware.

## Reading the two layers

`bench_internal_test.go` (white box) isolates pace's own machinery. These are
the numbers to track across changes.

`bench_test.go` (black box) contains the `_E2E` benchmarks, which include a real
loopback HTTP round-trip. They are dominated by kernel and TCP time, so a change
to pace's internals is largely invisible there. `BenchmarkRequest_NoHTTP` is the
same full request path with the network stubbed out, and is the honest
end-to-end number.

## What the v0.1.0 baseline shows

`BenchmarkSweep` is the headline. Sweeping 2,000 expired users costs roughly
72µs with no store configured and roughly 4.6 **seconds** with the SQLite store
— a ~64,000x difference. That gap is not merely slow: `sweep` holds each shard's
write lock across `store.Save`, so the entire duration is time during which
requests hashing to that shard are blocked. Restructuring `sweep` to persist
outside the locks is what closes it.

`BenchmarkShardIndex` reports 0 allocs/op, not the 2 allocs/op that
`fnv.New32a()` plus `[]byte(userID)` would suggest. On Go 1.25 the compiler
devirtualises the `hash.Hash32` result and keeps the byte slice on the stack, so
there is no GC pressure to remove here — only interface-dispatch overhead.
Optimise against ns/op, not allocs/op.
