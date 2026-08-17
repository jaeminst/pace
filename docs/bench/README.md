# Benchmark baselines

`baseline-v0.7.0.txt` is the current reference. Compare against it with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
make benchstat
```

`baseline-v0.1.0.txt` through `baseline-v0.6.0.txt` are kept for the historical
comparison below. Neither is a useful regression check any more, and v0.5.0
moved the white-box benchmarks into the packages they measure, so several names
in that history no longer exist:

| was | is |
|---|---|
| `Sweep/store=none` | `internal/registry.BenchmarkSweep` |
| `Sweep/store=sqlite` | `limiter.BenchmarkSweepWithStore` (black box) |
| `ShardIndex`, `UserFor_*` | `internal/registry` |
| `Bucket_TokensAt` | `internal/bucket` |

Every figure in the table below is the **median** of the six runs in the
committed baseline. Quoting a single run, or a number remembered from an
earlier release, is how the prose and the artifact drift apart — which they had.

Numbers are machine-specific — regenerate your own baseline before trusting a
comparison made on different hardware. Both files here were taken on an Intel
i5-10600KF, Windows. Neither records the Go version, which is a gap — the
format only carries goos, goarch, pkg and cpu.

## Reading the two layers

The white-box benchmarks live beside the code they measure —
`internal/registry/bench_test.go`, `internal/bucket/bench_test.go`,
`internal/store/bench_test.go` — and isolate pace's own machinery. These are the
numbers to track across changes.

They deliberately do not reach for a store. `internal/registry` does not own
persistence; the owner supplies a `Flush`, so a store-backed benchmark there
would measure an adapter written in the test file rather than the one callers
get. `BenchmarkSweepWithStore` in `limiter/` covers that path end to end,
through a real Limiter and its own batching flush.

`limiter/bench_test.go` (black box) contains the `_E2E` benchmarks, which include a real
loopback HTTP round-trip. They are dominated by kernel and TCP time, so a change
to pace's internals is largely invisible there. `BenchmarkRequest_NoHTTP` is the
same full request path with the network stubbed out, and is the honest
end-to-end number.

## v0.1.0 → v0.3.0

Geomean −37.6% on time, −9.4% on bytes allocated.

| Benchmark | v0.1.0 | v0.3.0 | |
|---|---|---|---|
| `Sweep/store=sqlite` | 4.65 **s** | 9.56 ms | −99.8% |
| `Request_NoHTTP` | 2.29 µs | 1.94 µs | −15.2% |
| `Bucket_TokensAt` | 34.2 ns | 30.3 ns | −11.4% |
| `Sweep/store=none` | 70.0 µs | 83.3 µs | +19.0% |
| `Caller_Request_*_E2E` | ~66 µs | ~73 µs | +11% |

`Sweep/store=sqlite` is the headline, and the reason the v0.2.0 work happened.
Sweeping 2,000 expired users cost roughly 4.6 **seconds**, all of it with shard
write locks held across `store.Save`, so every request hashing to those shards
blocked for the duration. Restructuring `sweep` into snapshot → persist → delete,
with no I/O under any lock, is what closed it.

**The two regressions are both explained, and both are paid for.**

`Sweep/store=none` — now `internal/registry.BenchmarkSweep` — costs ~13µs more
per 2,000 users because each eviction now
decrements a per-shard atomic counter. That counter is what makes
`Limiter.Stats().Users` a sum of 256 atomic loads rather than 256 lock
acquisitions, which is the trade worth making for a number scraped on an
interval. Both versions allocate nothing on this path — an earlier v0.3.0
revision allocated 57KB per sweep building an eviction list for an observer that
was usually not configured, which is now built only when one is.

The `_E2E` numbers are TCP-dominated, and the same release added a merged
request context so that `Close` can abort a round-trip in flight. That is one
extra `context.AfterFunc` per request against ~70µs of kernel time.
`Request_NoHTTP`, which measures the same path without the network, went the
other way by 16%.

`ShardIndex/len=8` (+22%) is 0.9ns on a 5ns operation, and `UserFor_Hot` (+6.6%)
is 1.5ns on 22ns. Both are at the level where the Go version used to compile
them matters as much as the code does; neither is worth chasing.

`BenchmarkShardIndex` reports 0 allocs/op, not the 2 allocs/op that
`fnv.New32a()` plus `[]byte(userID)` would suggest. The compiler devirtualises
the `hash.Hash32` result and keeps the byte slice on the stack, so there is no GC
pressure to remove here — only interface-dispatch overhead. Optimise against
ns/op, not allocs/op.

## A trap in these benchmarks

Any sweep benchmark must **backdate** its users rather than set `IdleExpiry` to
zero. The cutoff is `now - IdleExpiry` and the test is `lastUsed < cutoff`, so
with a zero expiry a user created inside the same clock tick as the sweep
compares equal rather than less and is not collected. On Windows' coarse clock
that silently swept a varying fraction of the population — which is not a
correctness bug, but it makes the number unrepeatable and makes two benchmarks
that look identical measure different amounts of work.

Related: `b.StopTimer()` around the setup loop does not keep that loop's
allocations out of `B/op` as reliably as it keeps its time out of `sec/op`. Two
sweep benchmarks whose setup differs — one loading each user from SQLite, one
not — report allocation figures that differ by more than the measured region
does. Compare `sec/op` first, and treat a large `B/op` gap between benchmarks
with different setup as a question rather than a finding.
