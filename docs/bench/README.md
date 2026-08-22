# Benchmark baselines

`baseline-v0.2.0.txt` is the current reference. Compare against it with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
make benchstat
```

The `Makefile` derives the filename from `git describe --tags --abbrev=0`, so
the baseline is always the one recorded for the most recent tag and cutting a
release means committing `docs/bench/baseline-$(tag).txt` and editing nothing.
`make bench-baseline` records it; `make benchstat BASELINE_VERSION=v0.1.0`
compares against an older one.

`sweep-lock-fix.txt` is not a release baseline. It is the run that substantiates
the one historical number worth keeping, and is explained below.

Every figure quoted here is the **median** of the six runs in the committed
file. Quoting a single run, or a number remembered from an earlier release, is
how the prose and the artifact drift apart — which they had.

Numbers are machine-specific. Both files were taken on an Intel i5-10600KF
running Windows; `baseline-v0.2.0.txt` on Go 1.26.6. Regenerate your own before
trusting a comparison made on different hardware. The `go test` format records
goos, goarch, pkg and cpu but **not the Go version**, which is why this
paragraph does.

## Reading the two layers

The white-box benchmarks live beside the code they measure —
`registry/bench_test.go`, `bucket/bench_test.go` — and isolate pace's own
machinery. These are the numbers to track across changes.

They deliberately do not reach for a store. `registry` does not own
persistence; the owner supplies a `Flush`, so a store-backed benchmark there
would measure an adapter written in the test file rather than the one callers
get. `BenchmarkSweepWithStore` in `limiter/` covers that path end to end,
through a real Limiter and its own batching flush.

`limiter/bench_test.go` is black box and holds the `_E2E` benchmarks, which
include a real loopback HTTP round-trip. They are dominated by kernel and TCP
time, so a change to pace's internals is largely invisible there.
`BenchmarkRequest_NoHTTP` is the same full request path with the network
stubbed out, and is the honest end-to-end number.

## v0.1.0 → v0.2.0

The library was rewritten between these two tags, so several benchmarks were
renamed with the code they measure:

| v0.1.0 | now |
|---|---|
| `Sweep/store=none` | `registry.BenchmarkSweep` |
| `Sweep/store=sqlite` | `limiter.BenchmarkSweepWithStore` — **not comparable**, see below |
| `UserFor_Hot`, `UserFor_Cold` | `registry.BenchmarkEntryFor_*` |
| `ConcurrentUsers_256` | `limiter.BenchmarkConcurrentKeys_256` |
| `Caller_Request_NewUser_E2E` | `limiter.BenchmarkCaller_Request_NewKey_E2E` |
| `ShardIndex`, `Bucket_TokensAt` | `registry`, `bucket` |

| Benchmark | v0.1.0 | v0.2.0 | |
|---|---|---|---|
| `Bucket_TokensAt` | 34.2 ns | 30.8 ns | −10.2% |
| `ShardIndex/len=8` | 4.65 ns | 4.34 ns | −6.8% |
| `ShardIndex/len=128` | 103 ns | 100 ns | −2.6% |
| `Request_NoHTTP` | 2.29 µs | 2.30 µs | +0.4% |
| `EntryFor_Cold` | 651 ns | 673 ns | +3.3% |
| `EntryFor_Hot` | 21.8 ns | 22.9 ns | +5.1% |
| `ConcurrentKeys_256` | 1.49 µs | 1.60 µs | +7.0% |
| `Sweep` (no store) | 70.0 µs | 80.3 µs | +14.6% |
| `Caller_Request_*_E2E` | ~66–69 µs | ~118–123 µs | +80% |

**The `_E2E` figure is the machine, not the code, and the file proves it.**
`Request_NoHTTP` runs the same request path with the network stubbed out and
moved 0.4%. A change that doubled pace's per-request work would show there
first. What moved is the loopback round-trip, on a host whose Go version and
Windows build are both different from the ones that produced the v0.1.0 file.

That is the rule this document has learned twice: **when a set of numbers moves
one way at once — especially in packages that did not change — suspect the room
before the diff.** An earlier run came out 3–20% slower across every benchmark
with `allocs/op` identical to the byte, including in packages that had not had a
commit since the previous baseline; re-running one of them alone reproduced
+7.8% on unchanged code.

**`Sweep` costs ~10µs more per 2,000 keys** because each eviction now decrements
a per-shard atomic counter. That counter is what makes `Limiter.Stats().Keys` a
sum of 256 atomic loads rather than 256 lock acquisitions, which is the trade
worth making for a number scraped on an interval. Both versions allocate nothing
on this path.

`EntryFor_Hot` (+1.1 ns on 22 ns) and `ShardIndex` are at the level where the Go
version used to compile them matters as much as the code does; neither is worth
chasing.

`BenchmarkShardIndex` reports 0 allocs/op, not the 2 allocs/op that
`fnv.New32a()` plus `[]byte(key)` would suggest. The compiler devirtualises the
`hash.Hash32` result and keeps the byte slice on the stack, so there is no GC
pressure to remove here — only interface-dispatch overhead. Optimise against
ns/op, not allocs/op.

## The sweep under a lock, and why one extra file is kept

v0.1.0 swept 2,000 expired keys in **4.65 seconds**, all of it with shard write
locks held across `store.Save`, so every request hashing to those shards blocked
for the duration. Restructuring `sweep` into snapshot → persist → delete, with
no I/O under any lock, took it to **9.59 ms** — a 99.8% cut, and the single
largest thing this benchmark suite has ever caught.

Both of those numbers are SQLite-to-SQLite, which is what makes them a
measurement of pace's locking rather than of a database. `sweep-lock-fix.txt` is
the run that holds the second one, kept for exactly that reason.

**Today's `SweepWithStore` cannot continue the series.** The SQLite backend was
removed in v0.2.0 — pace ships contracts, not backends — so it runs against
`store/memory`. It still measures what pace is responsible for: the sweep, the
batching, the chunking and the `BatchStore` assertion, with no I/O held under a
lock. But the store's own write latency is gone from the number, and comparing
759 µs against 4.65 s would credit pace's locking with the removal of a
database. The two answer different questions.

## A trap in these benchmarks

Any sweep benchmark must **backdate** its keys rather than set `IdleExpiry` to
zero. The cutoff is `now - IdleExpiry` and the test is `lastUsed < cutoff`, so
with a zero expiry a key created inside the same clock tick as the sweep
compares equal rather than less and is not collected. On Windows' coarse clock
that silently swept a varying fraction of the population — which is not a
correctness bug, but it makes the number unrepeatable and makes two benchmarks
that look identical measure different amounts of work.

Related: `b.StopTimer()` around the setup loop does not keep that loop's
allocations out of `B/op` as reliably as it keeps its time out of `sec/op`. Two
sweep benchmarks whose setup differs — one loading each key from a store, one
not — report allocation figures that differ by more than the measured region
does. Compare `sec/op` first, and treat a large `B/op` gap between benchmarks
with different setup as a question rather than a finding.
