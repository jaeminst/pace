# Contributing to pace

## Prerequisites

- Go 1.25 or later
- `golangci-lint` v2 (for linting and formatting) — install via [golangci-lint.run](https://golangci-lint.run/usage/install/)

Every command below is also available as a `make` target; run `make help` for
the list.

## Running tests

```sh
make test        # go test -race -shuffle=on -count=1 ./...
```

To view coverage:

```sh
make cover       # writes coverage.out and opens the HTML report
```

## Running the linter

```sh
make lint        # golangci-lint run ./...
make fmt         # golangci-lint fmt (use `make fmt-check` for a read-only diff)
```

## Running benchmarks

```sh
make bench
```

Benchmarks split into two layers. The `_E2E` benchmarks in `bench_test.go`
include a real loopback HTTP round-trip and are dominated by kernel time; the
white-box benchmarks in `bench_internal_test.go` isolate pace's own machinery
and are the numbers to track across changes. Compare against a baseline with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
go test -run=NONE -bench=. -benchmem -count=6 ./... > new.txt
benchstat docs/bench/baseline-v0.1.0.txt new.txt
```

## Pull request guidelines

1. Open an issue first for significant changes.
2. Keep commits focused — one logical change per commit.
3. All tests and lint checks must pass before review.
4. Add or update tests for any changed behaviour.
5. Follow existing code style (`gofmt`, no unnecessary comments).

## Code style

- Format with `gofmt`. CI enforces this via `golangci-lint fmt --diff` — note
  that `go vet` does **not** check formatting.
- Comments: only when the **why** is non-obvious. No godoc for unexported helpers unless the logic is subtle.
- No feature flags or backwards-compatibility shims — change the code directly.

## Tests must not sleep

`time.Sleep` is not an acceptable synchronisation primitive in this repository.
Use `testing/synctest`, the injectable `Config.Clock`, or an explicit test hook
instead. Sleeps make the suite slow on a good day and flaky under `-race` on a
loaded CI runner.
