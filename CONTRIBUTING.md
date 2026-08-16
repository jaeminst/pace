# Contributing to pace

## Prerequisites

- Go 1.26.6 or later
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
benchstat docs/bench/baseline-v0.3.0.txt new.txt
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

`time.Sleep` is not a synchronisation primitive. A test that waits 20ms for a
goroutine to reach a particular line is slower than it needs to be on a good day
and wrong under load — which is when CI runs.

Exactly one `time.Sleep` remains in the suite, and it is the poll interval
inside `waitFor` (`retry_test.go`). That one is legitimate: the test still fails
if the condition never holds, and never passes because the timing happened to
work out. `grep -rn 'time.Sleep' *_test.go` should return that line and nothing
else; a second hit is a review comment.

What to reach for instead, in rough order of preference:

1. **A channel the code under test already closes.** A handler that blocks can
   also signal that it was entered; `sync.OnceFunc` makes that a one-liner.
2. **A test hook** (`hooks.go`, installed through `export_test.go`). These exist
   for the cases where the interesting moment is inside the library: a goroutine
   about to block for a token, a sweep finishing, `Shutdown` closing the door.
3. **The injectable clock.** `Config.Clock` makes idle expiry, lease expiry, and
   token refill deterministic. Note that Windows' wall clock is coarse enough
   that two `time.Now()` calls can return the same value, so a test that expires
   something by setting a one-nanosecond timeout will pass locally and fail in
   CI. Drive a fake clock instead.
4. **Freeze the clock for token accounting.** Comparing token counts before and
   after an operation is only exact if the bucket cannot refill in between.

## Fuzzing

Six targets, run by `make fuzz` (or `make fuzz FUZZTIME=5m` when you want to
lean on it) and briefly in CI on every push. Their seed corpora also run as
ordinary tests, so a known-bad input can never regress silently.

They earn their place. Every fix in the v0.3.0 notes credited to fuzzing came
out of under a minute of it, including a path that could retarget a request at
a host the caller never named. When you add a function that parses anything a
server or a caller controls — a header, a URL, stored state — add a target for
it, and assert the property rather than the output.

`testing/synctest` is not usable for most of this suite: it requires every
goroutine in the bubble to be durably blocked, and a real `httptest` server
doing network I/O never is.

Proving a negative — "nothing else happened" — looks like the one place a sleep
is unavoidable, and it is not. Sleeping establishes only that time passed; what
you want to establish is that the background worker *looked* and found nothing.
`quietPolls` (`retry_test.go`) does that by waiting for N queue polls to
complete via the `afterPoll` hook, so the assertion fails on a slow machine
rather than passing because the retry it was watching for had not fired yet.
Reach for the same shape when you need to prove a negative about the sweep or
any other background loop.
