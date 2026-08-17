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

Benchmarks split into two layers. The `_E2E` benchmarks in `limiter/bench_test.go`
include a real loopback HTTP round-trip and are dominated by kernel time; the
white-box benchmarks sit beside the code they measure (`internal/registry`,
`internal/bucket`, `internal/store`) and are the numbers to track across
changes. Compare against a baseline with
[`benchstat`](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat):

```sh
make benchstat
```

Read [`docs/bench/README.md`](../docs/bench/README.md) before writing a new
one — it records two traps that make a sweep benchmark measure the wrong thing.

## Pull request guidelines

1. Open an issue first for significant changes.
2. Keep commits focused — one logical change per commit.
3. All tests and lint checks must pass before review.
4. Add or update tests for any changed behaviour.
5. Follow existing code style (`gofmt`, no unnecessary comments).

## Where code lives

One package per concern. `pace` at the root is the front door and holds ten
re-exported names; everything else is a package named for what it is:

- `limiter/` — the Limiter and the request path. This is where the behaviour is.
- `limit/`, `store/`, `shared/`, `observe/`, `queue/`, `response/`, `transport/`
  — one contract each, public and documented on their own pages.
- `internal/` — machinery with no place in the API: `bucket` (token accounting),
  `store` (SQLite, imported as `sqlite` where both are in scope), `queue` (the
  background runner, imported as `runner`), `registry` (the user population),
  `breaker`, `urlx`.

Three rules follow from that shape:

- **The dependency graph is a tree, and it is checked.** Nothing under a
  contract package may import `limiter/`. Two cuts were only possible in a
  particular order — `observe/` needed `limit/` first because `ThrottleInfo`
  holds a `Limit`, and `queue/` needed `response/` because `RetryDecision` holds
  a `*Response`. If a new field would point back up the tree, that is the design
  telling you the type is on the wrong side.
- **A contract package holds no `*Limiter` methods**, because it cannot. Every
  implementation in this library is a method reading `l.cfg`, so a cut moves
  declarations and never behaviour. Do not try to move a method by inventing a
  callback for it; that inverts the one-callback rule the internal packages keep.
- **`facade_test.go` is not optional.** Adding a name to the root does nothing
  until it is re-exported, and nothing warns you. It also pins each re-export as
  an *alias* rather than a defined type — a distinction the compiler will not
  catch, since `go build ./...` passes either way while `errors.As` and every
  caller's struct literal quietly stop working.

Two things inside `limiter/` have been looked at and deliberately left whole:

- **The durable singleflight** (`future`, `await`, `joinOrLead`). It caches a
  `*response.Response` keyed by job ID within one process, which is request
  deduplication rather than queue state.
- **`ratelimit.go`.** Its references reach shared quota, stats, the observer and
  the shutdown barrier. Extracting it would mean a per-request callback for each.

## Code style

- Format with `gofmt`. CI enforces this via `golangci-lint fmt --diff` — note
  that `go vet` does **not** check formatting.
- Comments: only when the **why** is non-obvious. No godoc for unexported helpers unless the logic is subtle.
- No feature flags or backwards-compatibility shims — change the code directly.

## Tests must not sleep

`time.Sleep` is not a synchronisation primitive. A test that waits 20ms for a
goroutine to reach a particular line is slower than it needs to be on a good day
and wrong under load — which is when CI runs.

Two remain, and both are legitimate. Check with:

```sh
grep -rn 'time.Sleep' --include='*.go' .
```

- `waitFor`'s poll interval (`retry_test.go`). The test still fails if the
  condition never holds, and never passes because the timing happened to work
  out.
- `pacetest.go`'s wait on a backend's own reported `RetryAfter`. It runs in the
  suite via `TestGCRAQuotaPassesTheConformanceSuite`, and it escapes a
  `*_test.go` grep because `pacetest` is shipped code — which is why the command
  above searches every file, not just tests. An earlier version of this section
  claimed one sleep and was wrong for exactly that reason.

A third hit is a review comment.

**`<-time.After` is not automatically a sleep.** Used as a ceiling — "fail if
this has not happened in ten seconds" — it is the right tool and the suite uses
it in about twenty places. Used to *prove a negative* — "wait 200ms, then assert
nothing happened" — it is a sleep wearing a different spelling, and the grep
above will not find it. Two such waits remain (`response_test.go` and
`shutdown_test.go`, both asserting `Shutdown` has not returned early); replacing
them with a hook is a welcome change.

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

## GitHub Actions are pinned to commit SHAs

Every `uses:` in `.github/workflows/` names a 40-character SHA with the tag as a
trailing comment. A floating tag is mutable by whoever owns the action, and two
of ours are high-privilege third parties: `action-gh-release` runs with
`contents: write`, `codecov-action` is handed `CODECOV_TOKEN`.

Do not replace a SHA with a tag to "make it readable" — the comment is what
makes it readable. Dependabot moves them, and it is configured to send majors
as their own PR so they get read.
