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
white-box benchmarks sit beside the code they measure (`registry`,
`bucket`) and are the numbers to track across
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

One package per concern. The repository root declares nothing — `doc.go` alone,
so `import "github.com/jaeminst/pace"` still resolves to a documentation page.
Everything else is a package named for what it is:

- `config/` — everything a caller configures: `Config`, its validation and
  defaults, and the rate vocabulary (`Limit`, `Quota`, `PerMinute`, `Inf`) they
  write it in. One configuration type, taken directly by the two `New`s below.
- `limiter/` — the rate limiter and only that. No `net/http`, no `urlx`.
- `client/` — creating and managing clients, and the request path. A `Pool` owns
  a limiter and mints a `Client` per user.
- `store/`, `shared/`, `observe/`, `transport/` — one contract each, public and
  documented on their own pages. None of them imports another package of pace's
  to declare a field.
- `store/memory/`, `store/storetest/`, `shared/quotatest/` — a reference
  implementation and the contracts as executable test suites. pace ships no
  backend; these are how you check one you wrote.
- `bucket/`, `registry/`, `gate/`, `breaker/`, `urlx/` — the pieces
  the Limiter is built from. There is no `internal/`: these are public because
  they are worth reading, not because a caller is expected to assemble one.

Some rules follow from that shape:

- **The dependency graph is a tree, and it is checked.** Nothing under a
  contract package may import `limiter/`. Two cuts were only possible in a
  particular order — `observe/` needed `rate/` first because `ThrottleInfo`
  holds a `Limit`, and `shared/` needed it because `TakeRequest` holds a
  `Quota`. If a new field would point back up the tree, that is the design
  telling you the type is on the wrong side.
- **A contract package holds no `*Limiter` methods**, because it cannot. Every
  implementation in this library is a method reading `l.cfg`, so a cut moves
  declarations and never behaviour. Do not try to move a method by inventing a
  callback for it; that inverts the one-callback rule those packages keep.
- **A vtable validates before it is used.** `registry.Spec` and `gate.Spec` are
  vtables, not option structs, and they are public, so a value they cannot work
  with must fail where it is written rather than on a background goroutine three
  calls later. Each `New` panics naming the field, and each has a test that
  proves it. A new field goes in the check.

- **`config.Config` is not a vtable, and `limiter.New` takes it anyway.** It
  validates the six fields it reads and ignores the four that describe HTTP —
  `limiter/httpfree_test.go` is what holds that line, since the type no longer
  does. `Config.Store` is the one field with a meaningful zero: a nil store is
  how pace runs unless persistence is configured, so nothing rejects it.

- **Do not add a struct whose construction is `X: src.X` for every field.** That
  is the mistake v0.12.0 made twice with `config.Spec` before deleting it. When a
  type restates another, the call site pays for it and the reader pays twice.

- **A caller-supplied func is called from request goroutines.** `Config.QuotaFor`
  is, and the docs did not say so until v0.13.0 — so both the README and the
  package example taught an unguarded map, which is a data race the test suite
  could not see. If you add another callback, say where it runs, and add a test
  that actually calls it from two goroutines. An `Example` cannot be that test:
  `// Output:` forces it to one goroutine and `-race` reports nothing about a
  program with one.

- **Read a pair as a pair.** A rate and its burst, a token count and its
  timestamp: reading the two halves separately can hand back a combination that
  was never configured, and `-race` will not tell you, because each read is
  properly synchronised on its own. `bucket.Quota` and `registry`'s `Snapshot`
  are the shape; `atomic.Pointer` to an immutable value is the mechanism.
- **`new_test.go` is not optional.** It pins the one signature that crosses
  between the three packages — `func(config.Config) (*client.Pool, error)` — and
  the end-to-end assertions that `client.New` actually wired what it was given.
  It was `facade_test.go` and most of it was alias declarations; those went with
  the re-exports in v0.11.0, and there is nothing to re-export from the root now.

One thing inside `limiter/` has been looked at and deliberately left whole:

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
`SetAfterSweepHook` does that for the GC: waiting for a sweep to complete
establishes that the collector *looked*, so the assertion fails on a slow
machine rather than passing because the eviction it was watching for had not
happened yet. Reach for the same shape for any other background loop.

## GitHub Actions are pinned to commit SHAs

Every `uses:` in `.github/workflows/` names a 40-character SHA with the tag as a
trailing comment. A floating tag is mutable by whoever owns the action, and two
of ours are high-privilege third parties: `action-gh-release` runs with
`contents: write`, `codecov-action` is handed `CODECOV_TOKEN`.

Do not replace a SHA with a tag to "make it readable" — the comment is what
makes it readable. Dependabot moves them, and it is configured to send majors
as their own PR so they get read.
