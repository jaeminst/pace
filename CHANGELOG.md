# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Work toward v0.2.0, the single consolidated breaking release before v1.0.0.

### Added

- White-box benchmarks isolating pace's own machinery from HTTP round-trip cost
  (`bench_internal_test.go`), plus `BenchmarkRequest_NoHTTP` for the full request
  path with the network stubbed out. A recorded baseline lives in
  `docs/bench/baseline-v0.1.0.txt`.
- `Makefile` with `test`, `lint`, `fmt`, `cover`, `bench`, `benchstat`, `vuln`,
  and `ci` targets.
- CI now runs a three-OS (Linux, macOS, Windows) by two-Go-version matrix with
  `-shuffle=on`, and enforces formatting via `golangci-lint fmt --diff`.
- `govulncheck`, CodeQL, and Dependabot workflows.
- Linters: `errorlint`, `nilerr`, `bodyclose`, `contextcheck`, `copyloopvar`,
  `makezero`, `wastedassign`, `predeclared`, `nolintlint`, `perfsprint`, `godot`,
  `thelper`, `tparallel`, `usetesting`, `depguard`.
- `.gitattributes` pinning the working tree to LF so `gofmt` checks behave the
  same on Windows as in CI.

### Changed

- **Breaking:** `Config.RatePerMinute int` is now `Config.Rate Limit`. Build it
  with `pace.PerSecond`, `pace.PerMinute`, `pace.PerHour`, or `pace.Every`, or
  use `pace.Inf` to disable throttling. Migration is mechanical:
  `RatePerMinute: 60` becomes `Rate: pace.PerMinute(60)`.
- **Breaking:** `ErrNoPersistence` is renamed `ErrNoQueue`. It reports a missing
  durable *queue*, which is a distinct thing from the `StateStore`.
- **Breaking:** `New` now returns `*ConfigError` rather than opaque errors, so
  callers can tell which field was rejected via `errors.As`.
- `go.mod` now requires `go 1.25` rather than the patch-level `go 1.25.7`, which
  had forced every consumer onto a toolchain at least that new.
- `BenchmarkCaller_ConcurrentUsers_256` is now `BenchmarkConcurrentUsers_256`
  and drives a stub transport. Pointing 256 goroutines at an `httptest` server
  measured the host's TCP accept backlog rather than pace, and overflowed it on
  Windows. The remaining loopback benchmarks are suffixed `_E2E`.

### Fixed

- A request that could not get a token in time reported `ErrClosed` — "client
  closed" — on a Client that was open and healthy. The limiter answers "would
  exceed context deadline" without waiting, so the caller's `ctx.Err()` is still
  nil at that point, and that was being read as proof the engine had shut down.
  Throttling now returns a `*LimitError` carrying the user, limit, and burst,
  and `ErrClosed` is returned only when the engine's own context is done. Both
  bundled examples printed the wrong error before this change.
- Restoring a persisted bucket rounded the token count to a whole number, so
  fractional credit was silently lost or invented on every restart and every GC
  eviction. Saving 0.5 tokens restored as 0; saving 2.7 restored as 3. With
  `Burst: 1`, any partial token restored as 0 — the user lost their credit
  entirely. Restore is now exact.
- `RestoreBucket` read the wall clock directly instead of the injected
  `Config.Clock`, which made the whole persistence-restore path impossible to
  test deterministically. It now takes `now` from the caller, and `Tokens`,
  `saveAll`, `sweep`, `evict`, and the `OnThrottle` check all read through the
  configured clock.
- A per-minute rate that does not divide 60s evenly was truncated by routing it
  through a `time.Duration` interval. The conversion is now exact.
- `internal/store` compared errors against `sql.ErrNoRows` with `==` instead of
  `errors.Is`, which would miss a wrapped error.
- `CONTRIBUTING.md` claimed CI enforced formatting via `go vet`. It does not —
  `go vet` has never checked formatting, and one file was in fact unformatted.

## [0.1.0]

Initial release.
