# Changelog

All notable changes to this project are documented here.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0]

The last release that may break the API. v1.0.0 freezes it, so what is here is
what becomes impossible afterwards — everything merely additive was deferred.
See [MIGRATION.md](MIGRATION.md) for an old/new table.

### Changed

- **`Config`'s nine durable-queue fields move into `Config.Queue`.**
  `IdempotencyHeader`, `AmbiguousPolicy`, `OnDeadLetter`, `Retry`, `RetryOn`,
  `QueueWorkers`, `QueuePollInterval`, `JobLease`, and `ResultTTL` become
  `Queue.IdempotencyHeader`, `.AmbiguousPolicy`, `.OnDeadLetter`, `.Retry`,
  `.RetryOn`, `.Workers`, `.PollInterval`, `.JobLease`, `.ResultTTL`. Nine
  fields configured one optional subsystem while sharing a namespace with
  `Rate` and `Burst`; grouping them is impossible after v1, and not grouping
  means every future queue knob inflates the top-level struct forever. A caller
  who set none of them sees no change: the zero `QueueConfig` resolves to the
  same defaults.
- **`Client.Durable(id)` returns `*Request`, not `(*Request, error)`.** Building
  a Request is documented twice as free and infallible; `Durable` was the sole
  exception, charging every call site a four-line error block for two conditions
  that never change during a process's life. `ErrNoQueue` and `ErrInvalidID` now
  surface from the terminal method, where an error is already being checked.
- `Config.Store` and `Config.DBPath` are no longer mutually exclusive. They
  persist different things, and forbidding both meant a Redis- or
  Postgres-backed caller could never have a durable queue — silently, with no
  signal at `New`. `ErrNoQueue`'s message now says which field provides one.

### Added

- **Per-user quotas.** `Config.QuotaFor func(string) Quota` overrides `Rate` and
  `Burst` for individual users, which is the feature the package name implies
  and did not have: pace could isolate users from each other but not tell a free
  tier from a paying one. Each `Quota` field falls back independently, so the
  zero value means "the defaults" and a map is a complete implementation.
  `Limiter.ReloadQuotas()` re-applies it to live buckets while keeping accrued
  tokens, and `Client.Quota()` reports what a user's bucket is enforcing.
- **`Client.Reserve`.** Holds a token, reports how long until it may be used,
  and lets you hand it back — the ground between `Allow`, which refuses rather
  than waits, and `Wait`, which waits and cannot refund.
- **`Config.SharedQuota`**, optional cross-replica rate limiting. Delegates the
  decision to a backend every replica consults, keeping the local bucket as a
  shadow that can only refuse. pace ships no backend; it ships the contract, as
  `pacetest.QuotaSuite`. Read [ADR
  0004](docs/adr/0004-shared-quota-is-approximate.md) first — it argues that
  most callers should not use this.
- `pacetest`, a package of conformance suites for the interfaces pace asks
  callers to implement.

### Fixed

- `LimitError` and `ThrottleInfo` report the quota that user's bucket is
  enforcing, rather than `Config.Rate`. Their documentation always said "the
  configuration in force for that user"; until `QuotaFor` existed there was only
  one configuration, so reading the global happened to be right.
- `Client.Allow`'s documentation no longer claims it never blocks without
  qualification. It does not wait for quota, but it can do bounded I/O: a store
  load on a user's first request, and a `SharedQuota` call when one is
  configured.

## [0.2.0]

The single consolidated breaking release before v1.0.0. Everything that was ever
going to break breaks here, so that v1 can freeze the API — after v1, a breaking
change costs a `/v2` import path permanently. There are no deprecation shims;
see [MIGRATION.md](MIGRATION.md) for an old/new table.

The common path is unchanged: `client.Get(ctx, "/path")` and its siblings keep
their signatures. What moves is how you obtain the client, how you configure it,
and what a few methods return.

### Added

- `Request.SetQuery`, `AddQuery`, and `SetQueryValues` add query parameters
  with proper escaping, merged with anything already written into the path.
- `Limiter.Stats` returns a snapshot of live users, requests, throttling, wait
  time, errors, and evictions. The counters are atomic loads and the user count
  sums a per-shard tally, so it is cheap enough to call on a scrape interval.
- `Config.Observer` reports throttling (with the expected delay), finished
  requests (status, latency, error), user evictions (with a reason), and durable
  job transitions. It is a struct of optional functions rather than an
  interface, so events can be added later without breaking implementations.
- `Config.MaxResponseBytes` caps the buffered response body (zero = unlimited,
  as `http.Client` does). Reading an unbounded body into memory is how a hostile
  or merely misbehaving upstream takes the process down.
- `Request.Stream` returns the raw `*http.Response` with its body unread, for
  responses too large to hold in memory. The caller closes the body; doing so
  releases the request, so `Shutdown` waits for it.
- `Config.RequestTimeout` bounds one HTTP round-trip. It excludes time spent
  waiting for a token: a request held back by throttling has not started, and
  charging that wait against its timeout would make the timeout a function of
  how busy the user is. It does not apply to `Request.Stream`, since a context
  deadline stays armed until the body is closed and would cut off the long
  download streaming exists for; use `TransportConfig.ResponseHeaderTimeout`
  there, which bounds the wait for headers without bounding the body.
- `Response.OK`, `Response.JSON`, and `Response.RetryAfter` (both the
  delta-seconds and HTTP-date forms), plus `Request.SetJSON`.
- `Config.ResultTTL` (default 24h) expires cached durable results. The cache is
  what makes a repeated `Durable` call cheap, but nothing bounded it, so on a
  busy service it was the dominant term in the database file's growth. Note that
  SQLite does not return freed pages to the filesystem: the file stops growing,
  it does not shrink.
- `Config.Retry` (a `RetryPolicy`) gives durable jobs exponential backoff with
  full jitter and an attempt ceiling; exhausting it dead-letters the job.
  Jitter is on by default because the failure that matters is correlated — an
  upstream outage stalls every job at once, and a fixed schedule sends them all
  back at the same instant.
- `Config.RetryOn` lets a caller decide which *responses* are worth repeating.
  It is nil by default: a response of any status means the request was
  delivered, and pace does not interpret status codes on your behalf.
- `Config.QueueWorkers` and `Config.QueuePollInterval` bound and pace the
  background retry loop.
- `Config.IdempotencyHeader` (default `Idempotency-Key`) is sent on every
  durable request carrying the job ID, so a cooperating server can collapse a
  retry into the original delivery. Set it to `"-"` to send nothing.
- `Config.AmbiguousPolicy` decides the fate of a durable job whose outcome is
  unknown after a crash: `AmbiguousAuto` (default) retries only when repeating
  is safe, `AmbiguousRetry` always retries, `AmbiguousPark` never does.
- `Config.OnDeadLetter` reports abandoned jobs, and `Limiter.DeadJobs` reads
  them back so they are visible to an operator after a restart.
- `Config.JobLease` bounds how long a claimed durable job stays owned, so a
  worker that crashes mid-send does not strand it.
- `ErrJobClaimed` reports that another worker owns a durable job.
- The SQLite schema is now versioned and migrated (`PRAGMA user_version`).
  Databases written by v0.1.0 upgrade in place; a database stamped newer than
  the running binary is refused rather than written through, so a rolled-back
  deploy cannot corrupt state a newer binary still expects to read.
- `TransportConfig` gains `Proxy`, `MaxConnsPerHost`, `ExpectContinueTimeout`,
  `DisableHTTP2`, and `DisableCompression`.
- `Request.AddHeader` appends a value without replacing existing ones, and
  `Request.Header()` exposes the underlying `http.Header`.
- `BatchStateStore` is an optional extension to `StateStore`. A store that
  implements it receives whole batches from the idle-user sweep and the final
  flush instead of one call per user, which matters when a sweep evicts
  thousands of users at once. The built-in SQLite backend implements it.
- `Config.StoreTimeout` bounds every `StateStore` operation (default 5s), so a
  wedged backend degrades to a fresh bucket rather than wedging the request.
- `Config.Shards` sets the lock-striping width (default 256, rounded up to a
  power of two, capped at 2^20). Lower it when running one Limiter per upstream.
- `Client.Wait(ctx)` blocks until the user has a token, and `Client.Allow()`
  takes one without blocking — the non-blocking and blocking halves of the
  `x/time/rate` trio, for pacing work pace does not perform itself.
- `Request.Do(ctx, method, path)` for methods without a named helper.
- `Client.UserID()` reports the identity a handle is bound to.
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

- **Breaking:** `Config.OnThrottle` is replaced by `Config.Observer.Throttled`,
  which carries the expected delay, the token count, and the limit in force.
  The old callback reported only that throttling had happened, which is the one
  thing the caller could already infer.
- **Breaking:** `Client.Tokens` returns `(float64, bool)` instead of using -1
  as a sentinel, which could not be told apart from a legitimately negative
  count. `Client.Evict` takes a context and returns `(bool, error)`: it performs
  store I/O, and swallowing that error into a log line is the wrong choice for
  an operation the caller invoked deliberately.
- **Breaking:** request headers are an `http.Header` rather than a
  `map[string]string`, which could not express a header that legitimately
  repeats (`Accept`, `Set-Cookie`). Durable jobs persisted by v0.1.0 have their
  stored headers converted by the schema migration.
- **Breaking:** `StateStore` methods now take a `context.Context`, and
  `SavedState` is replaced by `State` with a `time.Time` rather than unix
  nanoseconds. The README advertised Redis and Postgres backends that the old
  signature could not support — its own example closed over a `ctx` variable
  that did not exist in scope. Migration:

  ```go
  // was: Save(userID string, state pace.SavedState) error
  func (s *MyStore) Save(ctx context.Context, userID string, st pace.State) error
  ```
- **Breaking:** the rate-limit token is now taken when a request is sent, not
  when the builder is handed out. `Client.Request()` takes no context, returns
  no error, and costs nothing; the context moves to the terminal methods:

  ```go
  resp, err := lim.Client("alice").Request().
      SetHeader("X-Request-ID", "req-001").
      Post(ctx, "/resources")
  ```

  Code that called `Request(ctx)` only to acquire a token should call
  `Client.Wait(ctx)` instead. `Client.Get`/`Post`/`Put`/`Delete`/`Patch` are
  unchanged.
- **Breaking:** `Client.Durable(ctx, id)` is now `Client.Durable(id)
  (*Request, error)`, with the context passed to the terminal method. The
  deferred-error field it used to stash setup failures in is gone.
- **Breaking:** `New` returns a `*Limiter` rather than a `*Client`, and per-user
  handles come from `Limiter.Client(userID)`. `Config.Name` and `Client.For` are
  gone, and `Close`/`Shutdown` moved from `Client` to `Limiter`, where the
  resources they release actually live. `Close` now returns an error, so
  `*Limiter` satisfies `io.Closer`. Migration:

  ```go
  lim, err := pace.New(cfg)   // was: client, err := pace.New(cfg)
  defer lim.Close()
  alice := lim.Client("alice") // was: client.For("alice")
  resp, err := alice.Get(ctx, "/items/42") // unchanged
  ```
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

- **The durable queue never provided exactly-once delivery, and the README said
  it did.** A job dispatched but never recorded — a crash between the response
  and the commit, or a `Complete` that failed and was only logged — was replayed
  on restart, sending the request a second time. For a payment that is a
  duplicate charge. Delivery is now documented as at-least-once, the intent to
  send is committed *before* dispatch so the ambiguous window is detectable
  rather than silent, and `Config.AmbiguousPolicy` decides what happens to a job
  caught in it instead of blindly re-sending.
- `internal/store` stamped `created_at` and `completed_at` from `time.Now()`
  while everything else read `Config.Clock`, so the two disagreed and durable
  timestamps were untestable. The store is now told the time rather than
  deciding it, the same correction made earlier for `RestoreBucket`.
- Retrying happened only at startup, and only by spawning one goroutine per
  pending job. A fifty-thousand-job backlog became fifty thousand goroutines,
  each holding a request and a body buffer, and nothing was retried until the
  next restart. Recovery and retries now share one bounded worker pool.
- Two workers could send the same durable job. `INSERT OR IGNORE` deduplicates
  the row, not the send, so a replay goroutine and a live caller — or two
  processes sharing the database — could each decide they were the leader.
  Claiming a job is now a single conditional `UPDATE`.
- Two workers sharing a database could still double-send, by a second route the
  claim could not close. `Complete` deletes the pending row, so a finished job
  leaves nothing for `INSERT OR IGNORE` to conflict with: the losing worker
  reads the result cache just before the winner writes it, re-inserts the job as
  a fresh `queued` row, and legitimately wins the claim on it. `Enqueue` is now
  conditional on no recorded result. Found by writing the test for the
  multi-process guarantee the README states — it double-sent 14 of 40 jobs.
- `store.Release` matched on the job ID alone, so a worker whose lease had
  expired could return to the queue a job another worker was already sending,
  producing a third copy. It now matches on owner and state, and reports whether
  the release happened so the caller can stop rather than schedule a retry for a
  job it no longer owns.
- `Client.Allow`, `Client.Evict`, and `Limiter.DeadJobs` bypassed the shutdown
  barrier and could touch a store `Close` had already shut. The check-then-
  register sequence is now a single `enter`/`leave` pair rather than restated at
  each call site.
- `Client.Evict` called the `UserEvicted` observer with the shard write lock
  held, so an observer that asked the Limiter anything — `Tokens`, `Stats` —
  deadlocked against the eviction that notified it. It now fires outside the
  lock, and after the state has been persisted, so a failed save is no longer
  reported as a clean eviction.
- `Stats().Users` never returned to zero after `Close`: shutdown reported every
  remaining user as evicted but left them in the shards, so one snapshot claimed
  N users and +N evictions at once.
- `Request.Stream` was counted in `Stats.Requests` but skipped both
  `Stats.Errors` and `Observer.RequestFinished`, so the two halves of the metric
  described different populations.
- A failed `Complete` was logged at Warn and forgotten, silently converting a
  completed job into one that would be re-sent. It is now retried, and logged at
  Error when it still fails, because that is lost data.
- The built-in SQLite backend and user-supplied stores met at a private
  interface with a wrapper bridging them, so the batteries-included path was a
  special case that custom backends could not exercise. SQLite is now adapted to
  the same public `StateStore` a caller would implement, leaving one code path.
- The GC sweep held a shard's write lock across every `store.Save`, so evicting
  idle users blocked live requests hashing to that shard for the duration of a
  SQLite transaction each. Sweeping 2,000 users took ~4.6s of lock-held time;
  it now takes ~12ms with no lock held during persistence at all, by
  snapshotting under the lock, persisting outside it, and only then deleting.
  `saveAll` had the same shape and got the same treatment, and `SaveBatch`
  collapses a per-user transaction storm into chunks of 512.
- A user who made a request *during* a sweep was evicted anyway: `lastUsed` is
  updated atomically without taking the shard lock, so the sweep could not see
  it. The delete phase now skips any user touched since the snapshot.
- `userFor` called `store.Load` while holding the shard write lock, so a
  network-backed `StateStore` — the Redis and Postgres backends the README
  advertises — would close a shard for the length of a round-trip on every new
  user. The load now happens before the lock is taken.
- `Shutdown(ctx)` did not wait for in-flight requests, despite documenting that
  it does. The active-request counter was scoped to the call that returned the
  builder, which finishes before the HTTP round-trip starts, so the counter was
  already zero by the time a request was on the wire — and Shutdown returned and
  closed the store underneath it. The registration now spans the whole
  operation.
- Shutdown's deadline could not cancel a round-trip already in progress, so a
  server that never answered would outlive the Limiter. Each request context is
  now merged with the Limiter's lifetime.
- `Close` never waited for the GC goroutine: `gcWg` was created and added to but
  only ever waited on from a test helper, leaving a sweep free to `Save` into a
  store that `Close` had already shut. Both `Close` and `Shutdown` now run one
  teardown sequence that drains the GC loop, replay, and in-flight requests
  before touching the store.
- `Durable("")` silently degraded to a plain request *and* skipped rate limiting
  entirely, because dispatch keyed on a non-empty ID string while the plain
  branch assumed a token had already been paid for. An empty ID is now
  `ErrInvalidID`.
- A builder that was created and then abandoned burned a token nothing could
  refund. Building is now free.
- Replayed durable jobs ran on `context.Background()` and so could not be
  cancelled at shutdown; they now run on the Limiter's context.
- Lifecycle methods hung off a per-user handle, so `bob := alice.For("bob");
  bob.Close()` tore down the limiter `alice` and every other user shared. Only a
  `Limiter` can be closed now, so the mistake no longer compiles.
- With `Config.Name` unset, `userID` was the empty string and every convenience
  call silently rate-limited all traffic as one anonymous `""` user. Identity is
  now always supplied explicitly to `Limiter.Client`.
- The `_test.go` exclusion in `.golangci.yml` used the v1 `issues.exclude-rules`
  location, which v2 parses without complaint and ignores.
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
