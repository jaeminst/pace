# ADR 0005 — pace ships contracts, not backends

**Status:** accepted (v0.8.0)

Supersedes [ADR 0002](0002-sqlite-wal-with-a-separate-reader.md) and
[ADR 0003](0003-at-least-once-not-exactly-once.md).

## Context

Two of the three things a caller can plug into pace already had no
implementation in the repository, and the third had one.

`shared.Quota` — cross-replica rate limiting — ships as an interface plus
`shared/quotatest`, a runnable conformance suite. The reasoning was written down
in that package's doc when it was added:

> pace ships no backend of its own — a Redis or Postgres implementation would be
> a second module to version, tag and support, for a feature whose value depends
> entirely on your infrastructure. What it ships instead is the contract,
> executable.

`store.Store` — per-user token persistence — shipped as an interface *and* as a
SQLite implementation selected with one config field, `Config.DBPath`.

The durable request queue shipped with no contract at all. Its storage was
`runner.Jobs`, eleven methods of SQL over that same SQLite file, reachable only
by setting `DBPath`.

So the library made the argument in one place and did the opposite in another,
and the cost of the exception was not small:

- **Ten dependencies.** `modernc.org/sqlite` and, transitively, nine more.
  `go mod why -m` put every indirect requirement in `go.mod` down to that one
  import. `go.sum` was 52 lines.
- **A schema to migrate forever.** Four migrations, a `PRAGMA user_version`
  chain that refuses to open a newer file, and a header-format conversion
  carried since v0.1.0 — for a table holding two numbers under a key, plus three
  tables for the queue.
- **A compatibility carve-out.** The v1 promise had to exclude the on-disk
  format explicitly, while covering the Go API over it.
- **Operational caveats in a rate limiter's README.** WAL sidecar files, and
  "unsafe on NFS or SMB".
- **A coupling nothing checked.** The queue's SQL lived in `runner`, the schema
  in `sqlite`; a column added in one for a query in the other had no compiler
  between them. Both package docs said so.

## Decision

**Delete the SQLite backend. Ship `store.Store` the way `shared.Quota` is
shipped — the interface, a reference implementation, and the contract as an
executable test suite.**

- `store/storetest` is the contract: eight checks, each stating the guarantee it
  protects in its failure message.
- `store/memory` is a reference implementation that passes it. It is documented
  as a test double, not persistence: nothing it holds survives the process.
- `Config.DBPath` is gone. `Config.Store` is the only way to persist, and
  without one a Limiter is in-memory — a restart starts every user at a full
  burst.

**Delete the durable request queue with it.**

This is the harder half, and it is not a consequence of the first — it is a
decision the first one forces into the open. The queue's guarantee was
structural rather than incidental: `Claim` is one conditional `UPDATE`, and that
single statement is the entire reason two workers racing for a job cannot both
win. Keeping the feature without a shipped implementation meant publishing an
eleven-method interface whose contract is *cross-process atomicity*, with
nothing implementing it and nothing to check an implementation against.

A contract nobody can be expected to satisfy correctly is worse than no feature.
`shared.Quota` is defensible at four methods because the property — an atomic
decrement — is one a Redis or Postgres user already knows how to provide.
"Claim exclusively, release on the same ownership, do not resurrect a completed
job, page a dead-letter table on a composite cursor" is not that.

## Consequences

**The module depends on `golang.org/x/time` and nothing else.** `go.sum` is two
lines. There is no cgo, no driver, no schema, no file format.

**pace is a rate limiter.** Roughly 6,000 lines went, about 30% of the
repository. What is left does one thing.

**Durable requests are gone with no migration path.** A caller who needs them
should stay on v0.7.0. This is stated plainly in `docs/MIGRATION.md` rather than
softened: there is no replacement in the library, and pretending otherwise would
send people to write an eleven-method store against a contract this ADR has just
argued nobody should be asked to satisfy.

**Persistence is now work a caller does.** Two methods against whatever already
holds their state, checked with `storetest.Suite`. `examples/store` writes one
in forty lines, against a JSON file, and demonstrates the restart it claims.

**Coverage went up, not down** — 93.7% to 94.4%. The CI gate's own comment
blamed unreachable SQL error branches for most of the shortfall, and it was
right.

**ADR 0002 and ADR 0003 become history.** They are kept rather than deleted:
0002 records why the WAL and reader-handle split were necessary, and 0003
records that v0.1.0 shipped a false "exactly-once" claim and how it was
corrected. Both are worth reading by anyone who builds a queue on this contract
elsewhere. Neither describes code that exists.

## Alternatives considered

**Move SQLite to a nested module.** `sqlite/go.mod`, versioned separately. It
keeps the main module at one dependency and keeps the implementation available.
Rejected because it does not reduce what has to be maintained — the schema, the
migrations and the WAL caveats all survive, now with a second tag stream — and
because the main module's own tests would still need an in-memory store, so the
work is the same and the surface is larger.

**Keep the queue behind a `queue.Store` contract.** Rejected above: eleven
methods, a cross-process atomicity requirement, and no implementation. A
conformance suite could be written for it, and an in-memory implementation would
pass every check the suite could make in one process while telling a reader
nothing about whether their Postgres version is correct.

**Keep `Config.DBPath` and delete only the queue.** This keeps the ten
dependencies, the schema and the migration chain in order to store two numbers
under a key — the case a `map` covers for testing and a caller's existing
database covers in production. It buys convenience for the first five minutes of
using the library and charges for it in every release after.
