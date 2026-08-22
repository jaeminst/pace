# ADR 0002 — SQLite in WAL mode, with a separate reader handle

**Status:** superseded by [ADR 0005](0005-pace-ships-contracts-not-backends.md) (v0.2.0)

The SQLite backend was removed in v0.8.0 and no code described below still
exists. This is kept as a record of why the split was necessary, which is worth
reading by anyone implementing `store.Store` on SQLite themselves.

## Context

SQLite allows one writer. v0.1.0 expressed that as `db.SetMaxOpenConns(1)` on a
single `*sql.DB`, with the comment "a single connection avoids SQLITE_BUSY
contention".

That is true about writers, and wrong about the pool. Capping the whole pool at
one connection also serialises *reads* behind writes — and reads are on the
request path. A new user's `Load` queued behind whatever the GC sweep was
committing, and every durable `Get` behind that.

## Decision

Two handles to the same file, in WAL mode:

- **wdb** — one connection, one writer, as before. `Save`, `SaveBatch`,
  `Enqueue`, `Claim`, `Release`, `Kill`, `Complete`, `PurgeResults`, and the
  migrations.
- **rdb** — a small pool. `Load`, `Get`, `Pending`, `Due`, `Dead`.

WAL is what makes this worth doing: readers see a consistent snapshot without
waiting for the writer. `busy_timeout=5000` covers a second process on the file
and the WAL checkpointer. `synchronous=NORMAL` is the matching durability
choice — under WAL it still survives a process crash, losing at most the most
recent commits to a power failure, which for token accounting costs a bucket
that refills slightly early.

`ClaimN` reads back the attempt count it just wrote, and does that on **wdb**,
so it cannot miss its own write.

Measured, on this hardware:

| | before | after |
|---|---|---|
| One durable job (3 commits) | ~6.4ms | ~283µs |
| `Load` while a writer is busy | ~730µs | ~19µs |

## Consequences

- WAL keeps `-wal` and `-shm` files beside the database. They must be backed up
  and deleted together; this is documented on `Config.DBPath`.
- WAL is unsafe on network filesystems (NFS, SMB), which rely on shared memory
  those do not provide coherently. `DBPath` must point at local storage.
- Two handles must both be closed. `Store.Close` closes the reader first, and
  joins the errors.
- A rejected alternative: a write-coalescing goroutine. The only high-frequency
  writer is the sweep, which `SaveBatch` already collapses into chunked
  transactions. A general coalescer would put an unbounded durability window in
  front of `Complete` — which is the guarantee the durable queue exists to
  provide.

## Verification

The property is pinned by a test, not a benchmark. The writer pool holds exactly
one connection, so an open write transaction occupies it entirely; a read
sharing that pool would have nowhere to go. The test opens such a transaction
and then reads, and against a shared pool it fails with "Load blocked on the
writer's connection". Timing could not show this reliably — a read collides with
the writer only occasionally, and the resulting tail is indistinguishable from
scheduler noise.
