# ADR 0001 — A lock-striped map, not sync.Map

**Status:** accepted (predates v0.2.0; recorded here)

## Context

pace keeps one token bucket per user in memory. The access pattern is
read-dominated — an existing user on every request — with occasional writes when
a user first appears or is garbage-collected.

## Decision

A fixed array of shards, each a `map[string]*user` behind its own `sync.RWMutex`,
indexed by FNV-1a over the user ID. The count is configurable via `Config.Shards`
and defaults to 256.

`sync.Map` was the obvious alternative. It is tuned for entries that are written
once and read many times, which fits, but it does not fit what pace also needs:

- **The GC sweep iterates everything.** `sync.Map.Range` gives no consistent
  snapshot and no way to hold a section still while deciding what to evict. With
  striping, one shard can be sampled under a read lock and mutated under a write
  lock, which is what makes the three-phase sweep possible.
- **Eviction is the write path.** `sync.Map` degrades when writes are frequent,
  and eviction plus re-creation of active users is exactly that.
- **A concrete map is easier to reason about.** The per-shard mutex makes it
  obvious what is held during store I/O — which turned out to matter, since the
  original code held it across a SQLite write.

Shards are one allocated block rather than an array of pointers, and padded to a
cache line so that two shards' mutexes never share one.

## Consequences

- Lookup is a hash plus a read lock. Roughly 20ns for a 32-byte user ID, with no
  allocation.
- Memory is proportional to the shard count regardless of population: 256 empty
  maps for an idle Limiter. `Config.Shards` exists for callers running one
  Limiter per upstream endpoint, where that adds up.
- The hash must stay FNV-1a. A test pins `shardIndex` byte-for-byte against
  `hash/fnv`, including non-ASCII IDs, so a "cleanup" that switches to
  `range`-over-string — which decodes runes — cannot silently change the
  distribution.
