# Architecture decision records

One file per decision that shaped the library, kept because the reasoning is
harder to reconstruct than the code. A record is amended or superseded, never
rewritten: a decision that turned out wrong is more useful with the argument
that led to it still attached.

| | |
|---|---|
| [0001](0001-sharded-map-not-sync-map.md) | A sharded map, not `sync.Map` |
| [0002](0002-sqlite-wal-with-a-separate-reader.md) | SQLite WAL with a separate reader — *superseded by 0005* |
| [0003](0003-at-least-once-not-exactly-once.md) | At-least-once, not exactly-once — *superseded by 0005* |
| [0004](0004-shared-quota-is-approximate.md) | Shared quota is approximate |
| [0005](0005-pace-ships-contracts-not-backends.md) | pace ships contracts, not backends |
| [0006](0006-the-root-is-the-composition-root.md) | The root is the composition root — *superseded in part by 0009* |
| [0007](0007-contracts-carry-numbers-not-types.md) | Contracts carry numbers, not types |
| [0008](0008-the-root-re-exports-nothing.md) | The root re-exports nothing |
| [0009](0009-config-limiter-client.md) | config, limiter, client |
| [0010](0010-the-default-quota-is-live-state.md) | The default quota is live state — *superseded by 0012* |
| [0011](0011-the-vocabulary-belongs-to-the-bucket.md) | The rate vocabulary belongs to the bucket |
| [0012](0012-one-hook-holds-the-quota.md) | One hook holds the quota — *amended by 0013* |
| [0013](0013-values-are-config-behaviour-is-an-option.md) | Values are configuration, behaviour is an option — *amended in v0.3.0* |
| [0014](0014-the-pool-keeps-no-per-key-http-state.md) | The Pool keeps no per-key HTTP state |
| [0015](0015-the-transport-package-returns-to-the-standard-library.md) | The transport package returns to the standard library |

## A note on the version numbers inside

Records 0002 to 0013 were written while the library was reshaped between v0.1.0
and v0.2.0, and they narrate that work in terms of releases — "deleted in
v0.9.0", "until v0.12.0", "as of v0.13.0". **Those releases were never
published.** The release history starts v0.1.0, v0.2.0, v0.2.1, v0.3.0…; the
numbers those records cite between v0.1.0 and v0.2.0 were development milestones
that were collapsed into one release before it shipped.

Read them as an ordering, not as tags you can check out. The `Status` line of
each record gives the release it actually landed in.
