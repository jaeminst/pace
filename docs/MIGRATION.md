# Migrating

While the version is below 1.0.0, any release may break the API. The freeze
begins at v1.0.0; until then, expect a section here for every release.

- [From v0.2.0 to v0.2.1](#migrating-from-v020) — nothing to do
- [From v0.1.0 to v0.2.0](#migrating-from-v010) — the library becomes packages

# Migrating from v0.2.0

Nothing to do. The release adds one optional field and changes no behaviour:
without `Config.CookieJar` pace sends and stores no cookies, exactly as before.

If you were carrying a session cookie by hand on every request because there was
nowhere to put a jar, you can stop:

```go
// before
req.SetHeader("Cookie", "session="+token)

// after — set once, on the Config
cfg.CookieJar = jar
```

Read [Cookies](../README.md#cookies) before you do: one jar serves every key in
the Pool, which is right for a session your service holds and wrong for one that
belongs to an end user.

# Migrating from v0.1.0

Everything moved. The compiler finds nearly all of it, and the parts it cannot
are called out below.

## Imports

v0.1.0 was one package. There are now three you will name, and one more for the
rate vocabulary:

```go
import (
    "github.com/jaeminst/pace/bucket"  // Quota, Limit, NewQuota
    "github.com/jaeminst/pace/client"  // Pool, Client, Request, Response
    "github.com/jaeminst/pace/config"  // Config, Option
)
```

`github.com/jaeminst/pace` itself declares nothing now — importing it gets you a
documentation page and a compile error.

## Creating a pool

```go
// before
c, err := pace.New(pace.Config{
    BaseURL: "https://api.example.com",
    Rate:    pace.PerMinute(60),
    Burst:   10,
})

// after
pool, err := client.New(config.Config{
    BaseURL: "https://api.example.com",
    Quota:   bucket.NewQuota("60/m", 10),
})
```

`config.DefaultConfig("https://api.example.com")` gives you 100 a minute with a
burst of 10 if you have no rate in mind yet.

`Rate` and `Burst` are one `Quota` field. Build it from a string —
`bucket.NewQuota("60/m", 10)`, also `6/min`, `6rpm`, `1/s`, `100/hour`,
`100RPH`, `2.5/s`, `inf` — or from the constructors:
`bucket.Quota{Rate: bucket.PerMinute(60), Burst: 10}`. `NewQuota` panics on a
typo because the string is a literal in your source; `bucket.ParseQuota` returns
an error, for a rate that comes from a file or a flag.

## Clients

```go
// before
alice := c.For("alice")

// after
alice := pool.Client("alice")
```

**The word is `key`, not `userID`.** `Client.UserID()` is `Client.Key()`, and
every report carries `Key` where it carried `UserID`:
`limiter.LimitError.Key`, `observe.ThrottleInfo.Key`, `observe.EvictInfo.Key`,
`observe.RequestInfo.Key`, `shared.TakeRequest.Key`, `store.KeyState.Key`.

`Client.Tokens()` returns `(float64, bool)`. It used to report a sentinel for a
key it had never seen, which you could not tell from a real count.

## Per-key rates

New, and the reason `Config` grew an options list. A hook grades keys against
the configured quota:

```go
var tiers atomic.Pointer[map[string]bucket.Quota]  // replaced whole, never mutated

pool, err := client.New(cfg, config.WithQuotaFor(
    func(key string, def bucket.Quota) bucket.Quota {
        if q, ok := (*tiers.Load())[key]; ok {
            return q
        }
        return def   // def is cfg.Quota
    }))
```

**It must be safe for concurrent use** — it runs on request goroutines, one per
key whose bucket is being created, and on whatever goroutine calls
`ReloadQuotas`. Guard whatever it reads; a plain map here is a data race.

To change a rate while the process runs, swap what the hook reads and then call
`pool.ReloadQuotas()`, or `pool.Client(key).ReloadQuota()` for one key.

## State stores

`StateStore` and `SavedState` are `store.Store` and `store.State`, and the
interface is two methods:

```go
// before
type StateStore interface {
    Save(ctx context.Context, userID string, s pace.SavedState) error
    Load(ctx context.Context, userID string) (pace.SavedState, bool, error)
    Close() error
}

// after
type Store interface {
    Save(ctx context.Context, key string, s store.State) error
    Load(ctx context.Context, key string) (store.State, bool, error)
}
```

`Close` is gone from the interface. Implement `io.Closer` if you have resources
and pace will find it; if you do not, delete the method that returned nil
because the interface demanded one. `store.BatchStore` is the same kind of
opt-in for `SaveBatch`.

**Check your implementation against `store/storetest`:**

```go
func TestMyStore(t *testing.T) {
    storetest.Suite(t, func(t *testing.T) store.Store { return newMyStore(t) })
}
```

**The built-in SQLite store is gone**, along with the durable request queue and
`Client.Durable`. pace ships contracts, not backends. `store/memory` is the
reference implementation to read; a real one is yours to write, and the suite
above tells you when it is right.

## Observability

`Observer` is a struct of functions rather than an interface, so adding an event
does not break your implementation:

```go
cfg.Observer = &observe.Observer{
    Throttled:       func(ctx context.Context, i observe.ThrottleInfo) { … },
    RequestFinished: func(ctx context.Context, i observe.RequestInfo) { … },
    Evicted:         func(ctx context.Context, i observe.EvictInfo) { … },
}
```

Every hook takes a context, and every `Info` carries `Key`.

## Errors

- `*limiter.LimitError` is throttling — `Key`, `Limit`, `Burst`, `Delay`.
- `limiter.ErrClosed` is shutdown. Do not infer one from the other; a
  "would exceed context deadline" leaves `ctx.Err()` nil and used to be reported
  as a closed client.
- A non-2xx response is neither. Check `resp.OK()`, and `resp.RetryAfter()` for
  upstream's own statement of its limit.
- `*config.Error` names the field it rejected.

## Cross-replica limiting

New. `shared.Config{Backend: …}` on the `Config` hands the admission decision to
something you run — Redis, or anything implementing two methods — with a local
bucket kept as a shadow. `shared/quotatest.Suite` is the conformance suite.
Read [ADR 0004](adr/0004-shared-quota-is-approximate.md) before relying on it:
it is approximate by construction.

## Things that changed quietly

- **Restored token counts are exact.** They used to be rounded to a whole
  number, so a key with a burst of 1 lost any partial token on every restart.
- **A per-minute rate that does not divide 60s evenly is exact**, where it used
  to be truncated through a `time.Duration`.
- **A NaN or infinite rate is rejected** by `Config.Resolve` rather than
  producing a bucket that refuses every request forever.
- **Everything reads through `Config.Clock`**, so a fake clock makes the whole
  library deterministic, including the restore path.
