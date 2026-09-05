# ADR 0014 — The Pool keeps no per-key HTTP state

**Status:** accepted (v0.3.0)

## Context

`config.WithCookieJarFor` scopes cookies to a key. A jar rides on
`http.Client.Jar`, and the Pool owned exactly one `http.Client` — so per-key
jars seem to demand per-key clients, and per-key clients seem to demand a cache:
a map from key to `*http.Client` living on the Pool, with everything a cache
drags in. When is an entry created, when is it dropped, does dropping it follow
the registry's idle eviction, and what happens to a session when it does?

None of that is necessary, because of two facts about `net/http`:

- **An `http.Client` is stateless.** It is a handful of fields — transport,
  jar, redirect policy, timeout. The expensive shared thing, the connection
  pool, lives in the `Transport`, and pace already keeps one `Transport` for
  the whole Pool.
- **Both request paths go through `http.Client.Do`**, which is what applies a
  jar and carries it across redirects. pace never calls `RoundTrip` directly.

So a per-key client does not need to *exist* between requests. It can be
assembled at the moment of use.

## Decision

**With a `WithCookieJarFor` hook, the Pool assembles an `http.Client` per
request; it caches nothing per key.**

```go
func (p *Pool) httpClientFor(key string) *http.Client {
	if p.jarFor == nil {
		return p.httpClient
	}
	return &http.Client{Transport: p.transport, Jar: p.jarFor(key, p.jar)}
}
```

Without the hook, every request uses the one shared client, exactly as before —
the fast path is untouched down to the pointer.

**The jars are the caller's.** The hook is where a key meets its jar, so the map
behind the hook — its growth, its eviction, whether a session survives a process
restart — is policy the caller sets, not pace. This is the same trade
[ADR 0013](0013-values-are-config-behaviour-is-an-option.md) made with
`WithQuotaFor`'s tier map, and the same one
[ADR 0005](0005-pace-ships-contracts-not-backends.md) made with storage: pace
defines where the decision plugs in and refuses to own the state behind it.

It also keeps two subsystems honestly separate. The registry evicts a key's
*rate-limiter* state on idleness; a cookie session has a different lifetime (the
upstream's, usually), and coupling the two would make an idle sweep silently log
keys out. With the jars outside pace, eviction never touches a session — not by
coordination, but because there is nothing to touch.

## Costs

**One allocation and one hook call per request, on the hook path only.** The
struct is four words; the connection pool is shared through the `Transport`, so
connection reuse is unchanged. `BenchmarkRequest_NoHTTP` covers the no-hook path
and did not move.

**The hook runs on every request**, where `WithQuotaFor` runs only when a bucket
is created. That is the price of keeping no cache: there is no place to remember
the last answer. The hook's doc says so and sets the same bar as `QuotaFor` — a
map lookup, no I/O, safe for concurrent use.

**A caller who wants sessions to expire must expire them.** pace will not do it,
and — see above — should not.

## Alternatives considered

**A per-key `*http.Client` cache on the Pool.** Saves one allocation per request
and buys a map, a lock on the request path (the first the HTTP half would have),
a lifecycle question with no right answer inside pace, and a second population
whose size tracks the registry's without being the registry. The allocation is
cheaper.

**Tie jar lifetime to registry eviction via the Observer.** Couples a session's
lifetime to an idleness policy chosen for memory, and turns the Observer — a
reporting surface — into a control channel. Rejected for both reasons.

**No feature: document "one Pool per identity".** That was v0.2.1's answer. It
multiplies connection pools and GC goroutines by the number of live identities,
which is the resource shape pace exists to avoid.
