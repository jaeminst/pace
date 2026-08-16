package pace

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// State is the persisted snapshot of a single user's token bucket. It is the
// element type exchanged between a [Limiter] and a [StateStore].
type State struct {
	// Tokens is the bucket's token count at LastUsed. It may be fractional.
	Tokens float64
	// LastUsed is when the user last made a request.
	LastUsed time.Time
}

// UserState pairs a user with their state, for stores that write in batches.
type UserState struct {
	UserID string
	State  State
}

// StateStore persists per-user token state across process restarts and GC
// evictions. Implement it to use any backend (Redis, Postgres, DynamoDB, …)
// and supply it via [Config.Store].
//
// Every method receives a context bounded by [Config.StoreTimeout], so a
// backend that talks over a network can honour cancellation rather than block
// the caller indefinitely.
//
// The built-in SQLite backend is selected via [Config.DBPath]. Setting Store as
// well is supported and is how you get a custom state backend and a durable
// queue at the same time: Store takes every read and write of user state, and
// the SQLite file serves the queue alone.
type StateStore interface {
	// Save persists state for userID.
	Save(ctx context.Context, userID string, state State) error
	// Load returns the saved state for userID. Returning (State{}, false, nil)
	// when nothing is stored is valid and expected for a first-time user.
	Load(ctx context.Context, userID string) (State, bool, error)
	// Close releases any resources held by the store.
	Close() error
}

// BatchStateStore is an optional extension to [StateStore]. A store that
// implements it receives whole batches from the idle-user sweep and from the
// final flush on close, instead of one call per user.
//
// Implementing it matters: the sweep can evict thousands of users at once, and
// a round-trip each turns a background task into a sustained load spike.
type BatchStateStore interface {
	StateStore
	// SaveBatch persists every entry, or reports an error. Partial success
	// should be reported as an error so the caller can log it.
	SaveBatch(ctx context.Context, states []UserState) error
}

// Config configures a [Limiter].
type Config struct {
	// BaseURL is the base URL prepended to every request path. Required.
	BaseURL string

	// Rate is the maximum request rate per user. Required; must be greater
	// than zero. Build it with [PerSecond], [PerMinute], [PerHour], or
	// [Every], or use [Inf] to disable throttling.
	Rate Limit

	// Burst is the maximum number of tokens that can accumulate when the
	// endpoint is idle. Zero or negative values default to 1.
	Burst int

	// IdleExpiry is how long a user can be inactive before their in-memory
	// state is garbage-collected. Zero defaults to 10 minutes.
	IdleExpiry time.Duration

	// GCInterval controls how often the idle-user GC sweep runs.
	// Zero defaults to 1 minute.
	GCInterval time.Duration

	// Transport is the HTTP transport used for all requests. Nil defaults to
	// [http.DefaultTransport].
	Transport http.RoundTripper

	// MaxResponseBytes caps the buffered response body. A response larger than
	// this fails with [ErrBodyTooLarge]. Zero means unlimited, matching
	// [http.Client].
	//
	// Reading an unbounded body into memory is how a hostile or merely
	// misbehaving upstream takes the process down. Set this whenever you do not
	// control the far end. [Request.Stream] bypasses it, since a streamed body
	// is never fully buffered.
	MaxResponseBytes int64

	// RequestTimeout bounds one HTTP round-trip. Zero means the caller's
	// context is the only limit.
	//
	// It deliberately excludes time spent waiting for a rate-limit token: a
	// request held back by throttling has not started yet, and counting that
	// wait against its timeout would make the timeout a function of how busy
	// the user is.
	//
	// [Request.Stream] bypasses it, as it does MaxResponseBytes, and for the
	// same reason: a context deadline stays armed until the body is closed, so
	// applying one would cut off the long download Stream exists to enable. Use
	// [TransportConfig.ResponseHeaderTimeout] to bound a streamed request — it
	// limits the wait for headers without limiting the body.
	RequestTimeout time.Duration

	// Clock overrides wall-clock time. Nil uses the real system clock.
	// Useful for deterministic GC testing.
	Clock Clock

	// Logger receives internal warnings (e.g. store I/O errors during GC).
	// Nil defaults to [slog.Default].
	Logger *slog.Logger

	// DBPath is an optional path to a SQLite file that holds the durable queue,
	// and — unless [Config.Store] is set — per-user token state as well. Leave
	// empty to disable both.
	//
	// It is the only thing that provides a durable queue. Setting it alongside
	// Store is supported and is how you get a custom state backend and a queue
	// at the same time: Store owns token state, the file owns the queue, and
	// its user_state table stays empty.
	//
	// The database runs in WAL mode, which has two operational consequences.
	// It keeps "-wal" and "-shm" files alongside the one you name, so back up
	// and delete them together. And it is unsafe on a network filesystem —
	// NFS, SMB — because WAL relies on shared memory that those do not provide
	// coherently. Point DBPath at local storage.
	DBPath string

	// Store is an optional custom persistence backend for per-user token state.
	// Use it to plug in Redis, Postgres, or anything else.
	//
	// It does not provide a durable queue, and setting it does not disable one:
	// set [Config.DBPath] as well if you want both. When both are set, Store
	// takes every read and write of user state.
	Store StateStore

	// StoreTimeout bounds each [StateStore] operation. Zero defaults to 5s.
	StoreTimeout time.Duration

	// QuotaFor returns the quota for a user, overriding [Config.Rate] and
	// [Config.Burst] for that user alone. Nil — the default — gives every user
	// the same quota, which is what Rate and Burst mean without it.
	//
	// It is consulted when a user's bucket is created: their first request, or
	// the first after an eviction. It is not on the hot path, and it is called
	// with no shard lock held, so a slow QuotaFor delays one user's first
	// request rather than everyone who hashes to that shard. Even so, keep it
	// to a map lookup — it must not do I/O.
	//
	//	cfg.QuotaFor = func(userID string) pace.Quota { return tiers[tierOf(userID)] }
	//
	// To change a tier at run time, update whatever QuotaFor reads and then
	// call [Limiter.ReloadQuotas], or [Client.Evict] for a single user.
	QuotaFor func(userID string) Quota

	// SharedQuota makes rate limiting apply across replicas rather than once
	// per process, by delegating the decision to a backend every replica
	// consults. Nil — the default — limits per process.
	//
	// The local bucket stays, as a shadow that can only refuse. It never grants
	// a request the backend has not also granted, so it costs nothing in
	// correctness and saves a round-trip for every request this replica can
	// already tell is over its own share.
	//
	// Read [SharedQuota] and [Config.OnQuotaError] before adopting this. Most
	// callers who want "distributed rate limiting" are better served by setting
	// Rate to their share of the limit and handling 429s honestly; this trades
	// an operational dependency on every outbound call path for accuracy that
	// only matters when replicas are unevenly loaded.
	SharedQuota SharedQuota

	// QuotaNamespace is passed through in [TakeRequest.Namespace], so several
	// Limiters can share one backend without colliding. Ignored when
	// SharedQuota is nil.
	QuotaNamespace string

	// QuotaTimeout bounds each [SharedQuota] call. Zero defaults to 500ms.
	//
	// It is much shorter than StoreTimeout because it sits in front of every
	// request rather than in front of a user's first one.
	QuotaTimeout time.Duration

	// OnQuotaError decides what happens when the shared backend cannot be
	// reached. Zero is [QuotaFallbackLocal].
	OnQuotaError QuotaErrorPolicy

	// Queue configures the durable request queue. Every field in it is ignored
	// unless [Config.DBPath] is set, since that is what creates the queue.
	Queue QueueConfig

	// Shards is the number of lock-striped buckets the per-user map is split
	// across. Zero defaults to 256; other values are rounded up to a power of
	// two. Lower it when running many Limiters, one per upstream endpoint.
	Shards int

	// Observer receives notifications about requests, throttling, evictions,
	// and durable-job transitions. Nil disables all of them.
	//
	// Use it to feed metrics or tracing. For a periodic gauge, [Limiter.Stats]
	// is cheaper — it needs no hook at all.
	Observer *Observer
}
