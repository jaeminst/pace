package limiter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/jaeminst/pace/internal/registry"
	"github.com/jaeminst/pace/internal/store"
	"github.com/jaeminst/pace/internal/urlx"
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
// Two methods, both about persistence. A store that also needs tearing down
// implements [io.Closer], which [Limiter.Close] discovers by type assertion —
// the same way [BatchStateStore] extends this interface. Close was a member
// here until v0.4.0, which forced every implementation to carry one whether it
// had resources or not; the README's own example wrote `func (r *RedisStore)
// Close() error { return nil }` because the interface demanded it.
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

// sqliteStateStore adapts the built-in SQLite backend to the public
// [StateStore] interface.
//
// This is the only adapter in the package. Previously the built-in backend and
// user-supplied stores met at a private interface with a wrapper bridging them,
// which meant the batteries-included path was a special case. Making SQLite
// just another StateStore means per-user state takes one code path whichever
// backend is configured.
//
// It is used only when [Config.Store] is unset. With both fields set, the
// SQLite file serves the durable queue and this adapter is not built, so
// user_state stays empty and the caller's Store owns every read and write.
type sqliteStateStore struct{ s *store.Store }

var (
	_ StateStore      = sqliteStateStore{}
	_ BatchStateStore = sqliteStateStore{}
)

func (a sqliteStateStore) Save(ctx context.Context, userID string, st State) error {
	return a.s.Save(ctx, userID, st.Tokens, st.LastUsed.UnixNano())
}

func (a sqliteStateStore) SaveBatch(ctx context.Context, states []UserState) error {
	rows := make([]store.UserState, len(states))
	for i, u := range states {
		rows[i] = store.UserState{
			UserID:   u.UserID,
			Tokens:   u.State.Tokens,
			LastUsed: u.State.LastUsed.UnixNano(),
		}
	}
	return a.s.SaveBatch(ctx, rows)
}

func (a sqliteStateStore) Load(ctx context.Context, userID string) (State, bool, error) {
	ss, found, err := a.s.Load(ctx, userID)
	if err != nil || !found {
		return State{}, found, err
	}
	return State{Tokens: ss.Tokens, LastUsed: time.Unix(0, ss.LastUsed)}, true, nil
}

func (a sqliteStateStore) Close() error { return a.s.Close() }

// Clock abstracts wall-clock time. Implement it to control time in tests.
//
// It has one method deliberately. pace may later recognise optional extensions
// — a timer source, say — by type assertion, in the same way [BatchStateStore]
// extends [StateStore]; an implementation that provides only Now will keep
// working, because pace would never require the extension. So there is nothing
// to pre-emptively widen this to before the v1 freeze.
//
// Note that the token bucket schedules its own waits against the real clock,
// since golang.org/x/time/rate owns that timer and takes no time argument. A
// fake Clock therefore drives expiry, restore, and every timestamp pace
// records, but not how long [Client.Wait] actually blocks.
type Clock interface {
	Now() time.Time
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }

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
	// pace closes it. If the value implements [io.Closer], [Limiter.Close] and
	// [Limiter.Shutdown] call Close on it as part of teardown — so do not share
	// one Store between two Limiters, or between pace and your own code, unless
	// you are content for the first shutdown to close it for everybody.
	// Implement Close as a no-op if pace should not own the lifetime.
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

	// Shared makes rate limiting apply across replicas rather than once per
	// process, by delegating the decision to a backend every replica consults.
	// The zero SharedConfig limits per process, which is the default.
	Shared SharedConfig

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

// validate reports the first invalid field in cfg.
func (cfg *Config) validate() error {
	if cfg.BaseURL == "" {
		return &ConfigError{Field: "BaseURL", Err: errors.New("required")}
	}
	if err := urlx.Validate(cfg.BaseURL); err != nil {
		return &ConfigError{Field: "BaseURL", Value: cfg.BaseURL, Err: err}
	}
	if cfg.Rate <= 0 || math.IsNaN(float64(cfg.Rate)) {
		// NaN needs saying separately: it is not <= 0, so the check above lets
		// it through, and the bucket built from it holds NaN tokens and refuses
		// every request for the life of the process. Found by fuzzing.
		return &ConfigError{Field: "Rate", Value: cfg.Rate, Err: errors.New("must be greater than zero")}
	}
	if cfg.Shards > maxShards {
		return &ConfigError{
			Field: "Shards",
			Value: cfg.Shards,
			Err:   fmt.Errorf("must not exceed %d", maxShards),
		}
	}
	return nil
}

// withDefaults returns a copy of cfg with every optional field resolved, so
// nothing downstream has to re-check for zero values.
func (cfg Config) withDefaults() Config {
	cfg.Rate = finiteRate(cfg.Rate)
	if cfg.Burst <= 0 {
		cfg.Burst = 1
	}
	cfg.Shards = roundUpPowerOfTwo(cfg.Shards)
	if cfg.IdleExpiry <= 0 {
		cfg.IdleExpiry = 10 * time.Minute
	}
	if cfg.GCInterval <= 0 {
		cfg.GCInterval = time.Minute
	}
	if cfg.StoreTimeout <= 0 {
		cfg.StoreTimeout = 5 * time.Second
	}
	if cfg.Shared.Timeout <= 0 {
		cfg.Shared.Timeout = 500 * time.Millisecond
	}
	cfg.Queue = cfg.Queue.withDefaults()
	if cfg.Clock == nil {
		cfg.Clock = stdClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Transport == nil {
		cfg.Transport = http.DefaultTransport
	}
	return cfg
}

// maxShards bounds Config.Shards. Far beyond any useful striping, but it makes
// the shard count provably small enough to mask with a uint32 and stops
// roundUpPowerOfTwo from overflowing.
const maxShards = 1 << 20

// roundUpPowerOfTwo returns the smallest power of two at least n, defaulting to
// registry.DefaultShards for non-positive input. shardIndex masks rather than divides, which
// requires the count to be a power of two.
func roundUpPowerOfTwo(n int) int {
	if n <= 0 {
		return registry.DefaultShards
	}
	if n > maxShards {
		n = maxShards
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}
