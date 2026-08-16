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
// The built-in SQLite backend is selected via [Config.DBPath]; Config.Store and
// Config.DBPath are mutually exclusive.
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
	RequestTimeout time.Duration

	// Clock overrides wall-clock time. Nil uses the real system clock.
	// Useful for deterministic GC testing.
	Clock Clock

	// Logger receives internal warnings (e.g. store I/O errors during GC).
	// Nil defaults to [slog.Default].
	Logger *slog.Logger

	// DBPath is an optional path to a SQLite file used to persist per-user
	// token state across process restarts, and to hold the durable queue.
	// Leave empty to disable persistence. Mutually exclusive with
	// [Config.Store].
	//
	// The database runs in WAL mode, which has two operational consequences.
	// It keeps "-wal" and "-shm" files alongside the one you name, so back up
	// and delete them together. And it is unsafe on a network filesystem —
	// NFS, SMB — because WAL relies on shared memory that those do not provide
	// coherently. Point DBPath at local storage.
	DBPath string

	// Store is an optional custom persistence backend. When set, DBPath must
	// be empty. Use this to plug in Redis, Postgres, or any other backend.
	Store StateStore

	// StoreTimeout bounds each [StateStore] operation. Zero defaults to 5s.
	StoreTimeout time.Duration

	// IdempotencyHeader is set to the job ID on every durable request, so a
	// server that honours it can collapse a retry into the original delivery.
	// This is what turns pace's at-least-once queue into effective exactly-once
	// against a cooperating endpoint. Zero defaults to "Idempotency-Key"; set
	// it to "-" to send no such header.
	IdempotencyHeader string

	// AmbiguousPolicy decides the fate of a durable job whose outcome is
	// unknown after a crash. Zero is [AmbiguousAuto].
	AmbiguousPolicy AmbiguousPolicy

	// OnDeadLetter is called when a durable job is abandoned rather than
	// retried. Nil disables the callback; the job is still recorded in the
	// dead-letter table.
	OnDeadLetter func(job DeadJob)

	// Retry controls backoff and the attempt ceiling for durable jobs.
	Retry RetryPolicy

	// RetryOn decides whether a response counts as a delivery worth repeating.
	// Nil — the default — means no response triggers a retry: the request
	// reached the server, which is what the queue promises.
	//
	// pace does not interpret status codes anywhere else, and it will not
	// start here. Your API knows which of its own responses are transient:
	//
	//	cfg.RetryOn = func(r *pace.Response) bool {
	//	    return r.StatusCode() == http.StatusTooManyRequests || r.StatusCode() >= 500
	//	}
	RetryOn func(resp *Response) bool

	// QueueWorkers bounds how many durable jobs are retried concurrently in
	// the background. Zero defaults to 4.
	QueueWorkers int

	// QueuePollInterval is how often the background poller looks for durable
	// jobs that have become due. Zero defaults to 1s.
	QueuePollInterval time.Duration

	// ResultTTL is how long a completed durable job's cached response is kept.
	// Zero defaults to 24 hours; a negative value keeps results forever.
	//
	// The cache is what makes a repeated Durable call cheap, but nothing else
	// bounds it: on a busy service the results table is the dominant term in
	// the database file's growth. Note that SQLite does not return freed pages
	// to the filesystem — the file stops growing, it does not shrink. Run
	// VACUUM periodically if that matters.
	ResultTTL time.Duration

	// JobLease is how long a claimed durable job stays owned by the worker
	// that took it. A worker that crashes mid-send leaves its claim to expire,
	// after which the job becomes eligible again. Zero defaults to 5 minutes.
	JobLease time.Duration

	// Shards is the number of lock-striped buckets the per-user map is split
	// across. Zero defaults to 256; other values are rounded up to a power of
	// two. Lower it when running many Limiters, one per upstream endpoint.
	Shards int

	// OnThrottle is called in the caller's goroutine when a request must wait
	// for a rate-limit token. Nil disables the callback.
	OnThrottle func(userID string)
}
