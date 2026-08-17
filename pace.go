package pace

import (
	"net/http"
	"time"

	impl "github.com/jaeminst/pace/internal/pace"
)

// Every exported name below is an alias for, or a thin call into, the
// implementation in internal/pace. See doc.go for why the package is shaped
// this way and where the full documentation lives.

// AmbiguousPolicy decides what happens to a durable job found mid-flight after
// a crash — one whose intent to send was committed but whose outcome was never
// recorded.
type AmbiguousPolicy = impl.AmbiguousPolicy

// BatchStateStore is an optional extension to [StateStore]. A store that
// implements it receives whole batches from the idle-user sweep and from the
// final flush on close, instead of one call per user.
type BatchStateStore = impl.BatchStateStore

// Client is a rate-limited HTTP caller bound to one user identity. Obtain one
// from [Limiter.Client]. It is a lightweight handle: every Client derived from
// the same Limiter shares that Limiter's buckets, store, and durable queue.
type Client = impl.Client

// Clock abstracts wall-clock time. Implement it to control time in tests.
type Clock = impl.Clock

// Config configures a [Limiter].
type Config = impl.Config

// ConfigError reports an invalid [Config] field. It is returned only by [New].
type ConfigError = impl.ConfigError

// DeadJob is a durable job that will not be retried. It is reported to
// [QueueConfig.OnDeadLetter] and retained in the dead-letter table.
type DeadJob = impl.DeadJob

// DeadJobQuery selects a page of the dead-letter table for [Limiter.DeadJobs].
type DeadJobQuery = impl.DeadJobQuery

// EvictInfo describes a user whose in-memory state has just been dropped.
type EvictInfo = impl.EvictInfo

// EvictReason says why a user's in-memory state was dropped.
type EvictReason = impl.EvictReason

// Grant is a backend's answer to a [TakeRequest].
type Grant = impl.Grant

// JobInfo describes a durable job changing state.
type JobInfo = impl.JobInfo

// JobPhase is a durable job's position in its lifecycle, as reported to
// [Observer.JobTransition].
type JobPhase = impl.JobPhase

// Limit is a maximum request rate, expressed in requests per second.
type Limit = impl.Limit

// LimitError reports that a request could not obtain a rate-limit token.
type LimitError = impl.LimitError

// Limiter throttles outbound HTTP requests on a per-user basis toward a single
// base URL. It owns every resource involved: the idle-user GC goroutine, the
// state store, and the durable queue.
type Limiter = impl.Limiter

// Observer receives notifications about what a [Limiter] is doing. Every field
// is optional; a nil hook is skipped.
type Observer = impl.Observer

// QueueConfig configures the durable request queue. Every field is ignored
// unless [Config.DBPath] is set, since that is what creates the queue.
type QueueConfig = impl.QueueConfig

// Quota is the rate and burst in force for one user.
type Quota = impl.Quota

// QuotaErrorPolicy decides what happens to a request when the shared backend
// cannot be reached. See [SharedConfig.OnError].
type QuotaErrorPolicy = impl.QuotaErrorPolicy

// Request is a chainable HTTP request builder. Obtain one via [Client.Request]
// or [Client.Durable], then call Get, Post, Put, Delete, or Patch to execute.
type Request = impl.Request

// RequestInfo describes a completed HTTP round-trip.
type RequestInfo = impl.RequestInfo

// Reservation is a rate-limit token held for a request the caller intends to
// make. Obtain one from [Client.Reserve].
type Reservation = impl.Reservation

// Response wraps an HTTP response. All fields are immutable after construction.
type Response = impl.Response

// RetryDecision is what [QueueConfig.RetryOn] is asked to judge: a delivered
// response, and the context in which it arrived.
type RetryDecision = impl.RetryDecision

// RetryPolicy controls how a durable job is retried after a delivery failure.
type RetryPolicy = impl.RetryPolicy

// SharedConfig configures cross-replica rate limiting. Every field is ignored
// unless Quota is set, since that is what turns it on.
type SharedConfig = impl.SharedConfig

// SharedQuota is a token supply shared by every process that consults it.
type SharedQuota = impl.SharedQuota

// State is the persisted snapshot of a single user's token bucket. It is the
// element type exchanged between a [Limiter] and a [StateStore].
type State = impl.State

// StateStore persists per-user token state across process restarts and GC
// evictions. Implement it to use any backend (Redis, Postgres, DynamoDB, …)
// and supply it via [Config.Store].
type StateStore = impl.StateStore

// Stats is a point-in-time snapshot of a [Limiter].
type Stats = impl.Stats

// TakeRequest is one request for shared tokens.
type TakeRequest = impl.TakeRequest

// ThrottleInfo describes a request that must wait for a token.
type ThrottleInfo = impl.ThrottleInfo

// TransportConfig holds tuneable knobs for the underlying HTTP transport.
// Pass the result of [NewTransport] to [Config.Transport].
type TransportConfig = impl.TransportConfig

// UserState pairs a user with their state, for stores that write in batches.
type UserState = impl.UserState

// WaitingSharedQuota is an optional extension to [SharedQuota], discovered by
// type assertion in the same way [BatchStateStore] extends [StateStore].
type WaitingSharedQuota = impl.WaitingSharedQuota

// Inf is a Limit that permits requests without throttling. A Limiter
// configured with Inf ignores Burst.
const Inf = impl.Inf

// AmbiguousAuto retries when repeating the request is safe — an idempotent
// method (GET, HEAD, PUT, DELETE, OPTIONS, TRACE), or any method when
// QueueConfig.IdempotencyHeader is set so the server can collapse the retry.
// Anything else is parked. This is the default and the right answer for
// most callers.
const AmbiguousAuto = impl.AmbiguousAuto

// AmbiguousRetry always retries. Choose it when every endpoint you call is
// safe to repeat, or when duplicate delivery is cheaper than loss.
const AmbiguousRetry = impl.AmbiguousRetry

// AmbiguousPark never retries: the job goes to the dead-letter table and
// QueueConfig.OnDeadLetter fires. Choose it when a duplicate would be worse than
// a drop — charging a card, sending a message.
const AmbiguousPark = impl.AmbiguousPark

// EvictIdle means the GC sweep collected an inactive user.
const EvictIdle = impl.EvictIdle

// EvictExplicit means a caller invoked Client.Evict.
const EvictExplicit = impl.EvictExplicit

// EvictShutdown means the Limiter closed and flushed everything.
const EvictShutdown = impl.EvictShutdown

// JobClaimed means a worker took ownership and is about to send.
const JobClaimed = impl.JobClaimed

// JobCompleted means the response was recorded.
const JobCompleted = impl.JobCompleted

// JobRetrying means the attempt failed and another is scheduled.
const JobRetrying = impl.JobRetrying

// JobDead means the job was abandoned.
const JobDead = impl.JobDead

// QuotaFallbackLocal falls back to this replica's local bucket, which
// enforces the configured rate per replica rather than in total. This is
// the default, and it is the same trade pace already makes when a
// StateStore is unavailable: refusing traffic because bookkeeping is down
// is usually worse than briefly over-serving.
const QuotaFallbackLocal = impl.QuotaFallbackLocal

// QuotaDeny refuses the request with [ErrQuotaUnavailable]. Choose it when
// exceeding the shared limit is worse than dropping traffic — a hard
// contractual cap, or an upstream that bans rather than throttles.
const QuotaDeny = impl.QuotaDeny

// QuotaAllow lets the request through without consulting anything. Choose
// it only when the limit is advisory and availability is the point.
const QuotaAllow = impl.QuotaAllow

// ErrBodyTooLarge is returned when a response body exceeds
// [Config.MaxResponseBytes].
var ErrBodyTooLarge = impl.ErrBodyTooLarge

// ErrClosed is returned once the [Limiter] has been closed or has begun
// shutting down. It reports that the Limiter will accept no further work — not
// that a particular request timed out; see [LimitError] for that.
var ErrClosed = impl.ErrClosed

// ErrInvalidID is returned by [Client.Durable] when id is empty. An empty ID
// cannot identify a job, so it is rejected rather than quietly degrading to a
// non-durable request.
var ErrInvalidID = impl.ErrInvalidID

// ErrJobClaimed reports that another worker — possibly in another process
// sharing the same database — already owns this durable job. It is not a
// failure: it means the request is being sent exactly once, by someone else.
var ErrJobClaimed = impl.ErrJobClaimed

// ErrNoQueue is returned by [Client.Durable] when no durable queue is
// configured.
var ErrNoQueue = impl.ErrNoQueue

// ErrQuotaUnavailable reports that the shared backend could not be reached and
// [SharedConfig.OnError] is [QuotaDeny]. The cause is wrapped.
var ErrQuotaUnavailable = impl.ErrQuotaUnavailable

// ErrStreamDurable is returned by [Request.Stream] on a durable request. The
// queue caches a response so it can be returned to a later caller, which it
// cannot do for a stream that is consumed once.
var ErrStreamDurable = impl.ErrStreamDurable

// New creates a Limiter from cfg. It starts a background GC goroutine and opens
// the configured store (SQLite or custom). Call [Limiter.Close] or
// [Limiter.Shutdown] when the Limiter is no longer needed.
//
// Bind a user identity with [Limiter.Client].
func New(cfg Config) (*Limiter, error) { return impl.New(cfg) }

// PerSecond returns the Limit permitting n requests per second.
func PerSecond(n float64) Limit { return impl.PerSecond(n) }

// PerMinute returns the Limit permitting n requests per minute.
func PerMinute(n float64) Limit { return impl.PerMinute(n) }

// PerHour returns the Limit permitting n requests per hour.
func PerHour(n float64) Limit { return impl.PerHour(n) }

// Every returns the Limit permitting one request per interval.
// Every(0) or a negative interval returns [Inf].
func Every(interval time.Duration) Limit { return impl.Every(interval) }

// NewTransport returns an *http.Transport configured from cfg.
// Use it to set connection timeouts, TLS settings, and keep-alive behaviour
// before passing the result to [Config.Transport]:
//
//	lim, err := pace.New(pace.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    pace.PerMinute(60),
//	    Transport: pace.NewTransport(pace.TransportConfig{
//	        DialTimeout:         5 * time.Second,
//	        TLSHandshakeTimeout: 3 * time.Second,
//	        MaxIdleConnsPerHost: 10,
//	    }),
//	})
func NewTransport(cfg TransportConfig) *http.Transport { return impl.NewTransport(cfg) }
