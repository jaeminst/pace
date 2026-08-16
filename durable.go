package pace

import (
	"context"
	"net/http"
	"time"
)

// QueueConfig configures the durable request queue. Every field is ignored
// unless [Config.DBPath] is set, since that is what creates the queue.
//
// These nine knobs were once nine top-level Config fields, which put the
// configuration of one optional subsystem in the same namespace as Rate and
// Burst. Nesting them keeps Config readable and makes "the queue is optional"
// visible in the shape of the type rather than only in prose.
type QueueConfig struct {
	// IdempotencyHeader is set to the job ID on every durable request, so a
	// server that honours it can collapse a retry into the original delivery.
	// This is what turns pace's at-least-once queue into effective exactly-once
	// against a cooperating endpoint. Zero defaults to "Idempotency-Key"; set
	// it to "-" to send no such header.
	IdempotencyHeader string

	// AmbiguousPolicy decides the fate of a job whose outcome is unknown after
	// a crash. Zero is [AmbiguousAuto].
	AmbiguousPolicy AmbiguousPolicy

	// OnDeadLetter is called when a job is abandoned rather than retried. Nil
	// disables the callback; the job is still recorded in the dead-letter
	// table.
	//
	// The context is the Limiter's own, cancelled at Close, so a hook that
	// notifies something over the network can bail instead of holding up
	// shutdown.
	OnDeadLetter func(ctx context.Context, job DeadJob)

	// Retry controls backoff and the attempt ceiling.
	Retry RetryPolicy

	// RetryOn decides whether a response counts as a delivery worth repeating.
	// Nil — the default — means no response triggers a retry: the request
	// reached the server, which is what the queue promises.
	//
	// pace does not interpret status codes anywhere else, and it will not
	// start here. Your API knows which of its own responses are transient:
	//
	//	cfg.Queue.RetryOn = func(_ context.Context, d pace.RetryDecision) bool {
	//	    return d.Response.StatusCode() >= 500 && d.Attempt < 3
	//	}
	//
	// It takes a struct rather than a bare response because this is the one
	// hook whose whole job is judgement, and judgement accumulates inputs. A
	// signature frozen at one argument could never learn the attempt number,
	// which "retry a 503 twice, not five times" needs.
	RetryOn func(ctx context.Context, d RetryDecision) bool

	// Workers bounds how many jobs are retried concurrently in the background.
	// Zero defaults to 4.
	Workers int

	// PollInterval is how often the background poller looks for jobs that have
	// become due. Zero defaults to 1s.
	PollInterval time.Duration

	// JobLease is how long a claimed job stays owned by the worker that took
	// it. A worker that crashes mid-send leaves its claim to expire, after
	// which the job becomes eligible again. Zero defaults to 5 minutes.
	JobLease time.Duration

	// ResultTTL is how long a completed job's cached response is kept. Zero
	// defaults to 24 hours; a negative value keeps results forever.
	//
	// The cache is what makes a repeated Durable call cheap, but nothing else
	// bounds it: on a busy service the results table is the dominant term in
	// the database file's growth. Note that SQLite does not return freed pages
	// to the filesystem — the file stops growing, it does not shrink. Run
	// VACUUM periodically if that matters.
	ResultTTL time.Duration
}

// withDefaults resolves every optional field, so nothing downstream has to
// re-check for zero values.
func (q QueueConfig) withDefaults() QueueConfig {
	if q.JobLease <= 0 {
		q.JobLease = 5 * time.Minute
	}
	if q.ResultTTL == 0 {
		q.ResultTTL = 24 * time.Hour
	}
	if q.Workers <= 0 {
		q.Workers = 4
	}
	if q.PollInterval <= 0 {
		q.PollInterval = time.Second
	}
	q.Retry = q.Retry.withDefaults()
	switch q.IdempotencyHeader {
	case "":
		q.IdempotencyHeader = "Idempotency-Key"
	case noIdempotencyHeader:
		q.IdempotencyHeader = ""
	}
	return q
}

// AmbiguousPolicy decides what happens to a durable job found mid-flight after
// a crash — one whose intent to send was committed but whose outcome was never
// recorded.
//
// This window cannot be closed. Once bytes leave the process, a crash before
// the response is recorded leaves no way to know whether the server acted. What
// pace can do is make the window visible and let you choose.
type AmbiguousPolicy int

const (
	// AmbiguousAuto retries when repeating the request is safe — an idempotent
	// method (GET, HEAD, PUT, DELETE, OPTIONS, TRACE), or any method when
	// QueueConfig.IdempotencyHeader is set so the server can collapse the retry.
	// Anything else is parked. This is the default and the right answer for
	// most callers.
	AmbiguousAuto AmbiguousPolicy = iota

	// AmbiguousRetry always retries. Choose it when every endpoint you call is
	// safe to repeat, or when duplicate delivery is cheaper than loss.
	AmbiguousRetry

	// AmbiguousPark never retries: the job goes to the dead-letter table and
	// QueueConfig.OnDeadLetter fires. Choose it when a duplicate would be worse than
	// a drop — charging a card, sending a message.
	AmbiguousPark
)

func (p AmbiguousPolicy) String() string {
	switch p {
	case AmbiguousAuto:
		return "auto"
	case AmbiguousRetry:
		return "retry"
	case AmbiguousPark:
		return "park"
	default:
		return "unknown"
	}
}

// isIdempotentMethod reports whether repeating the method is defined to be
// safe, per RFC 9110 §9.2.2.
func isIdempotentMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut,
		http.MethodDelete, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

// resolve reports whether an ambiguous job should be retried.
func (p AmbiguousPolicy) resolve(method, idempotencyHeader string) bool {
	switch p {
	case AmbiguousRetry:
		return true
	case AmbiguousPark:
		return false
	default: // AmbiguousAuto
		return idempotencyHeader != "" || isIdempotentMethod(method)
	}
}

// RetryDecision is what [QueueConfig.RetryOn] is asked to judge: a delivered
// response, and the context in which it arrived.
type RetryDecision struct {
	// Response is what the server returned. Never nil — RetryOn is consulted
	// only for a request that was delivered.
	Response *Response
	// Method and Path are the request that produced it.
	Method string
	Path   string
	// Attempt is which attempt this was, counting from one.
	Attempt int
}

// DeadJob is a durable job that will not be retried. It is reported to
// [QueueConfig.OnDeadLetter] and retained in the dead-letter table.
type DeadJob struct {
	ID       string
	UserID   string
	Method   string
	Path     string
	Headers  http.Header
	Body     []byte
	Attempts int
	// Reason explains why the job was abandoned, in human-readable form.
	Reason string
}

// noIdempotencyHeader is the sentinel a caller sets QueueConfig.IdempotencyHeader
// to in order to send no header at all. An empty string cannot mean that,
// because the zero value has to select the default.
const noIdempotencyHeader = "-"
