package pace

import "net/http"

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
	// Config.IdempotencyHeader is set so the server can collapse the retry.
	// Anything else is parked. This is the default and the right answer for
	// most callers.
	AmbiguousAuto AmbiguousPolicy = iota

	// AmbiguousRetry always retries. Choose it when every endpoint you call is
	// safe to repeat, or when duplicate delivery is cheaper than loss.
	AmbiguousRetry

	// AmbiguousPark never retries: the job goes to the dead-letter table and
	// Config.OnDeadLetter fires. Choose it when a duplicate would be worse than
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

// DeadJob is a durable job that will not be retried. It is reported to
// [Config.OnDeadLetter] and retained in the dead-letter table.
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
