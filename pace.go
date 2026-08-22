package pace

import (
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/response"
)

// Limiter throttles outbound HTTP requests on a per-user basis toward a single
// base URL. It owns every resource involved: the idle-user GC goroutine and
// the state store.
type Limiter = limiter.Limiter

// Client is a rate-limited HTTP caller bound to one user identity. Obtain one
// from [Limiter.Client].
type Client = limiter.Client

// Request is a chainable HTTP request builder. Obtain one via [Client.Request].
type Request = limiter.Request

// Response wraps an HTTP response. All fields are immutable after construction.
type Response = response.Response

// Reservation is a token claimed for a future request, with the option to give
// it back.
type Reservation = limiter.Reservation

// Config configures a [Limiter].
type Config = limiter.Config

// Clock abstracts wall-clock time. Implement it to control time in tests.
type Clock = limiter.Clock

// ConfigError reports an invalid [Config] field. It is returned only by [New].
type ConfigError = limiter.ConfigError

// LimitError reports that a request was throttled, and carries the limit that
// was in force.
type LimitError = limiter.LimitError

// The sentinel errors a caller matches with [errors.Is].
var (
	ErrClosed       = limiter.ErrClosed
	ErrBodyTooLarge = limiter.ErrBodyTooLarge
)

// New creates a Limiter from cfg. It starts a background GC goroutine and opens
// the configured store (SQLite or custom). Call [Limiter.Close] or
// [Limiter.Shutdown] when the Limiter is no longer needed.
//
// Bind a user identity with [Limiter.Client].
func New(cfg Config) (*Limiter, error) { return limiter.New(cfg) }
