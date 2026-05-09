package pace

import (
	"log/slog"
	"net/http"
	"time"
)

// EndpointConfig configures a single named endpoint.
type EndpointConfig struct {
	// BaseURL is the base URL prepended to every request path. Required.
	BaseURL string

	// RatePerMinute is the maximum number of requests per user per minute.
	// Must be greater than zero.
	RatePerMinute int

	// Burst is the maximum number of tokens that can accumulate when the
	// endpoint is idle. Zero or negative values default to 1.
	Burst int
}

// Config configures a [Manager].
type Config struct {
	// Endpoints maps endpoint names to their configurations. Required.
	Endpoints map[string]EndpointConfig

	// IdleExpiry is how long a user can be inactive before their in-memory
	// state is garbage-collected. Zero defaults to 10 minutes.
	IdleExpiry time.Duration

	// GCInterval controls how often the idle-user GC sweep runs.
	// Zero defaults to 1 minute.
	GCInterval time.Duration

	// Transport is the HTTP transport used for all requests. Nil defaults to
	// [http.DefaultTransport].
	Transport http.RoundTripper

	// Clock overrides wall-clock time. Nil uses the real system clock.
	// Useful for deterministic GC testing.
	Clock Clock

	// Logger receives internal warnings (e.g. store I/O errors during GC).
	// Nil defaults to [slog.Default].
	Logger *slog.Logger

	// DBPath is an optional path to a SQLite file used to persist per-user
	// token state across process restarts. Leave empty to disable persistence.
	DBPath string
}
