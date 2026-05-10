package pace

import (
	"log/slog"
	"net/http"
	"time"
)

// SavedState holds the persisted snapshot of a single user's bucket.
// It is the element type exchanged between Client and a [StateStore].
type SavedState struct {
	Tokens   float64
	LastUsed int64 // unix nanoseconds
}

// StateStore persists per-user token state across process restarts and GC
// evictions. Implement this interface to use any backend (Redis, Postgres,
// DynamoDB, …) and supply it via [Config.Store].
//
// The built-in SQLite backend is selected via [Config.DBPath]; Config.Store
// and Config.DBPath are mutually exclusive.
type StateStore interface {
	// Save persists the token count for a user.
	Save(userID string, state SavedState) error
	// Load returns the saved state for a user.
	// Returning (zero, false, nil) when no prior state exists is valid.
	Load(userID string) (SavedState, bool, error)
	// Close releases any resources held by the store.
	Close() error
}

// Config configures a [Client].
type Config struct {
	// BaseURL is the base URL prepended to every request path. Required.
	BaseURL string

	// RatePerMinute is the maximum number of requests per user per minute.
	// Must be greater than zero.
	RatePerMinute int

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

	// Clock overrides wall-clock time. Nil uses the real system clock.
	// Useful for deterministic GC testing.
	Clock Clock

	// Logger receives internal warnings (e.g. store I/O errors during GC).
	// Nil defaults to [slog.Default].
	Logger *slog.Logger

	// DBPath is an optional path to a SQLite file used to persist per-user
	// token state across process restarts. Leave empty to disable persistence.
	// Mutually exclusive with [Config.Store].
	DBPath string

	// Store is an optional custom persistence backend. When set, DBPath must
	// be empty. Use this to plug in Redis, Postgres, or any other backend.
	Store StateStore

	// OnThrottle is called in the caller's goroutine when a request must wait
	// for a rate-limit token. Nil disables the callback.
	OnThrottle func(userID string)
}
