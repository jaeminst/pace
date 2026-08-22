package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/shared"
	"github.com/jaeminst/pace/store"
	"github.com/jaeminst/pace/urlx"
)

// Error reports an invalid [Config] field. It is returned only by [Config.Resolve].
type Error struct {
	// Field is the offending field's name, without the Config prefix.
	Field string
	// Value is what was supplied, when showing it helps.
	Value any
	// Err is the underlying cause, if any.
	Err error
}

func (e *Error) Error() string {
	switch {
	case e.Err != nil && e.Value != nil:
		return fmt.Sprintf("pace: invalid Config.%s (%v): %v", e.Field, e.Value, e.Err)
	case e.Err != nil:
		return fmt.Sprintf("pace: invalid Config.%s: %v", e.Field, e.Err)
	case e.Value != nil:
		return fmt.Sprintf("pace: invalid Config.%s: %v", e.Field, e.Value)
	default:
		return "pace: invalid Config." + e.Field
	}
}

func (e *Error) Unwrap() error { return e.Err }

// Clock abstracts wall-clock time. Implement it to control time in tests.
//
// It has one method deliberately. pace may later recognise optional extensions
// — a timer source, say — by type assertion, in the same way [github.com/jaeminst/pace/store.BatchStore]
// extends [github.com/jaeminst/pace/store.Store]; an implementation that provides only Now will keep
// working, because pace would never require the extension. So there is nothing
// to pre-emptively widen this to before the v1 freeze.
//
// Note that the token bucket schedules its own waits against the real clock,
// since golang.org/x/time/rate owns that timer and takes no time argument. A
// fake Clock therefore drives expiry, restore, and every timestamp pace
// records, but not how long a Client.Wait actually blocks.
type Clock interface {
	Now() time.Time
}

type stdClock struct{}

func (stdClock) Now() time.Time { return time.Now() }

// Config configures a [github.com/jaeminst/pace/Limiter].
type Config struct {
	// BaseURL is the base URL prepended to every request path. Required.
	BaseURL string

	// Rate is the maximum request rate per user. Required; must be greater
	// than zero. Build it with [PerSecond], [PerMinute], [PerHour] or
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
	// this fails with
	// [github.com/jaeminst/pace/client.ErrBodyTooLarge]. Zero means
	// unlimited, matching
	// [http.Client].
	//
	// Reading an unbounded body into memory is how a hostile or merely
	// misbehaving upstream takes the process down. Set this whenever you do not
	// control the far end. Request.Stream bypasses it, since a streamed body
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
	// Request.Stream bypasses it, as it does MaxResponseBytes, and for the
	// same reason: a context deadline stays armed until the body is closed, so
	// applying one would cut off the long download Stream exists to enable. Use
	// transport.Config.ResponseHeaderTimeout to bound a streamed request — it
	// limits the wait for headers without limiting the body.
	RequestTimeout time.Duration

	// Clock overrides wall-clock time. Nil uses the real system clock.
	// Useful for deterministic GC testing.
	Clock Clock

	// Logger receives internal warnings (e.g. store I/O errors during GC).
	// Nil defaults to [slog.Default].
	Logger *slog.Logger

	// Store is an optional custom persistence backend for per-user token state.
	// Use it to plug in Redis, Postgres, or anything else.
	//
	// pace closes it. If the value implements [io.Closer], the Limiter's Close
	// and Shutdown call Close on it as part of teardown — so do not share
	// one Store between two Limiters, or between pace and your own code, unless
	// you are content for the first shutdown to close it for everybody.
	// Implement Close as a no-op if pace should not own the lifetime.
	//
	// There is no built-in backend to fall back on: without a Store, a
	// Limiter is in-memory and a restart starts every user at a full burst.
	Store store.Store

	// StoreTimeout bounds each [github.com/jaeminst/pace/store.Store] operation. Zero defaults to 5s.
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
	//	cfg.QuotaFor = func(userID string) Quota { return tiers[tierOf(userID)] }
	//
	// To change a tier at run time, update whatever QuotaFor reads and then
	// call the Limiter's ReloadQuotas, or a Client's Evict for a single user.
	QuotaFor func(userID string) Quota

	// Shared makes rate limiting apply across replicas rather than once per
	// process, by delegating the decision to a backend every replica consults.
	// The zero shared.Config limits per process, which is the default.
	Shared shared.Config

	// Shards is the number of lock-striped buckets the per-user map is split
	// across. Zero defaults to 256; other values are rounded up to a power of
	// two. Lower it when running many Limiters, one per upstream endpoint.
	Shards int

	// Observer receives notifications about requests, throttling and
	// evictions. Nil disables all of them.
	//
	// Use it to feed metrics or tracing. For a periodic gauge, the Limiter's Stats
	// is cheaper — it needs no hook at all.
	Observer *observe.Observer
}

// Resolve checks cfg and fills in every optional field, returning the
// configuration the rest of pace is built from.
//
// It is the single exported entry to both halves because they are never useful
// apart: defaulting an invalid Config would hide the error, and validating one
// without defaulting leaves zero values downstream must re-check. A failure is
// a [*Error] naming the field.
//
// [github.com/jaeminst/pace/client.New] calls it, so a caller normally never
// does. Call it directly to check a configuration before building anything —
// on startup, say, against a file you have just parsed.
func (cfg Config) Resolve() (Config, error) {
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg.withDefaults(), nil
}

// validate reports the first invalid field in cfg.
func (cfg *Config) validate() error {
	if cfg.BaseURL == "" {
		return &Error{Field: "BaseURL", Err: errors.New("required")}
	}
	if err := urlx.Validate(cfg.BaseURL); err != nil {
		return &Error{Field: "BaseURL", Value: cfg.BaseURL, Err: err}
	}
	if cfg.Rate <= 0 || math.IsNaN(float64(cfg.Rate)) {
		// NaN needs saying separately: it is not <= 0, so the check above lets
		// it through, and the bucket built from it holds NaN tokens and refuses
		// every request for the life of the process. Found by fuzzing.
		return &Error{Field: "Rate", Value: cfg.Rate, Err: errors.New("must be greater than zero")}
	}
	if cfg.Shards > maxShards {
		return &Error{
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
	// Finite rather than a re-export: it maps a true infinity onto
	// the value the bucket can work with, which is plumbing rather than
	// configuration vocabulary, so the root does not lend it a name.
	cfg.Rate = Finite(cfg.Rate)
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

// Spec resolves cfg into the vtable the rate limiter takes.
//
// Call it on a resolved Config — [Config.Resolve] is what makes Rate finite,
// Burst positive and every optional field non-zero, and this folds those in
// without re-checking them. [github.com/jaeminst/pace/client.New] does both in
// order; this is exported for a caller assembling the pieces themselves.
//
// Four Config fields are deliberately absent from the result: BaseURL,
// Transport, RequestTimeout and MaxResponseBytes. The engine makes no requests,
// so they stop at client.New. That the translation is this short is the whole
// of the difference between what a caller writes and what the engine needs — a
// [Clock] becomes a function, and a rate, a burst and an override become one
// function answering "what is this user's quota".
func (cfg Config) Spec() Spec {
	return Spec{
		Quota:        cfg.Quota,
		Now:          cfg.Clock.Now,
		Logger:       cfg.Logger,
		Observer:     cfg.Observer,
		Shards:       cfg.Shards,
		IdleExpiry:   cfg.IdleExpiry,
		GCInterval:   cfg.GCInterval,
		Store:        cfg.Store,
		StoreTimeout: cfg.StoreTimeout,
		Shared:       cfg.Shared,
	}
}

// Quota resolves the quota in force for userID, filling in the Config-wide
// defaults for anything [Config.QuotaFor] left unset.
//
// It is a method rather than a closure over three fields because that is what
// the engine takes: one function, answering the question, with the defaulting
// already done. It runs caller-supplied code, so the engine calls it outside
// any shard lock.
//
// The name is Quota rather than Quota because [Config.QuotaFor] is a field on
// the same struct, and a method whose name differed from it by two letters
// would read as a typo at every call site.
//
// Call it on a resolved Config — [Config.Resolve] is what makes Rate finite and
// Burst positive, and this folds those in without re-checking them.
func (cfg Config) Quota(userID string) Quota {
	q := Quota{Rate: cfg.Rate, Burst: cfg.Burst}
	if cfg.QuotaFor == nil {
		return q
	}
	got := cfg.QuotaFor(userID)
	if got.Rate > 0 {
		q.Rate = got.Rate
	}
	if got.Burst > 0 {
		q.Burst = got.Burst
	}
	q.Rate = Finite(q.Rate)
	return q
}
