package config

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"time"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/registry"
	"github.com/jaeminst/pace/shared"
	"github.com/jaeminst/pace/store"
	"github.com/jaeminst/pace/urlx"
)

// Error reports an invalid [Config] field, or an invalid value handed to a
// setter that takes one — [Config.Resolve] and
// [github.com/jaeminst/pace/limiter.Limiter.ReloadQuotas] are the two that
// return it.
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

	// IdleExpiry is how long a key can be inactive before their in-memory
	// state is garbage-collected. Zero defaults to 10 minutes.
	IdleExpiry time.Duration

	// GCInterval controls how often the idle-user GC sweep runs.
	// Zero defaults to 1 minute.
	GCInterval time.Duration

	// Transport is the HTTP transport used for all requests. Nil defaults to
	// [http.DefaultTransport].
	Transport http.RoundTripper

	// CookieJar stores the cookies the upstream sets and puts them back on
	// subsequent requests, including the ones a redirect generates. Nil — the
	// default — sends and stores no cookies at all, which is what pace did
	// before this field existed.
	//
	// It is handed straight to [http.Client.Jar], so the semantics are the
	// standard library's rather than anything pace invents:
	//
	//	jar, err := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	//
	// Pass a nil Options and the jar has no public suffix list, which
	// net/http/cookiejar itself calls insecure: without one, a server for
	// foo.co.uk can set a cookie for bar.co.uk. golang.org/x/net/publicsuffix
	// supplies the list; pace does not depend on it, because a cookie policy is
	// the caller's to choose.
	//
	// **One jar serves every key in the Pool** unless a [WithCookieJarFor]
	// option scopes them. Without one, a
	// [github.com/jaeminst/pace/client.Pool] owns a single http.Client and every
	// Client minted from it shares that one, so a cookie the upstream sets while
	// serving key "alice" goes back out on a request made for key "bob". That is
	// right when the cookie identifies *your* service to the upstream — a
	// session your process holds, which is the usual reason to want a jar. When
	// a key is an end user whose session this is, pass [WithCookieJarFor] to
	// New — the hook is handed this field as its default — or keep no jar and
	// carry the cookie yourself with
	// [github.com/jaeminst/pace/client.Request.SetHeader].
	//
	// **It must be safe for concurrent use.** pace issues requests from every
	// caller's goroutine, so the jar is read and written concurrently.
	// net/http/cookiejar's implementation is; a hand-written one has to be.
	CookieJar http.CookieJar

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
	// the key is.
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

	// Store is an optional custom persistence backend for per-key token state.
	// Use it to plug in Redis, Postgres, or anything else.
	//
	// pace closes it. If the value implements [io.Closer], the Limiter's Close
	// and Shutdown call Close on it as part of teardown — so do not share
	// one Store between two Limiters, or between pace and your own code, unless
	// you are content for the first shutdown to close it for everybody.
	// Implement Close as a no-op if pace should not own the lifetime.
	//
	// There is no built-in backend to fall back on: without a Store, a
	// Limiter is in-memory and a restart starts every key at a full burst.
	Store store.Store

	// StoreTimeout bounds each [github.com/jaeminst/pace/store.Store] operation. Zero defaults to 5s.
	StoreTimeout time.Duration

	// Quota is the rate and burst every key gets. Required; the rate must be
	// greater than zero. Build the rate with [bucket.PerSecond],
	// [bucket.PerMinute], [bucket.PerHour] or [bucket.Every], or use
	// [bucket.Inf] to disable throttling.
	//
	// It is a value, not a function, because this struct is configuration:
	// what a caller writes down, and what [Config.Resolve] can check before
	// anything runs. A burst below one is raised to one and an infinite rate
	// is made finite, both here rather than later.
	//
	// Grading keys into tiers is [WithQuotaFor], an option to
	// [github.com/jaeminst/pace/client.New]. That hook is handed this value as
	// its default, so there is no rule about which of the two wins: this is
	// the quota unless something the caller passed says otherwise, and that
	// something is told what it is overriding.
	Quota bucket.Quota

	// Shared makes rate limiting apply across replicas rather than once per
	// process, by delegating the decision to a backend every replica consults.
	// The zero shared.Config limits per process, which is the default.
	Shared shared.Config

	// Shards is the number of lock-striped buckets the per-key map is split
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
	if cfg.Quota.Rate <= 0 || math.IsNaN(float64(cfg.Quota.Rate)) {
		// NaN needs saying separately: it is not <= 0, so the check above lets
		// it through, and the bucket built from it holds NaN tokens and refuses
		// every request for the life of the process. Found by fuzzing.
		return &Error{Field: "Quota", Value: cfg.Quota, Err: errors.New("rate must be greater than zero")}
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
	// The bucket owns this: a true infinity maps onto the largest rate the
	// arithmetic downstream can hold. Plumbing rather than configuration
	// vocabulary, which is why this package calls it by name instead of
	// wrapping it.
	cfg.Quota.Rate = bucket.Finite(cfg.Quota.Rate)
	if cfg.Quota.Burst <= 0 {
		cfg.Quota.Burst = 1
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

// DefaultConfig returns a Config for baseURL that is ready to pass to
// [github.com/jaeminst/pace/client.New]: 100 requests a minute with a burst of
// 10, and every other field at the default [Config.Resolve] would give it.
//
// It exists because a Config has exactly two required fields and one of them is
// a number somebody has to invent before they can see the library work. This
// invents it. The rate is deliberately modest — low enough that throttling is
// visible in a first test, high enough not to be in the way — and it is a
// starting point rather than a recommendation: the right rate is whatever the
// upstream you are calling allows.
//
// The result is a plain Config, not a resolved one, so it composes:
//
//	cfg := config.DefaultConfig("https://api.example.com")
//	cfg.Quota = bucket.NewQuota("6/m", 2)
//	cfg.Store = myStore
func DefaultConfig(baseURL string) Config {
	return Config{
		BaseURL: baseURL,
		Quota:   bucket.NewQuota("100/m", 10),
	}
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
