package pace

import (
	"net/http"

	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/rate"
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

// LimitError reports that a request was throttled, and carries the limit that
// was in force.
type LimitError = limiter.LimitError

// The sentinel errors a caller matches with [errors.Is].
var (
	ErrClosed       = limiter.ErrClosed
	ErrBodyTooLarge = limiter.ErrBodyTooLarge
)

// New creates a Limiter from cfg. It validates and defaults the configuration,
// assembles the engine, and starts a background GC goroutine. Call
// [Limiter.Close] or [Limiter.Shutdown] when the Limiter is no longer needed.
//
// Bind a user identity with [Limiter.Client].
//
// This is where the library is put together. [Config] is what a caller writes;
// [github.com/jaeminst/pace/limiter.Config] is what the engine needs, and the
// translation below is the whole of the difference — a transport becomes a
// client, a clock becomes a function, and a rate, a burst and an override
// become one function answering "what is this user's quota".
//
// What is *not* assembled here is anything whose configuration is a callback
// into the engine. The registry and the shared-quota gate both need methods on
// the Limiter that does not exist yet, so they are built inside limiter.New.
// That line — a piece is assembled here if it can be built before the Limiter
// exists — is what decides where each one lives.
func New(cfg Config) (*Limiter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	return limiter.New(limiter.Config{
		BaseURL:          cfg.BaseURL,
		HTTPClient:       &http.Client{Transport: cfg.Transport},
		Quota:            cfg.quotaFor,
		Now:              cfg.Clock.Now,
		Logger:           cfg.Logger,
		Observer:         cfg.Observer,
		RequestTimeout:   cfg.RequestTimeout,
		MaxResponseBytes: cfg.MaxResponseBytes,
		Shards:           cfg.Shards,
		IdleExpiry:       cfg.IdleExpiry,
		GCInterval:       cfg.GCInterval,
		Store:            cfg.Store,
		StoreTimeout:     cfg.StoreTimeout,
		Shared:           cfg.Shared,
	}), nil
}

// quotaFor resolves the quota in force for userID, filling in the Config-wide
// defaults for anything [Config.QuotaFor] left unset.
//
// It is a method value rather than a closure over three fields because that is
// what the engine takes: one function, answering the question, with the
// defaulting already done. It runs caller-supplied code, so the engine calls it
// outside any shard lock.
func (cfg Config) quotaFor(userID string) rate.Quota {
	q := rate.Quota{Rate: cfg.Rate, Burst: cfg.Burst}
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
	q.Rate = rate.Finite(q.Rate)
	return q
}
