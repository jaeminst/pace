package client

import (
	"context"
	"net/http"
	"time"

	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/observe"
)

// Pool is a rate-limited HTTP client for one upstream. Create one with [New],
// derive a per-user handle with [Pool.Client], and release its resources with
// [Pool.Close] or [Pool.Shutdown].
//
// It is two halves. The rate limiter is
// [github.com/jaeminst/pace/limiter.Limiter], which knows nothing about HTTP —
// buckets, the user population, the shared-quota decision, the lifecycle. The
// rest of this struct is what turns a request into a round-trip. Reach the
// first with [Pool.Limiter]; everything below is the second.
//
// A Pool is safe for concurrent use by multiple goroutines.
type Pool struct {
	lim *limiter.Limiter

	// The HTTP half, resolved from a config.Config by New. It is spelled out in
	// fields rather than kept as a Config so that nothing on the request path
	// can read a value the front door was supposed to have defaulted.
	baseURL          string
	httpClient       *http.Client
	requestTimeout   time.Duration
	maxResponseBytes int64
	now              func() time.Time
}

// New creates a Pool from cfg. It resolves the configuration, assembles the
// engine, and starts a background GC goroutine. Call [Pool.Close] or
// [Pool.Shutdown] when the Pool is no longer needed.
//
// This is where the library is put together, and the split in the literal below
// is the whole architecture: what the engine needs, and what only the HTTP half
// needs. [github.com/jaeminst/pace/config.Config] is what a caller writes;
// [github.com/jaeminst/pace/limiter.Spec] is what the engine needs, and the
// translation is the difference — a transport becomes a client, a clock becomes
// a function, and a rate, a burst and an override become one function answering
// "what is this user's quota". The four HTTP fields never reach the engine at
// all, because it makes no requests.
//
// What is *not* assembled here is anything whose configuration is a callback
// into the engine. The registry and the shared-quota gate both need methods on
// the Limiter that does not exist yet, so they are built inside limiter.New.
// That line — a piece is assembled here if it can be built before the Limiter
// exists — is what decides where each one lives.
func New(cfg config.Config) (*Pool, error) {
	cfg, err := cfg.Resolve()
	if err != nil {
		return nil, err
	}

	return &Pool{
		lim: limiter.New(limiter.Spec{
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
		}),
		baseURL:          cfg.BaseURL,
		httpClient:       &http.Client{Transport: cfg.Transport},
		requestTimeout:   cfg.RequestTimeout,
		maxResponseBytes: cfg.MaxResponseBytes,
		now:              cfg.Clock.Now,
	}, nil
}

// Client returns a handle bound to userID. It is lightweight and safe for
// concurrent use; every Client derived from one Pool shares that Pool's
// rate-limiter state and store.
func (p *Pool) Client(userID string) *Client {
	return &Client{userID: userID, pool: p}
}

// Limiter returns the rate limiter underneath, for pacing work this package
// does not perform on your behalf. It is keyed by user ID rather than bound to
// one: see [github.com/jaeminst/pace/limiter.Limiter].
//
// It is the same engine every [Client] from this Pool consults, so a token
// taken through it is a token an HTTP request will not get. Closing the Pool
// closes it; it has no lifecycle of its own.
func (p *Pool) Limiter() *limiter.Limiter { return p.lim }

// Close stops the background GC goroutine and flushes all in-memory user state
// to the configured store. Close is idempotent; it reports the store's close
// error, if any.
//
// It cancels in-flight requests rather than waiting for them. Use
// [Pool.Shutdown] to let them finish first.
func (p *Pool) Close() error { return p.lim.Close() }

// Shutdown stops the Pool gracefully. It prevents new requests and waits until
// ctx expires (or all in-flight requests finish) before cleaning up. If ctx
// expires first, remaining waiters are force-cancelled and Shutdown returns
// ctx.Err(). The store is always flushed and closed on return.
func (p *Pool) Shutdown(ctx context.Context) error { return p.lim.Shutdown(ctx) }

// Stats reports what the Pool has done since it was created.
func (p *Pool) Stats() observe.Stats { return p.lim.Stats() }

// ReloadQuotas re-reads config.Config.QuotaFor for every user currently holding
// in-memory state and applies the result to their live bucket, keeping the
// tokens they have already accrued. Call it when whatever that function reads
// has changed.
//
// Users not in memory need nothing: their bucket is built from QuotaFor the
// next time they appear. It walks every shard, so it is a maintenance
// operation rather than something to call per request.
func (p *Pool) ReloadQuotas() { p.lim.ReloadQuotas() }
