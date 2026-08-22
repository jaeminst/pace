package pace

import (
	"net/http"

	"github.com/jaeminst/pace/limiter"
)

// New creates a Limiter from cfg. It validates and defaults the configuration,
// assembles the engine, and starts a background GC goroutine. Call
// [github.com/jaeminst/pace/limiter.Limiter.Close] or Shutdown when the Limiter
// is no longer needed.
//
// The Limiter it returns is the engine's own type, not a wrapper — bind a user
// identity with its Client method and everything after this line is named in
// that package. This one holds [Config], and nothing else.
//
// This is where the library is put together. [Config] is what a caller writes;
// [github.com/jaeminst/pace/limiter.Spec] is what the engine needs, and the
// translation below is the whole of the difference — a transport becomes a
// client, a clock becomes a function, and a rate, a burst and an override
// become one function answering "what is this user's quota".
//
// What is *not* assembled here is anything whose configuration is a callback
// into the engine. The registry and the shared-quota gate both need methods on
// the Limiter that does not exist yet, so they are built inside limiter.New.
// That line — a piece is assembled here if it can be built before the Limiter
// exists — is what decides where each one lives.
func New(cfg Config) (*limiter.Limiter, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	cfg = cfg.withDefaults()

	return limiter.New(limiter.Spec{
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
func (cfg Config) quotaFor(userID string) limiter.Quota {
	q := limiter.Quota{Rate: cfg.Rate, Burst: cfg.Burst}
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
	q.Rate = limiter.Finite(q.Rate)
	return q
}
