package limiter

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/shared"
	"github.com/jaeminst/pace/store"
)

// Spec is everything the engine needs from its owner.
//
// It is a vtable rather than a set of options, in the manner of
// [github.com/jaeminst/pace/registry.Config]: every field is required, [New]
// panics on one it cannot work with, and nothing here is defaulted.
//
// It is called Spec rather than Config on purpose. The configuration a caller
// writes is github.com/jaeminst/pace.Config — optional fields, validation,
// defaults — and this is what pace.New resolves one *into*. Two types named
// Config, eleven of whose fields share a name, is a question every reader would
// have to ask once.
//
// Where the two differ, this one takes the answer rather than the type:
// [Spec.Now] rather than a clock, [Spec.Quota] rather than a rate, a burst
// and an override function, [Spec.HTTPClient] rather than a transport. Each
// saves this package a decision that has already been made.
type Spec struct {
	// BaseURL is prepended to every request path. Already validated.
	BaseURL string

	// HTTPClient performs every round-trip. Its Transport is the caller's.
	HTTPClient *http.Client

	// Quota resolves the quota in force for a user, defaults folded in and the
	// rate already made finite. It runs caller-supplied code, so every call
	// site must be outside any shard lock.
	Quota func(userID string) Quota

	// Now is the owner's clock, so that every instant pace reports comes from
	// one source.
	Now func() time.Time

	Logger *slog.Logger

	// Observer is the caller's hook set, or nil if they registered none.
	Observer *observe.Observer

	// RequestTimeout bounds one round-trip, excluding the wait for a token.
	// Zero means the caller's context is the only limit.
	RequestTimeout time.Duration

	// MaxResponseBytes caps the buffered response body. Zero is unlimited.
	MaxResponseBytes int64

	// Shards is the lock-striping width, already rounded to a power of two.
	// IdleExpiry and GCInterval drive the sweep. All three must be positive.
	Shards     int
	IdleExpiry time.Duration
	GCInterval time.Duration

	// Store is the caller's persistence backend, or nil for none — the one
	// field whose zero is meaningful. StoreTimeout bounds each call to it and
	// must be positive whether or not a Store is set, because it also bounds
	// the load that happens when a user is first seen.
	Store        store.Store
	StoreTimeout time.Duration

	// Shared configures the cross-replica decision. The zero value keeps
	// limiting per process, which is the default.
	Shared shared.Config
}

// validate panics on a Spec this package cannot work with, naming the field.
//
// A zero field here is a nil call or a division on the first request rather
// than a default, and the owner that builds one has already run its own
// validation — so anything wrong at this point is a wiring bug, which is what
// a panic is for.
func (spec Spec) validate() {
	switch {
	case spec.BaseURL == "":
		panic("limiter: BaseURL is required")
	case spec.HTTPClient == nil:
		panic("limiter: HTTPClient is required")
	case spec.Quota == nil || spec.Now == nil || spec.Logger == nil:
		panic("limiter: Quota, Now and Logger are required")
	case spec.Shards <= 0 || spec.Shards&(spec.Shards-1) != 0:
		panic("limiter: Shards must be a positive power of two")
	case spec.IdleExpiry <= 0 || spec.GCInterval <= 0 || spec.StoreTimeout <= 0:
		panic("limiter: IdleExpiry, GCInterval and StoreTimeout must be positive")
	}
}
