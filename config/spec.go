package config

import (
	"log/slog"
	"time"

	"github.com/jaeminst/pace/observe"
	"github.com/jaeminst/pace/shared"
	"github.com/jaeminst/pace/store"
)

// Spec is everything the rate limiter needs from its owner.
//
// It is a vtable rather than a set of options, in the manner of
// [github.com/jaeminst/pace/registry.Spec]: every field is required, nothing is
// defaulted, and [Spec.Validate] panics on one it cannot work with.
//
// It is called Spec rather than Config on purpose, and both live here. [Config]
// is what a caller writes — optional fields, validation, defaults — and this is
// what [Config.Spec] resolves one *into*. Two types named Config, ten of whose
// fields share a name, is a question every reader would have to ask once.
//
// Where the two differ, this one takes the answer rather than the type:
// [Spec.Now] rather than a [Clock], [Spec.Quota] rather than a rate, a burst
// and an override function. Each saves the engine a decision that has already
// been made.
//
// What is absent is as deliberate: there is no base URL, HTTP client, request
// timeout or response-size cap here, because the engine makes no requests.
// Those four are [Config] fields that stop at
// [github.com/jaeminst/pace/client.New].
type Spec struct {
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

// Validate panics on a Spec the engine cannot work with, naming the field.
//
// A zero field here is a nil call or a division on the first request rather than
// a default, and whoever builds a Spec has either come through [Config.Spec] —
// which cannot produce a bad one — or assembled it by hand. So anything wrong at
// this point is a wiring bug, which is what a panic is for.
//
// It is exported because [github.com/jaeminst/pace/limiter.New] calls it, and it
// panics under this package's name because these are this package's invariants:
// [Spec] is declared here, so a reader chasing the message lands where the
// fields are documented.
func (spec Spec) Validate() {
	switch {
	case spec.Quota == nil || spec.Now == nil || spec.Logger == nil:
		panic("config: Spec.Quota, Spec.Now and Spec.Logger are required")
	case spec.Shards <= 0 || spec.Shards&(spec.Shards-1) != 0:
		panic("config: Spec.Shards must be a positive power of two")
	case spec.IdleExpiry <= 0 || spec.GCInterval <= 0 || spec.StoreTimeout <= 0:
		panic("config: Spec.IdleExpiry, Spec.GCInterval and Spec.StoreTimeout must be positive")
	}
}
