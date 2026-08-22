package limiter

import "github.com/jaeminst/pace/config"

// validate panics on a Config the engine cannot work with, naming the field.
//
// This is the engine stating its own requirements, which is why the check lives
// here rather than in config: a zero field is a nil call or a division on the
// first request rather than a default, and whoever hands one over has either
// come through [github.com/jaeminst/pace/config.Config.Resolve] — which cannot
// produce a bad one — or filled the struct in by hand. Anything wrong at this
// point is a wiring bug, which is what a panic is for.
//
// The fields that describe HTTP are none of this package's business. Nor is the
// quota a QuotaFor returns: that arrives one user at a time, long after this
// runs, and quotaFor normalises it there. What can be checked here is that the
// hook exists at all, because a nil one is a nil call on someone's first
// request rather than a default.
func validate(cfg config.Config) {
	switch {
	case cfg.QuotaFor == nil:
		panic("limiter: Config.QuotaFor is required")
	case cfg.Clock == nil || cfg.Logger == nil:
		panic("limiter: Config.Clock and Config.Logger are required")
	case cfg.Shards <= 0 || cfg.Shards&(cfg.Shards-1) != 0:
		panic("limiter: Config.Shards must be a positive power of two")
	case cfg.IdleExpiry <= 0 || cfg.GCInterval <= 0 || cfg.StoreTimeout <= 0:
		panic("limiter: Config.IdleExpiry, Config.GCInterval and Config.StoreTimeout must be positive")
	}
}
