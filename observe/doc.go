// Package observe is what a Limiter reports about itself.
//
// [Observer] is a set of hooks fired as work happens — a request throttled, a
// round-trip finished, a key evicted. [Stats] is the counter snapshot behind
// them, for a metrics scrape rather than an event stream. Supply an Observer as
// [github.com/jaeminst/pace/config.Config.Observer].
//
// [Observer] carries the two things worth knowing before you write one: why it
// is a struct rather than an interface, and what a hook may cost.
package observe
