// Package observe is what a Limiter reports about itself.
//
// [Observer] is a set of hooks fired as work happens — a request throttled, a
// round-trip finished, a user evicted — and
// [Stats] is the counter snapshot behind them, for a metrics scrape rather than
// an event stream. Supply an Observer as
// github.com/jaeminst/pace.Config.Observer.
//
// Observer is a struct of functions rather than an interface on purpose. An
// interface cannot gain a method after v1 without breaking every
// implementation, and the events worth reporting will grow; a struct can gain a
// field. Hooks run on the caller's goroutine, in the request path, so keep them
// cheap or hand the work to a channel of your own.
package observe
