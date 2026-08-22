// export_test.go exposes unexported Limiter internals for white-box testing.
package limiter

import (
	"io"

	"github.com/jaeminst/pace/store"
)

// CollectIdle exposes the internal GC sweep so tests can trigger eviction
// without waiting for the GC ticker.
var CollectIdle = func(l *Limiter) { l.reg.Sweep() }

// WaitGCLoop blocks until the gcLoop goroutine has exited.
func WaitGCLoop(l *Limiter) { l.gcWg.Wait() }

// setHook installs one hook by rewriting the whole set, so that the background
// goroutines reading it see a consistent snapshot.
func setHook(l *Limiter, apply func(*hooks)) {
	next := &hooks{}
	if cur := l.hooks.Load(); cur != nil {
		*next = *cur
	}
	apply(next)
	l.hooks.Store(next)
}

// SetGetOrCreateHook installs fn as the hook called in entryFor's cold path.
// Pass nil to clear the hook.
func SetGetOrCreateHook(l *Limiter, fn func()) { setHook(l, func(h *hooks) { h.getOrCreate = fn }) }

// SetBeforeWaitHook installs fn as the hook called just before a caller blocks
// waiting for a token.
func SetBeforeWaitHook(l *Limiter, fn func()) { setHook(l, func(h *hooks) { h.beforeWait = fn }) }

// SetShuttingDownHook installs fn as the hook called once Shutdown has stopped
// accepting new requests.
func SetShuttingDownHook(l *Limiter, fn func()) { setHook(l, func(h *hooks) { h.shuttingDown = fn }) }

// SetAfterSweepHook installs fn as the hook called at the end of each GC sweep.
func SetAfterSweepHook(l *Limiter, fn func()) { setHook(l, func(h *hooks) { h.afterSweep = fn }) }

// CloseLimiterStore closes the underlying store without going through Close.
func CloseLimiterStore(l *Limiter) {
	if c, ok := l.store.(io.Closer); ok {
		_ = c.Close()
	}
}

// SetLimiterStore replaces l's persistence backend with a custom store.Store.
// The persistence adapter is rebuilt rather than patched, because it holds the
// store by value.
func SetLimiterStore(l *Limiter, s store.Store) {
	l.store = s
	l.state = l.newState()
}
