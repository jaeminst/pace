// export_test.go exposes unexported Limiter internals for white-box testing.
package pace

import (
	"context"
	"errors"

	"github.com/jaeminst/pace/internal/store"
)

// CollectIdle exposes the internal GC sweep so tests can trigger eviction
// without waiting for the GC ticker.
var CollectIdle = func(l *Limiter) { l.sweep() }

// WaitGCLoop blocks until the gcLoop goroutine has exited.
func WaitGCLoop(l *Limiter) { l.gcWg.Wait() }

// SetGetOrCreateHook installs fn as the hook called in userFor's cold path.
// Pass nil to clear the hook.
func SetGetOrCreateHook(l *Limiter, fn func()) { l._testHookGetOrCreate = fn }

// CloseLimiterStore closes the underlying store without going through Close.
func CloseLimiterStore(l *Limiter) {
	if l.store != nil {
		_ = l.store.Close()
	}
}

// SetLimiterStore replaces l's persistence backend with a custom StateStore.
func SetLimiterStore(l *Limiter, s StateStore) { l.store = s }

// WaitReplay blocks until all goroutines spawned by replay have exited.
func WaitReplay(l *Limiter) { l.replayWg.Wait() }

// SetDurableEnqueueHook installs fn as the hook called in Durable before Enqueue.
// Pass nil to clear the hook.
func SetDurableEnqueueHook(l *Limiter, fn func()) { l._testHookDurableBeforeEnqueue = fn }

// Enqueue plants a pending job directly into l's SQLite queue without
// executing it. Used by tests to simulate a job left over from a previous run.
func Enqueue(l *Limiter, id, userID, method, path string) error {
	if method == "" {
		method = "GET"
	}
	return l.sqliteStore.Enqueue(context.Background(), store.Job{
		ID:     id,
		UserID: userID,
		Method: method,
		Path:   path,
	})
}

// ClaimJob takes ownership of a durable job on behalf of owner, simulating a
// worker that claimed a job and then died.
func ClaimJob(l *Limiter, id, owner string) error {
	now := l.cfg.Clock.Now()
	ok, err := l.sqliteStore.Claim(context.Background(), id, owner,
		now.UnixNano(), now.Add(l.cfg.JobLease).UnixNano())
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("pace: test: claim was refused")
	}
	return nil
}
