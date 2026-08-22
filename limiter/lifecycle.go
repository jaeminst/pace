// lifecycle.go is everything about a Limiter's own lifetime: the shutdown
// barrier every entry point passes through, the two ways to close one, and the
// GC goroutine that runs until it does.
//
// It is separate from limiter.go because that file is about assembly — New and
// the three constructors it calls — and this is about the thing after it is
// assembled. They share only the struct.

package limiter

import (
	"context"
	"fmt"
	"io"
	"time"
)

// Close stops the background GC goroutine and flushes all in-memory key state
// to the configured store. Close is idempotent; it reports the store's close
// error, if any.
//
// It cancels in-flight requests rather than waiting for them: every request
// runs under a context derived from the Limiter's own. Use [Limiter.Shutdown]
// to let them finish first.
func (l *Limiter) Close() error { return l.close() }

// Shutdown stops the Limiter gracefully. It prevents new requests and waits
// until ctx expires (or all in-flight requests finish) before cleaning up.
// If ctx expires first, remaining waiters are force-cancelled and Shutdown
// returns ctx.Err(). The store is always flushed and closed on return.
// Shutdown is idempotent via the underlying Close call.
func (l *Limiter) Shutdown(ctx context.Context) error {
	// Stop accepting new requests.
	l.shutdownMu.Lock()
	l.shuttingDown = true
	l.shutdownMu.Unlock()
	l.fireShuttingDown()

	// Wait for active requests to finish, honouring the caller's deadline.
	waitDone := make(chan struct{})
	go func() {
		l.activeWg.Wait()
		close(waitDone)
	}()

	var shutdownErr error
	select {
	case <-waitDone:
	case <-ctx.Done():
		shutdownErr = ctx.Err()
		l.cancel() // force-cancel remaining waiters
		<-waitDone
	}

	// finish deliberately does not receive ctx. Shutdown documents that the
	// store is always flushed on return, and by this point ctx may already be
	// expired — that is the branch above. Inheriting it would discard the flush
	// at exactly the moment it matters. StoreTimeout bounds it instead; see
	// TestFinalFlushSurvivesLimiterCancellation.
	closeErr := l.finish() //nolint:contextcheck // the final flush must outlive the caller's shutdown deadline
	if shutdownErr != nil {
		return shutdownErr
	}
	return closeErr
}

// withLifetime derives a context cancelled when either ctx or the Limiter's
// lifetime ends, so shutting the Limiter down aborts work already in progress.
//
// context.AfterFunc rather than a goroutine: nothing is spawned in the common
// case where the Limiter outlives the request. Deriving this once per request
// and reusing it — rather than re-merging inside every operation — keeps the
// hot path to a single context allocation.
func (l *Limiter) withLifetime(ctx context.Context) (context.Context, func()) {
	merged, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(l.ctx, cancel)
	return merged, func() {
		stop()
		cancel()
	}
}

// enter registers an operation that must finish before shutdown completes. It
// reports false when the Limiter is already shutting down, in which case
// nothing was registered and [Limiter.leave] must not be called.
//
// The check and the Add share one mutex acquisition on purpose: that is what
// stops a registration slipping in between finish setting shuttingDown and its
// activeWg.Wait. Every path that touches the store after this point goes
// through here, so the invariant lives in one place rather than being restated
// — and forgotten — at each call site.
func (l *Limiter) enter() bool {
	l.shutdownMu.RLock()
	defer l.shutdownMu.RUnlock()
	if l.shuttingDown {
		return false
	}
	l.activeWg.Add(1)
	return true
}

// leave releases a registration taken by [Limiter.enter].
func (l *Limiter) leave() { l.activeWg.Done() }

// close marks the Limiter as shutting down and then tears it down. It is
// idempotent; repeated calls return the error recorded by the first.
func (l *Limiter) close() error {
	l.shutdownMu.Lock()
	l.shuttingDown = true
	l.shutdownMu.Unlock()
	return l.finish()
}

// finish is the single teardown sequence, shared by Close and Shutdown so the
// ordering exists in exactly one place.
//
// The invariant it establishes: once the store is closed, nothing may touch it.
// Store I/O has two producers — the GC sweep and new-user loads — so both must
// be drained first. gcWg in
// particular used to be started and never waited on, leaving a sweep free to
// Save into a store that Close had already shut.
//
// Waiting on activeWg cannot deadlock: shuttingDown is set before finish runs,
// and it is checked under the same mutex that guards activeWg.Add, so no new
// registration can appear after this point.
func (l *Limiter) finish() error {
	// sync.Once establishes happens-before for closeErr, so later callers read
	// what the first writer stored.
	l.closeOnce.Do(func() {
		l.cancel()
		l.gcWg.Wait()
		l.activeWg.Wait()
		// Persist before discarding: dropUsers empties the shards, so a flush
		// after it would find nothing to write.
		if l.state.persists() {
			l.state.flush(l.reg.SnapshotAll())
		}
		// Drop whether or not there is a store: shutdown discards every key's
		// in-memory state either way, and an observer watching the population
		// should see it go rather than have it vanish silently.
		l.reg.DropAll()
		var cerr error
		// Discovered by assertion rather than required by store.Store: a store
		// with nothing to release should not have to write an empty method.
		if c, ok := l.store.(io.Closer); ok && l.store != nil {
			cerr = c.Close()
		}
		if cerr != nil {
			l.cfg.Logger.Warn("pace: close store", "err", cerr)
			l.closeErr = fmt.Errorf("pace: close store: %w", cerr)
		}
	})
	return l.closeErr
}

// gcLoop drives the idle-user sweep.
func (l *Limiter) gcLoop() {
	defer l.gcWg.Done()
	ticker := time.NewTicker(l.cfg.GCInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.reg.Sweep()
		case <-l.ctx.Done():
			return
		}
	}
}
