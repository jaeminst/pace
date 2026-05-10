// export_test.go exposes unexported Manager internals for white-box testing.
package pace

// CollectIdle exposes the internal GC sweep so tests can trigger eviction
// without waiting for the GC ticker.
var CollectIdle = (*Manager).collectIdle

// WaitGCLoop blocks until the gcLoop goroutine has exited.
// Call after Close() to guarantee the ctx.Done branch is covered.
func WaitGCLoop(m *Manager) { m.gcWg.Wait() }

// SetGetOrCreateHook installs fn as the hook called in getOrCreateUser's cold
// path after the read-lock is released and before the write-lock is acquired.
// Pass nil to clear the hook.
func SetGetOrCreateHook(m *Manager, fn func()) { m._testHookGetOrCreate = fn }

// CloseManagerStore closes the underlying store without going through
// Manager.Close, allowing tests to put the store into an error state.
func CloseManagerStore(m *Manager) {
	if m.store != nil {
		_ = m.store.Close()
	}
}

// SetManagerStore replaces m's persistence backend with a custom StateStore.
func SetManagerStore(m *Manager, s StateStore) { m.store = &stateStoreWrapper{s: s} }

// WaitReplay blocks until all goroutines spawned by replayPending have exited.
// Call after New() to ensure replay has completed before making assertions.
func WaitReplay(m *Manager) { m.replayWg.Wait() }

// SetOnceEnqueueHook installs fn as the hook called in Once() before EnqueueJob.
// Pass nil to clear the hook.
func SetOnceEnqueueHook(m *Manager, fn func()) { m._testHookOnceBeforeEnqueue = fn }

// EnqueueJob plants a pending job directly into m's SQLite queue without
// executing it. Used by tests to simulate a job left over from a previous run.
func EnqueueJob(m *Manager, id, userID, endpoint string, spec RequestSpec) error {
	if m.sqliteStore == nil {
		return ErrNoPersistence
	}
	method := spec.Method
	if method == "" {
		method = "GET"
	}
	return m.sqliteStore.EnqueueJob(id, userID, endpoint, method, spec.Path, spec.Headers, spec.Body)
}
