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

// CloseManagerStore closes the underlying SQLite store without going through
// Manager.Close, allowing tests to put the store into an error state.
func CloseManagerStore(m *Manager) {
	if m.store != nil {
		_ = m.store.Close()
	}
}

// SetManagerStore replaces m's persistence backend. The replacement must
// satisfy the same Save/Load/Close interface as *store.Store.
func SetManagerStore(m *Manager, s storer) { m.store = s }
