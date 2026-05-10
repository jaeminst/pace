// export_test.go exposes unexported Client/engine internals for white-box testing.
package pace

import "github.com/jaeminst/pace/internal/store"

// CollectIdle exposes the internal GC sweep so tests can trigger eviction
// without waiting for the GC ticker.
var CollectIdle = func(c *Client) { c.eng.sweep() }

// WaitGCLoop blocks until the gcLoop goroutine has exited.
func WaitGCLoop(c *Client) { c.eng.gcWg.Wait() }

// SetGetOrCreateHook installs fn as the hook called in userFor's cold path.
// Pass nil to clear the hook.
func SetGetOrCreateHook(c *Client, fn func()) { c.eng._testHookGetOrCreate = fn }

// CloseClientStore closes the underlying store without going through Client.Close.
func CloseClientStore(c *Client) {
	if c.eng.store != nil {
		_ = c.eng.store.Close()
	}
}

// SetClientStore replaces c's persistence backend with a custom StateStore.
func SetClientStore(c *Client, s StateStore) { c.eng.store = &storeWrapper{s: s} }

// WaitReplay blocks until all goroutines spawned by replay have exited.
func WaitReplay(c *Client) { c.eng.replayWg.Wait() }

// SetDurableEnqueueHook installs fn as the hook called in Durable before Enqueue.
// Pass nil to clear the hook.
func SetDurableEnqueueHook(c *Client, fn func()) { c.eng._testHookDurableBeforeEnqueue = fn }

// Enqueue plants a pending job directly into c's SQLite queue without
// executing it. Used by tests to simulate a job left over from a previous run.
func Enqueue(c *Client, id, userID, method, path string) error {
	if method == "" {
		method = "GET"
	}
	return c.eng.sqliteStore.Enqueue(store.Job{
		ID:     id,
		UserID: userID,
		Method: method,
		Path:   path,
	})
}
