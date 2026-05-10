// export_test.go exposes unexported Client internals for white-box testing.
package pace

import "github.com/jaeminst/pace/internal/store"

// CollectIdle exposes the internal GC sweep so tests can trigger eviction
// without waiting for the GC ticker.
var CollectIdle = (*Client).sweep

// WaitGCLoop blocks until the gcLoop goroutine has exited.
// Call after Close() to guarantee the ctx.Done branch is covered.
func WaitGCLoop(c *Client) { c.gcWg.Wait() }

// SetGetOrCreateHook installs fn as the hook called in userFor's cold path
// after the read-lock is released and before the write-lock is acquired.
// Pass nil to clear the hook.
func SetGetOrCreateHook(c *Client, fn func()) { c._testHookGetOrCreate = fn }

// CloseClientStore closes the underlying store without going through
// Client.Close, allowing tests to put the store into an error state.
func CloseClientStore(c *Client) {
	if c.store != nil {
		_ = c.store.Close()
	}
}

// SetClientStore replaces c's persistence backend with a custom StateStore.
func SetClientStore(c *Client, s StateStore) { c.store = &storeWrapper{s: s} }

// WaitReplay blocks until all goroutines spawned by replay have exited.
// Call after New() to ensure replay has completed before making assertions.
func WaitReplay(c *Client) { c.replayWg.Wait() }

// SetDurableEnqueueHook installs fn as the hook called in Durable before Enqueue.
// Pass nil to clear the hook.
func SetDurableEnqueueHook(c *Client, fn func()) { c._testHookDurableBeforeEnqueue = fn }

// Enqueue plants a pending job directly into c's SQLite queue without
// executing it. Used by tests to simulate a job left over from a previous run.
func Enqueue(c *Client, id, userID, method, path string) error {
	if method == "" {
		method = "GET"
	}
	return c.sqliteStore.Enqueue(store.Job{
		ID:     id,
		UserID: userID,
		Method: method,
		Path:   path,
	})
}
