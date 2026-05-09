// export_test.go exposes unexported Manager methods for white-box testing.
package pace

// CollectIdle exposes the internal GC sweep so tests can trigger eviction
// without waiting for the GC ticker.
var CollectIdle = (*Manager).collectIdle
