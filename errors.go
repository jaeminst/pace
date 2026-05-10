package pace

import "errors"

// ErrClosed is returned by Request and convenience methods after the Client
// has been closed.
var ErrClosed = errors.New("pace: client closed")

// ErrNoPersistence is returned by [Client.Durable] when no SQLite store is
// configured. Set [Config.DBPath] to enable durable request queuing.
var ErrNoPersistence = errors.New("pace: Durable requires Config.DBPath")
