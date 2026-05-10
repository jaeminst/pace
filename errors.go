package pace

import "errors"

// ErrClosed is returned by Request and Get after the Manager has been closed.
var ErrClosed = errors.New("pace: manager closed")

// ErrUnknownEndpoint is returned when the endpoint name is not present in
// Config.Endpoints.
var ErrUnknownEndpoint = errors.New("pace: unknown endpoint")

// ErrNoPersistence is returned by [Manager.Once] when no SQLite store is
// configured. Set [Config.DBPath] to enable durable request queuing.
var ErrNoPersistence = errors.New("pace: Once requires Config.DBPath")
