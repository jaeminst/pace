package pace

import "context"

// Client is a rate-limited HTTP caller bound to a specific user identity.
// Create one with [New] (binds to [Config.Name]) or derive one for a different
// user via [Client.For]. Multiple Clients sharing the same [New] origin share
// one underlying rate-limiter.
type Client struct {
	userID string // user this client represents; empty when Name not set in Config
	eng    *engine
}

// For returns a Client bound to userID, sharing the same underlying
// rate-limiter and configuration. It is lightweight (no allocation beyond the
// struct itself) and safe for concurrent use.
func (c *Client) For(userID string) *Client {
	return &Client{userID: userID, eng: c.eng}
}

// Request acquires a rate-limit token and returns a chainable [*Request]
// ready to execute. It blocks until a token is available, the caller's
// context expires, or the Client is closed/shut down.
func (c *Client) Request(ctx context.Context) (*Request, error) {
	return c.eng.request(ctx, c.userID)
}

// Get acquires a token and executes an HTTP GET to path.
func (c *Client) Get(ctx context.Context, path string) (*Response, error) {
	req, err := c.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Get(path)
}

// Post acquires a token and executes an HTTP POST to path.
func (c *Client) Post(ctx context.Context, path string) (*Response, error) {
	req, err := c.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Post(path)
}

// Put acquires a token and executes an HTTP PUT to path.
func (c *Client) Put(ctx context.Context, path string) (*Response, error) {
	req, err := c.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Put(path)
}

// Delete acquires a token and executes an HTTP DELETE to path.
func (c *Client) Delete(ctx context.Context, path string) (*Response, error) {
	req, err := c.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Delete(path)
}

// Patch acquires a token and executes an HTTP PATCH to path.
func (c *Client) Patch(ctx context.Context, path string) (*Response, error) {
	req, err := c.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Patch(path)
}

// Durable returns a chainable [*Request] with exactly-once semantics
// identified by id. See [New] documentation for details.
func (c *Client) Durable(ctx context.Context, id string) *Request {
	if c.eng.sqliteStore == nil {
		return &Request{durableErr: ErrNoQueue}
	}
	return newDurableRequest(ctx, c.eng, c.userID, id)
}

// Tokens returns the approximate number of available rate-limit tokens.
// Returns -1 if the user has no in-memory state (not yet seen, or GC'd).
func (c *Client) Tokens() float64 {
	return c.eng.tokens(c.userID)
}

// Evict removes this user from the in-memory shard immediately, saving token
// state to the store if one is configured. Returns false if the user had no
// in-memory state.
func (c *Client) Evict() bool {
	return c.eng.evictUser(c.userID)
}

// Close shuts down the background GC goroutine and flushes all in-memory
// user states to the configured store. Close is idempotent.
func (c *Client) Close() { c.eng.close() }

// Shutdown stops the Client gracefully. It prevents new requests and waits
// until ctx expires (or all in-flight requests finish) before cleaning up.
// Shutdown returns ctx.Err() if the deadline is exceeded. The store is always
// flushed and closed on return. Shutdown is idempotent.
func (c *Client) Shutdown(ctx context.Context) error { return c.eng.shutdown(ctx) }
