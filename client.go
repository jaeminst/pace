package pace

import "context"

// Client is a rate-limited HTTP caller bound to one user identity. Obtain one
// from [Limiter.Client]. It is a lightweight handle: every Client derived from
// the same Limiter shares that Limiter's buckets, store, and durable queue.
//
// A Client owns no resources and has no lifecycle of its own. Shutting the
// service down is [Limiter.Close] or [Limiter.Shutdown], which act on the whole
// Limiter rather than on one user's view of it.
type Client struct {
	userID string
	lim    *Limiter
}

// UserID returns the identity this Client is bound to.
func (c *Client) UserID() string { return c.userID }

// Request acquires a rate-limit token and returns a chainable [*Request]
// ready to execute. It blocks until a token is available, the caller's
// context expires, or the Limiter is closed/shut down.
func (c *Client) Request(ctx context.Context) (*Request, error) {
	return c.lim.request(ctx, c.userID)
}

// Get acquires a token and executes an HTTP GET to path.
func (c *Client) Get(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, path, (*Request).Get)
}

// Post acquires a token and executes an HTTP POST to path.
func (c *Client) Post(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, path, (*Request).Post)
}

// Put acquires a token and executes an HTTP PUT to path.
func (c *Client) Put(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, path, (*Request).Put)
}

// Delete acquires a token and executes an HTTP DELETE to path.
func (c *Client) Delete(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, path, (*Request).Delete)
}

// Patch acquires a token and executes an HTTP PATCH to path.
func (c *Client) Patch(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, path, (*Request).Patch)
}

// do is the shared body of the convenience verbs: acquire a token, then run
// the verb against the resulting builder.
func (c *Client) do(
	ctx context.Context,
	path string,
	verb func(*Request, string) (*Response, error),
) (*Response, error) {
	req, err := c.Request(ctx)
	if err != nil {
		return nil, err
	}
	return verb(req, path)
}

// Durable returns a chainable [*Request] whose execution is recorded in the
// durable queue under id, so a job interrupted by a restart is replayed and a
// job already completed returns its cached response.
//
// It reports [ErrNoQueue] when [Config.DBPath] is not set.
func (c *Client) Durable(ctx context.Context, id string) *Request {
	if c.lim.sqliteStore == nil {
		return &Request{durableErr: ErrNoQueue}
	}
	return newDurableRequest(ctx, c.lim, c.userID, id)
}

// Tokens returns the approximate number of available rate-limit tokens.
// Returns -1 if the user has no in-memory state (not yet seen, or GC'd).
func (c *Client) Tokens() float64 {
	return c.lim.tokens(c.userID)
}

// Evict removes this user from the in-memory shard immediately, saving token
// state to the store if one is configured. Returns false if the user had no
// in-memory state.
func (c *Client) Evict() bool {
	return c.lim.evictUser(c.userID)
}
