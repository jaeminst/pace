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

// Request returns a chainable [*Request]. It never blocks and never fails:
// no rate-limit token is consumed until a terminal method (Get, Post, …) runs,
// so a Request that is built and then abandoned costs the user nothing.
func (c *Client) Request() *Request {
	return newRequest(c.lim, c.userID)
}

// Allow reports whether one request may proceed right now, consuming a token
// if so. It never blocks. Use it to shed load rather than queue behind it.
func (c *Client) Allow() bool {
	return c.lim.allow(c.userID)
}

// Wait blocks until a token is available for this user, ctx is done, or the
// Limiter is closed. It consumes the token, so every successful Wait must be
// matched by a request the caller actually intends to make.
//
// Prefer the request methods, which acquire a token themselves; Wait is for
// pacing work that pace does not perform on your behalf.
func (c *Client) Wait(ctx context.Context) error {
	l := c.lim
	l.shutdownMu.RLock()
	if l.shuttingDown {
		l.shutdownMu.RUnlock()
		return ErrClosed
	}
	l.activeWg.Add(1)
	l.shutdownMu.RUnlock()
	defer l.activeWg.Done()

	waitCtx, release := l.withLifetime(ctx)
	defer release()
	return l.acquire(waitCtx, c.userID)
}

// Get acquires a token and executes an HTTP GET to path.
func (c *Client) Get(ctx context.Context, path string) (*Response, error) {
	return c.Request().Get(ctx, path)
}

// Post acquires a token and executes an HTTP POST to path.
func (c *Client) Post(ctx context.Context, path string) (*Response, error) {
	return c.Request().Post(ctx, path)
}

// Put acquires a token and executes an HTTP PUT to path.
func (c *Client) Put(ctx context.Context, path string) (*Response, error) {
	return c.Request().Put(ctx, path)
}

// Delete acquires a token and executes an HTTP DELETE to path.
func (c *Client) Delete(ctx context.Context, path string) (*Response, error) {
	return c.Request().Delete(ctx, path)
}

// Patch acquires a token and executes an HTTP PATCH to path.
func (c *Client) Patch(ctx context.Context, path string) (*Response, error) {
	return c.Request().Patch(ctx, path)
}

// Durable returns a chainable [*Request] whose execution is recorded in the
// durable queue under id, so a job interrupted by a restart is replayed and a
// job already completed returns its cached response.
//
// id must not be empty. Durable reports [ErrNoQueue] when [Config.DBPath] is
// not set, and [ErrInvalidID] when id is empty.
func (c *Client) Durable(id string) (*Request, error) {
	if c.lim.sqliteStore == nil {
		return nil, ErrNoQueue
	}
	if id == "" {
		return nil, ErrInvalidID
	}
	r := newRequest(c.lim, c.userID)
	r.durable = true
	r.durableID = id
	return r, nil
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
