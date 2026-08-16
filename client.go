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
// if so. Use it to shed load rather than queue behind it.
//
// It does not wait for a token. It can still do bounded I/O: a user's first
// request may load their saved state, bounded by [Config.StoreTimeout], and
// with [SharedConfig.Quota] configured a request the local bucket admits costs
// one backend call bounded by [SharedConfig.Timeout]. Neither is a wait for
// quota, but neither is free either, and Allow takes no context to cancel them
// with — a wart it shares with [Client.Reserve].
func (c *Client) Allow() bool {
	return c.lim.allow(c.userID)
}

// Wait blocks until a token is available for this user, ctx is done, or the
// Limiter is closed. It consumes the token, so every successful Wait must be
// matched by a request the caller actually intends to make.
//
// That is inherent rather than an oversight, and not something to "fix": a Wait
// cut short by ctx already gives its token back, and once Wait has returned
// successfully there is no signal that would tell pace the caller changed their
// mind. [Client.Reserve] is the answer when you want to see the wait before
// committing to it, and to be able to hand the token back.
//
// Prefer the request methods, which acquire a token themselves; Wait is for
// pacing work that pace does not perform on your behalf.
func (c *Client) Wait(ctx context.Context) error {
	l := c.lim
	if !l.enter() {
		return ErrClosed
	}
	defer l.leave()

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
// durable queue under id. A job already completed returns its cached response
// without contacting the server, and a job interrupted by a restart is handled
// according to what is actually known about it.
//
// Delivery is at-least-once, not exactly-once: once a request is dispatched, a
// crash before the response is recorded leaves no way to tell whether the
// server acted. pace records the intent to send before dispatching, so that
// window is detectable rather than silent, and [QueueConfig.AmbiguousPolicy] decides
// what happens to a job caught in it. Every durable request carries
// [QueueConfig.IdempotencyHeader] set to id, so a server that honours it can collapse
// a retry into the original delivery — against such a server, delivery is
// effectively exactly-once.
//
// Two setup failures are deferred to the terminal method, where the caller is
// already checking an error: [ErrNoQueue] when [Config.DBPath] is not set, and
// [ErrInvalidID] when id is empty. Neither depends on the request being built,
// and both are constant for the life of the process, so neither is worth a
// second return value on a builder that is otherwise documented — twice — as
// unable to fail.
func (c *Client) Durable(id string) *Request {
	r := newRequest(c.lim, c.userID)
	switch {
	case c.lim.sqliteStore == nil:
		r.setErr(ErrNoQueue)
	case id == "":
		r.setErr(ErrInvalidID)
	default:
		r.durable, r.durableID = true, id
	}
	return r
}

// Tokens returns the tokens currently available to this user, and whether the
// user has in-memory state at all. A user who has not been seen, or who has
// been garbage-collected, reports (0, false).
//
// The comma-ok form replaces a -1 sentinel, which could not be told apart from
// a legitimately negative token count and required every caller to know the
// convention.
func (c *Client) Tokens() (float64, bool) {
	return c.lim.tokens(c.userID)
}

// Evict removes this user from memory, first persisting their token state if a
// store is configured. It reports whether the user had in-memory state, and any
// error from that persistence.
//
// It takes a context because it performs store I/O; the error used to be
// swallowed into a log line, which is the wrong choice for an operation the
// caller invoked deliberately.
func (c *Client) Evict(ctx context.Context) (bool, error) {
	return c.lim.evictUser(ctx, c.userID)
}
