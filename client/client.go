package client

import (
	"context"

	"github.com/jaeminst/pace/config"
	"github.com/jaeminst/pace/limiter"
)

// Client is a rate-limited HTTP caller bound to one user identity. Obtain one
// from [Pool.Client]. It is a lightweight handle: every Client derived from the
// same Pool shares that Pool's buckets and store.
//
// A Client owns no resources and has no lifecycle of its own. Shutting the
// service down is [Pool.Close] or [Pool.Shutdown], which act on the whole Pool
// rather than on one user's view of it.
type Client struct {
	userID string
	pool   *Pool
}

// UserID returns the identity this Client is bound to.
func (c *Client) UserID() string { return c.userID }

// Request returns a chainable [*Request]. It never blocks and never fails:
// no rate-limit token is consumed until a terminal method (Get, Post, …) runs,
// so a Request that is built and then abandoned costs the user nothing.
func (c *Client) Request() *Request {
	return newRequest(c.pool, c.userID)
}

// Allow reports whether one request may proceed right now, consuming a token
// if so. Use it to shed load rather than queue behind it.
//
// It never waits for a token — that is [Client.Wait] — but it is not free
// either, which is why it takes a context. A user's first request may load
// their saved state, bounded by config.Config.StoreTimeout, and with a shared
// quota configured every request the local bucket admits costs a backend call
// bounded by shared.Config.Timeout. Both are cancellable through ctx.
//
// It is the method an inbound handler calls with a request context already in
// hand, so it takes one for the same reason every other entry point that does
// I/O does.
func (c *Client) Allow(ctx context.Context) bool {
	return c.pool.lim.Allow(ctx, c.userID)
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
	return c.pool.lim.Wait(ctx, c.userID)
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

// Tokens returns the tokens currently available to this user, and whether the
// user has in-memory state at all. A user who has not been seen, or who has
// been garbage-collected, reports (0, false).
//
// The comma-ok form replaces a -1 sentinel, which could not be told apart from
// a legitimately negative token count and required every caller to know the
// convention.
func (c *Client) Tokens() (float64, bool) {
	return c.pool.lim.Tokens(c.userID)
}

// Evict removes this user from memory, first persisting their token state if a
// store is configured. It reports whether the user had in-memory state, and any
// error from that persistence.
//
// It takes a context because it performs store I/O; the error used to be
// swallowed into a log line, which is the wrong choice for an operation the
// caller invoked deliberately.
func (c *Client) Evict(ctx context.Context) (bool, error) {
	return c.pool.lim.Evict(ctx, c.userID)
}

// Quota returns the rate and burst in force for this user.
//
// While the user holds in-memory state this is what their bucket is actually
// enforcing, which can differ from what [github.com/jaeminst/pace/config.Config.QuotaFor] would return now —
// see [Pool.ReloadQuotas]. Otherwise it is what they would be given on their
// next request. Unlike [Client.Tokens] it always has an answer, because a quota
// is configuration rather than state.
func (c *Client) Quota() config.Quota {
	return c.pool.lim.Quota(c.userID)
}

// Reserve claims a token for a request the caller intends to make, reporting
// how long it must wait rather than blocking for it.
//
// Use it when the wait itself is information: to answer a caller with a
// Retry-After instead of holding the connection, or to decide between two
// backends by which one is free sooner.
// [github.com/jaeminst/pace/limiter.Reservation.Cancel] hands the token back if
// the request is not made after all, while the reported wait has not yet
// elapsed. A reservation that needed no wait is already at its deadline, so
// cancelling that one may refund nothing — which costs one token, and only in
// the case where you were not being throttled anyway.
func (c *Client) Reserve(ctx context.Context) *limiter.Reservation {
	return c.pool.lim.Reserve(ctx, c.userID)
}
