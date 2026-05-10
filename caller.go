package pace

import "context"

// Caller is a user-scoped view of a [Client]. All methods use the userID
// bound at creation time; no userID parameter is needed per call.
// Obtain one via [Client.For].
type Caller struct {
	c      *Client
	userID string
}

// For returns a Caller bound to userID. The returned Caller is lightweight
// (no allocation other than the struct itself) and safe for concurrent use.
func (c *Client) For(userID string) *Caller {
	return &Caller{c: c, userID: userID}
}

// Request acquires a rate-limit token and returns a chainable [*Request]
// ready to execute. It blocks until a token is available, the caller's
// context expires, or the Client is closed/shut down.
func (caller *Caller) Request(ctx context.Context) (*Request, error) {
	return caller.c.request(ctx, caller.userID)
}

// Get acquires a token and executes an HTTP GET to path.
func (caller *Caller) Get(ctx context.Context, path string) (*Response, error) {
	req, err := caller.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Get(path)
}

// Post acquires a token and executes an HTTP POST to path.
func (caller *Caller) Post(ctx context.Context, path string) (*Response, error) {
	req, err := caller.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Post(path)
}

// Put acquires a token and executes an HTTP PUT to path.
func (caller *Caller) Put(ctx context.Context, path string) (*Response, error) {
	req, err := caller.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Put(path)
}

// Delete acquires a token and executes an HTTP DELETE to path.
func (caller *Caller) Delete(ctx context.Context, path string) (*Response, error) {
	req, err := caller.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Delete(path)
}

// Patch acquires a token and executes an HTTP PATCH to path.
func (caller *Caller) Patch(ctx context.Context, path string) (*Response, error) {
	req, err := caller.Request(ctx)
	if err != nil {
		return nil, err
	}
	return req.Patch(path)
}

// Durable returns a chainable [*Request] that executes with exactly-once
// semantics. See [Client] documentation for details.
func (caller *Caller) Durable(ctx context.Context, id string) *Request {
	if caller.c.sqliteStore == nil {
		return &Request{durableErr: ErrNoPersistence}
	}
	return newDurableRequest(ctx, caller.c, caller.userID, id)
}

// Tokens returns the approximate number of available tokens for this user.
// Returns -1 if the user has no in-memory state.
func (caller *Caller) Tokens() float64 {
	return caller.c.Tokens(caller.userID)
}

// Evict removes this user from the in-memory shard immediately.
// Returns false if the user had no in-memory state.
func (caller *Caller) Evict() bool {
	return caller.c.Evict(caller.userID)
}

