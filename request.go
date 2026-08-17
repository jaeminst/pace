package pace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/jaeminst/pace/internal/urlx"
)

// Request is a chainable HTTP request builder. Obtain one via [Client.Request]
// or [Client.Durable], then call Get, Post, Put, Delete, or Patch to execute.
//
// Building a Request costs nothing and cannot fail: no rate-limit token is
// consumed until a terminal method runs, so a Request that is abandoned — on a
// validation failure, an early return, a panic — leaves the user's quota
// untouched, and every builder method returns r so the chain never breaks.
//
// Setup failures that a builder method notices — a body that will not encode,
// a [Client.Durable] with no queue behind it — are held and returned by the
// terminal method, which is already returning an error.
type Request struct {
	lim     *Limiter
	userID  string
	headers http.Header
	body    []byte
	query   url.Values
	// err is a setup failure deferred to the terminal method: a SetJSON
	// encoding error, or a Durable that could not be honoured. One slot, so
	// every terminal path has one thing to check.
	err error

	// set only for requests created by Client.Durable
	durable   bool
	durableID string
}

func newRequest(l *Limiter, userID string) *Request {
	return &Request{lim: l, userID: userID, headers: make(http.Header)}
}

// setErr records the first deferred setup failure. First rather than last:
// the earliest thing that went wrong is the one that explains the rest.
func (r *Request) setErr(err error) {
	if r.err == nil {
		r.err = err
	}
}

// SetHeader sets an HTTP header, replacing any existing values. It returns r
// for chaining.
func (r *Request) SetHeader(key, value string) *Request {
	r.headers.Set(key, value)
	return r
}

// AddHeader appends a value to an HTTP header, keeping any existing values.
// Use it for headers that legitimately repeat, such as Accept or Cookie.
func (r *Request) AddHeader(key, value string) *Request {
	r.headers.Add(key, value)
	return r
}

// Header returns the request's headers for direct manipulation. The returned
// map is live: writing to it affects the request.
func (r *Request) Header() http.Header { return r.headers }

// SetQuery sets a query parameter, replacing any existing values for the key.
// It returns r for chaining.
//
// Parameters set here are merged with any already present in the path, and are
// escaped properly — which hand-built query strings routinely are not.
func (r *Request) SetQuery(key, value string) *Request {
	if r.query == nil {
		r.query = url.Values{}
	}
	r.query.Set(key, value)
	return r
}

// AddQuery appends a query parameter, keeping any existing values for the key.
func (r *Request) AddQuery(key, value string) *Request {
	if r.query == nil {
		r.query = url.Values{}
	}
	r.query.Add(key, value)
	return r
}

// SetQueryValues replaces the request's query parameters wholesale.
func (r *Request) SetQueryValues(v url.Values) *Request {
	r.query = v
	return r
}

// SetBody sets the request body. It returns r for chaining.
func (r *Request) SetBody(body []byte) *Request { r.body = body; return r }

// SetJSON encodes v as the request body and sets Content-Type. An encoding
// failure surfaces from the terminal method, since that is where the request
// would have been sent.
//
// Deferring the error keeps the builder infallible, which is what makes it
// chainable: the failure appears at the one call that was going to use it, and
// that call is already returning an error.
func (r *Request) SetJSON(v any) *Request {
	b, err := json.Marshal(v)
	if err != nil {
		r.setErr(fmt.Errorf("pace: encode request body: %w", err))
		return r
	}
	r.body = b
	r.headers.Set("Content-Type", "application/json")
	return r
}

// Get acquires a rate-limit token and executes an HTTP GET to path
// (appended to the Limiter's BaseURL).
func (r *Request) Get(ctx context.Context, path string) (*Response, error) {
	return r.do(ctx, http.MethodGet, path)
}

// Post acquires a token and executes an HTTP POST to path.
func (r *Request) Post(ctx context.Context, path string) (*Response, error) {
	return r.do(ctx, http.MethodPost, path)
}

// Put acquires a token and executes an HTTP PUT to path.
func (r *Request) Put(ctx context.Context, path string) (*Response, error) {
	return r.do(ctx, http.MethodPut, path)
}

// Delete acquires a token and executes an HTTP DELETE to path.
func (r *Request) Delete(ctx context.Context, path string) (*Response, error) {
	return r.do(ctx, http.MethodDelete, path)
}

// Patch acquires a token and executes an HTTP PATCH to path.
func (r *Request) Patch(ctx context.Context, path string) (*Response, error) {
	return r.do(ctx, http.MethodPatch, path)
}

// Do acquires a token and executes an arbitrary HTTP method against path.
func (r *Request) Do(ctx context.Context, method, path string) (*Response, error) {
	return r.do(ctx, method, path)
}

// Stream acquires a token and executes the request, returning the raw
// [http.Response] with its body unread. The caller owns that body and must
// close it.
//
// Use it for responses too large to hold in memory. Nothing else in pace hands
// back an unread body, so [Config.MaxResponseBytes] does not apply here — the
// whole point is that the body is never buffered.
//
// [Config.RequestTimeout] does not apply either, for the same reason. A context
// deadline does not end when the headers arrive; it stays armed until the body
// is closed, so imposing one here would cut off exactly the long download
// Stream exists to enable. The hang it would otherwise catch — a server that
// accepts the connection and never answers — is covered by
// [TransportConfig.ResponseHeaderTimeout], which is on by default and bounds
// the wait for headers without bounding the body.
//
// [Observer.RequestFinished] fires when this call returns, with the response
// headers in hand; its Latency therefore excludes the time the caller spends
// reading the body, which pace does not observe.
//
// Stream is not available for durable requests: the queue caches a response so
// it can be returned again later, which it cannot do for a stream that is
// consumed once.
func (r *Request) Stream(ctx context.Context, method, path string) (*http.Response, error) {
	if r.err != nil {
		return nil, r.err
	}
	if r.durable {
		return nil, ErrStreamDurable
	}
	l := r.lim

	if !l.enter() {
		return nil, ErrClosed
	}

	// The caller reads the body after this returns, so the request context has
	// to outlive this function. Ownership of the context and of the in-flight
	// registration passes to the returned body, which releases both on Close.
	reqCtx, release := l.withLifetime(ctx)
	done := func() {
		release()
		l.leave()
	}

	httpReq, err := r.build(reqCtx, method, path)
	if err != nil {
		done()
		return nil, err
	}
	if err := l.acquire(reqCtx, r.userID); err != nil {
		done()
		return nil, err
	}

	var started time.Time
	if l.observesRequests() {
		started = l.cfg.Clock.Now()
	}
	resp, err := l.httpClient.Do(httpReq)
	// Counted and reported exactly as send does it: a streamed request is still
	// a request, and leaving it out made Stats.Requests and Stats.Errors count
	// different populations.
	l.countRequest(err)
	if l.observesRequests() {
		l.cfg.Observer.RequestFinished(ctx, RequestInfo{
			UserID:  r.userID,
			Method:  method,
			Path:    path,
			Status:  httpStatusOf(resp),
			Latency: l.cfg.Clock.Now().Sub(started),
			Err:     err,
		})
	}
	if err != nil {
		done()
		return nil, err
	}
	resp.Body = &releasingBody{ReadCloser: resp.Body, release: done}
	return resp, nil
}

// do runs one request end to end. Everything that must outlive the round-trip
// is established here, in a single scope:
//
//   - The active-request registration spans the whole operation, so Shutdown
//     genuinely waits for requests that are on the wire. The shuttingDown check
//     and activeWg.Add share the mutex, so no Add can slip past Shutdown's Wait.
//   - The request context merges the caller's context with the Limiter's
//     lifetime, so cancelling the Limiter aborts a round-trip in progress.
//     Without it, Shutdown's own force-cancel could not end a request against a
//     server that never answers.
func (r *Request) do(ctx context.Context, method, path string) (*Response, error) {
	// Ahead of everything else: doDurable dereferences the queue handle, and a
	// Request whose Durable was refused has no queue to dereference.
	if r.err != nil {
		return nil, r.err
	}
	l := r.lim

	if !l.enter() {
		return nil, ErrClosed
	}
	defer l.leave()

	reqCtx, release := l.withLifetime(ctx)
	defer release()

	if r.durable {
		return r.doDurable(reqCtx, method, path)
	}
	return r.send(reqCtx, method, path)
}

// send builds the request, pays for it, then performs the round-trip. Building
// comes first so a malformed URL fails without costing a token.
//
// RequestTimeout starts after the token is acquired, not before. A request held
// back by throttling has not begun; charging that wait against its timeout would
// make the timeout depend on how busy the user happens to be.
func (r *Request) send(ctx context.Context, method, path string) (*Response, error) {
	httpReq, err := r.build(ctx, method, path)
	if err != nil {
		return nil, err
	}
	if err := r.lim.acquire(ctx, r.userID); err != nil {
		return nil, err
	}
	// The timeout is attached after the token is paid for, so the clock starts
	// with the round-trip rather than with the wait for it.
	timed, cancel := r.lim.withRequestTimeout(ctx)
	defer cancel()

	var started time.Time
	if r.lim.observesRequests() {
		started = r.lim.cfg.Clock.Now()
	}
	resp, err := r.roundTrip(r.lim.timed(timed, httpReq))
	r.lim.countRequest(err)
	if r.lim.observesRequests() {
		r.lim.cfg.Observer.RequestFinished(ctx, RequestInfo{
			UserID:  r.userID,
			Method:  method,
			Path:    path,
			Status:  statusOf(resp),
			Latency: r.lim.cfg.Clock.Now().Sub(started),
			Err:     err,
		})
	}
	return resp, err
}

func (r *Request) build(ctx context.Context, method, path string) (*http.Request, error) {
	var bodyReader io.Reader
	if r.body != nil {
		bodyReader = bytes.NewReader(r.body)
	}
	target, err := urlx.Build(r.lim.cfg.BaseURL, path, r.query)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("pace: build request: %w", err)
	}
	if len(r.headers) > 0 {
		req.Header = r.headers.Clone()
	}
	return req, nil
}

func (r *Request) roundTrip(req *http.Request) (*Response, error) {
	resp, err := r.lim.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // body is fully drained below; a close error tells the caller nothing actionable

	body, err := readBody(resp.Body, r.lim.cfg.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	return &Response{
		statusCode: resp.StatusCode,
		status:     resp.Status,
		body:       body,
		header:     resp.Header,
		clock:      r.lim.cfg.Clock,
	}, nil
}

// readBody buffers the response, refusing to exceed max.
//
// It reads one byte past the limit rather than stopping at it, so that hitting
// the cap is reported as an error instead of silently handing back a truncated
// body that looks complete.
func readBody(rc io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes <= 0 {
		b, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("pace: read response: %w", err)
		}
		return b, nil
	}
	b, err := io.ReadAll(io.LimitReader(rc, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("pace: read response: %w", err)
	}
	if int64(len(b)) > maxBytes {
		return nil, fmt.Errorf("pace: response exceeds %d bytes: %w", maxBytes, ErrBodyTooLarge)
	}
	return b, nil
}

// releasingBody hands back the in-flight registration and the request context
// when a streamed body is closed. Without it, a caller who never closes the
// body would keep Shutdown waiting indefinitely.
type releasingBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (b *releasingBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(b.release)
	return err
}

// withRequestTimeout bounds one HTTP round-trip, if RequestTimeout is set.
//
// It is applied after the rate-limit token is acquired. A request queued behind
// throttling has not started, and charging that wait against its timeout would
// make the timeout a function of how busy the user is rather than of how slow
// the server is.
func (l *Limiter) withRequestTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if l.cfg.RequestTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, l.cfg.RequestTimeout)
}

// timed re-attaches a request to a deadline-bearing context, if there is one.
//
// http.Request.WithContext copies the whole request, so it is skipped entirely
// when no RequestTimeout is configured — which is the default, and the path
// most callers take.
func (l *Limiter) timed(ctx context.Context, req *http.Request) *http.Request {
	if l.cfg.RequestTimeout <= 0 {
		return req
	}
	return req.WithContext(ctx)
}
