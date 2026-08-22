package limiter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/jaeminst/pace/response"

	"github.com/jaeminst/pace/urlx"
)

// Request is a chainable HTTP request builder. Obtain one via [Client.Request],
// then call Get, Post, Put, Delete, or Patch to execute.
//
// Building a Request costs nothing and cannot fail: no rate-limit token is
// consumed until a terminal method runs, so a Request that is abandoned — on a
// validation failure, an early return, a panic — leaves the user's quota
// untouched, and every builder method returns r so the chain never breaks.
//
// A setup failure that a builder method notices — a body that will not encode
// — is held and returned by the terminal method, which is already returning an
// error.
type Request struct {
	lim     *Limiter
	userID  string
	headers http.Header
	body    []byte
	query   url.Values
	// err is a setup failure deferred to the terminal method — today only a
	// SetJSON encoding error. One slot, so every terminal path has one thing
	// to check.
	err error
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
func (r *Request) Get(ctx context.Context, path string) (*response.Response, error) {
	return r.do(ctx, http.MethodGet, path)
}

// Post acquires a token and executes an HTTP POST to path.
func (r *Request) Post(ctx context.Context, path string) (*response.Response, error) {
	return r.do(ctx, http.MethodPost, path)
}

// Put acquires a token and executes an HTTP PUT to path.
func (r *Request) Put(ctx context.Context, path string) (*response.Response, error) {
	return r.do(ctx, http.MethodPut, path)
}

// Delete acquires a token and executes an HTTP DELETE to path.
func (r *Request) Delete(ctx context.Context, path string) (*response.Response, error) {
	return r.do(ctx, http.MethodDelete, path)
}

// Patch acquires a token and executes an HTTP PATCH to path.
func (r *Request) Patch(ctx context.Context, path string) (*response.Response, error) {
	return r.do(ctx, http.MethodPatch, path)
}

// Do acquires a token and executes an arbitrary HTTP method against path.
func (r *Request) Do(ctx context.Context, method, path string) (*response.Response, error) {
	return r.do(ctx, method, path)
}

func (r *Request) do(ctx context.Context, method, path string) (*response.Response, error) {
	// Ahead of everything else, so a setup failure is reported without costing
	// a token or touching the shutdown barrier.
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

	return r.send(reqCtx, method, path)
}

// send builds the request, pays for it, then performs the round-trip. Building
// comes first so a malformed URL fails without costing a token.
//
// RequestTimeout starts after the token is acquired, not before. A request held
// back by throttling has not begun; charging that wait against its timeout would
// make the timeout depend on how busy the user happens to be.
func (r *Request) send(ctx context.Context, method, path string) (*response.Response, error) {
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

	started := r.lim.startTiming()
	resp, err := r.roundTrip(r.lim.timed(timed, httpReq))
	r.lim.finishRequest(ctx, started, r.userID, method, path, statusOf(resp), err)
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

func (r *Request) roundTrip(req *http.Request) (*response.Response, error) {
	resp, err := r.lim.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck // body is fully drained below; a close error tells the caller nothing actionable

	body, err := readBody(resp.Body, r.lim.cfg.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	return response.New(resp.StatusCode, resp.Status, body, resp.Header, r.lim.cfg.Now), nil
}

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
