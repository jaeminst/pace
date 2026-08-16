package pace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/jaeminst/pace/internal/store"
)

// Request is a chainable HTTP request builder. Obtain one via [Client.Request]
// or [Client.Durable], then call Get, Post, Put, Delete, or Patch to execute.
//
// Building a Request costs nothing: no rate-limit token is consumed until a
// terminal method runs, so a Request that is abandoned — on a validation
// failure, an early return, a panic — leaves the user's quota untouched.
type Request struct {
	lim     *Limiter
	userID  string
	headers http.Header
	body    []byte

	// set only for requests created by Client.Durable
	durable   bool
	durableID string
}

func newRequest(l *Limiter, userID string) *Request {
	return &Request{lim: l, userID: userID, headers: make(http.Header)}
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

// SetBody sets the request body. It returns r for chaining.
func (r *Request) SetBody(body []byte) *Request { r.body = body; return r }

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
	l := r.lim

	l.shutdownMu.RLock()
	if l.shuttingDown {
		l.shutdownMu.RUnlock()
		return nil, ErrClosed
	}
	l.activeWg.Add(1)
	l.shutdownMu.RUnlock()
	defer l.activeWg.Done()

	reqCtx, release := l.withLifetime(ctx)
	defer release()

	if r.durable {
		return r.doDurable(reqCtx, method, path)
	}
	return r.send(reqCtx, method, path)
}

// send builds the request, pays for it, then performs the round-trip. Building
// comes first so a malformed URL fails without costing a token.
func (r *Request) send(ctx context.Context, method, path string) (*Response, error) {
	httpReq, err := r.build(ctx, method, path)
	if err != nil {
		return nil, err
	}
	if err := r.lim.acquire(ctx, r.userID); err != nil {
		return nil, err
	}
	return r.roundTrip(httpReq)
}

func (r *Request) build(ctx context.Context, method, path string) (*http.Request, error) {
	var bodyReader io.Reader
	if r.body != nil {
		bodyReader = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(ctx, method, r.lim.cfg.BaseURL+path, bodyReader)
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
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("pace: read response: %w", err)
	}
	return &Response{
		statusCode: resp.StatusCode,
		status:     resp.Status,
		body:       b,
		header:     resp.Header,
	}, nil
}

// doDurable executes the request against the durable queue.
//
// The order is what gives the queue its properties: record the job, claim it
// exclusively, commit the intent to send, dispatch, then record the outcome.
// Committing before dispatch is what makes a crash detectable — a job found
// mid-flight afterwards is one whose outcome is genuinely unknown, which is a
// fact worth storing rather than a case to guess at.
//
// Concurrent callers in this process share one execution (singleflight); the
// claim is what stops a second process, or a replay goroutine, from sending the
// same request again.
func (r *Request) doDurable(ctx context.Context, method, path string) (*Response, error) {
	l, id := r.lim, r.durableID

	f, leading := l.joinOrLead(id)
	if !leading {
		return await(ctx, f)
	}
	defer l.finishInflight(id, f)

	// A result recorded by an earlier run, possibly in an earlier process,
	// means the request was already delivered.
	if result, ok, err := l.sqliteStore.Get(ctx, id); err != nil {
		f.err = fmt.Errorf("pace: durable: %w", err)
		return nil, f.err
	} else if ok {
		f.resp = toResponse(result)
		return f.resp, nil
	}

	f.resp, f.err = r.sendDurable(ctx, method, path)
	return f.resp, f.err
}

// sendDurable performs one attempt at a durable job, from recording it to
// recording its outcome.
func (r *Request) sendDurable(ctx context.Context, method, path string) (*Response, error) {
	l, id := r.lim, r.durableID

	if hook := l._testHookDurableBeforeEnqueue; hook != nil {
		hook()
	}
	if err := l.sqliteStore.Enqueue(ctx, store.Job{
		ID:      id,
		UserID:  r.userID,
		Method:  method,
		Path:    path,
		Headers: r.headers,
		Body:    r.body,
	}, l.cfg.Clock.Now().UnixNano()); err != nil {
		return nil, fmt.Errorf("pace: durable: enqueue: %w", err)
	}

	// Claim before dispatching. The row was deduplicated by INSERT OR IGNORE,
	// but that deduplicates the *row*, not the *send*: without this, a replay
	// worker and a live caller could both decide they were the leader and put
	// the same request on the wire twice. The claim is one conditional UPDATE,
	// so exactly one of them wins.
	now := l.cfg.Clock.Now()
	claimed, attempt, err := l.sqliteStore.ClaimN(ctx, id, l.owner, now.UnixNano(), now.Add(l.cfg.JobLease).UnixNano())
	if err != nil {
		return nil, fmt.Errorf("pace: durable: claim: %w", err)
	}
	if !claimed {
		return nil, fmt.Errorf("pace: durable %q: %w", id, ErrJobClaimed)
	}

	httpReq, err := r.build(ctx, method, path)
	if err != nil {
		l.releaseJob(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see releaseJob
		return nil, err
	}
	if l.cfg.IdempotencyHeader != "" {
		httpReq.Header.Set(l.cfg.IdempotencyHeader, id)
	}
	if err := l.acquire(ctx, r.userID); err != nil {
		// Nothing was dispatched, so the job is unambiguously still pending.
		l.releaseJob(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see releaseJob
		return nil, err
	}

	resp, err := r.roundTrip(httpReq)
	if err != nil {
		// No response means no way to know whether bytes reached the server.
		// scheduleRetry applies the same ambiguity rules the startup path uses
		// rather than assuming it was not delivered — the wrong assumption
		// sends a payment twice.
		l.scheduleRetry(job{id: id, method: method, attempts: attempt}, err) //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
		return nil, err
	}

	// A response, of any status, means the request was delivered — which is
	// what the queue promises. Whether that response is worth repeating is the
	// caller's judgement, not pace's.
	if l.cfg.RetryOn != nil && l.cfg.RetryOn(resp) {
		l.scheduleRetry( //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
			job{id: id, method: method, attempts: attempt, delivered: true},
			fmt.Errorf("pace: durable: response %d rejected by RetryOn", resp.statusCode))
		return resp, nil
	}

	if cerr := l.completeJob(ctx, id, resp); cerr != nil {
		// The response is in hand but could not be recorded. Log at Error, not
		// Warn: this is lost data, and the job is now ambiguous.
		l.cfg.Logger.Error("pace: durable: record result", "job", id, "err", cerr)
	}
	return resp, nil
}

// Response wraps an HTTP response. All fields are immutable after construction.
type Response struct {
	statusCode int
	status     string
	body       []byte
	header     http.Header
}

// StatusCode returns the HTTP status code (e.g. 200, 404).
func (r *Response) StatusCode() int { return r.statusCode }

// Status returns the HTTP status string (e.g. "200 OK").
func (r *Response) Status() string { return r.status }

// Body returns the fully-read response body.
func (r *Response) Body() []byte { return r.body }

// Header returns the response headers.
func (r *Response) Header() http.Header { return r.header }
