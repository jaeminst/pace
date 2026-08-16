package pace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

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
	bodyErr error // deferred encoding failure from SetJSON

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

// SetJSON encodes v as the request body and sets Content-Type. An encoding
// failure surfaces from the terminal method, since that is where the request
// would have been sent.
//
// Deferring the error is idiomatic here in a way it was not for Durable: what
// is being configured is the body itself, and the failure appears at the one
// call that was going to use it.
func (r *Request) SetJSON(v any) *Request {
	b, err := json.Marshal(v)
	if err != nil {
		r.bodyErr = fmt.Errorf("pace: encode request body: %w", err)
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
// Stream is not available for durable requests: the queue caches a response so
// it can be returned again later, which it cannot do for a stream that is
// consumed once.
func (r *Request) Stream(ctx context.Context, method, path string) (*http.Response, error) {
	if r.durable {
		return nil, ErrStreamDurable
	}
	l := r.lim

	l.shutdownMu.RLock()
	if l.shuttingDown {
		l.shutdownMu.RUnlock()
		return nil, ErrClosed
	}
	l.activeWg.Add(1)
	l.shutdownMu.RUnlock()

	// The caller reads the body after this returns, so the request context has
	// to outlive this function. Ownership of the context and of the in-flight
	// registration passes to the returned body, which releases both on Close.
	reqCtx, release := l.withLifetime(ctx)
	done := func() {
		release()
		l.activeWg.Done()
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
	resp, err := l.httpClient.Do(httpReq)
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
	return r.roundTrip(httpReq.WithContext(timed))
}

func (r *Request) build(ctx context.Context, method, path string) (*http.Request, error) {
	if r.bodyErr != nil {
		return nil, r.bodyErr
	}
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

	body, err := readBody(resp.Body, r.lim.cfg.MaxResponseBytes)
	if err != nil {
		return nil, err
	}
	return &Response{
		statusCode: resp.StatusCode,
		status:     resp.Status,
		body:       body,
		header:     resp.Header,
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

	timed, cancel := l.withRequestTimeout(ctx)
	defer cancel()
	resp, err := r.roundTrip(httpReq.WithContext(timed))
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

// OK reports whether the status is in the 2xx range.
//
// pace does not treat a non-2xx response as an error: a 404 is a successful
// round-trip, and folding it into err would mean handing back a non-nil error
// beside a non-nil response. This is the convenience without that cost.
func (r *Response) OK() bool { return r.statusCode >= 200 && r.statusCode < 300 }

// JSON decodes the response body into v.
func (r *Response) JSON(v any) error {
	if err := json.Unmarshal(r.body, v); err != nil {
		return fmt.Errorf("pace: decode response body: %w", err)
	}
	return nil
}

// RetryAfter returns the Retry-After header as a duration, and whether it was
// present and parsable. Both forms are handled: delta-seconds and HTTP-date.
//
// This is the number that matters most to this library's readers. You throttle
// outbound requests because upstream limits you, and Retry-After is upstream
// stating the real limit — worth more than any guess pace could make.
func (r *Response) RetryAfter() (time.Duration, bool) {
	v := r.header.Get("Retry-After")
	if v == "" {
		return 0, false
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0, false
		}
		return time.Duration(secs) * time.Second, true
	}
	when, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	// The header carries an absolute time; report it relative to now, and
	// never negative — a date already past means "retry immediately".
	return max(0, time.Until(when)), true
}

// Status returns the HTTP status string (e.g. "200 OK").
func (r *Response) Status() string { return r.status }

// Body returns the fully-read response body.
func (r *Response) Body() []byte { return r.body }

// Header returns the response headers.
func (r *Response) Header() http.Header { return r.header }
