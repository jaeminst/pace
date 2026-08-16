package pace

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/jaeminst/pace/internal/store"
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
	target, err := r.lim.buildURL(path, r.query)
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
		f.resp = toResponse(result, l.cfg.Clock)
		return f.resp, nil
	}

	f.resp, f.err = r.sendDurable(ctx, method, path)
	return f.resp, f.err
}

// sendDurable performs one attempt at a durable job, from recording it to
// recording its outcome.
func (r *Request) sendDurable(ctx context.Context, method, path string) (*Response, error) {
	l, id := r.lim, r.durableID

	l.fireDurableBeforeEnqueue()
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
	claimed, attempt, err := l.sqliteStore.ClaimN(ctx, id, l.owner, now.UnixNano(), now.Add(l.cfg.Queue.JobLease).UnixNano())
	if err != nil {
		return nil, fmt.Errorf("pace: durable: claim: %w", err)
	}
	if !claimed {
		// Losing the claim has two causes and they need different answers.
		// Another worker may still be sending, in which case there is nothing
		// to report but the contention — or it may have already finished, in
		// which case the result is now in the cache and this caller should get
		// the response rather than an error. The first read of the cache
		// happened before the claim; this one happens after, which is what
		// makes the difference visible.
		if result, ok, gerr := l.sqliteStore.Get(ctx, id); gerr == nil && ok {
			return toResponse(result, l.cfg.Clock), nil
		}
		return nil, fmt.Errorf("pace: durable %q: %w", id, ErrJobClaimed)
	}
	l.observeJob(JobInfo{ID: id, UserID: r.userID, Method: method, Phase: JobClaimed, Attempt: attempt})

	httpReq, err := r.build(ctx, method, path)
	if err != nil {
		l.releaseJob(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see releaseJob
		return nil, err
	}
	if l.cfg.Queue.IdempotencyHeader != "" {
		httpReq.Header.Set(l.cfg.Queue.IdempotencyHeader, id)
	}
	if err := l.acquire(ctx, r.userID); err != nil {
		// Nothing was dispatched, so the job is unambiguously still pending.
		l.releaseJob(id, err) //nolint:contextcheck // the release must outlive a cancelled request ctx; see releaseJob
		return nil, err
	}

	timed, cancel := l.withRequestTimeout(ctx)
	defer cancel()
	var started time.Time
	if l.observesRequests() {
		started = l.cfg.Clock.Now()
	}
	resp, err := r.roundTrip(r.lim.timed(timed, httpReq))
	l.countRequest(err)
	if l.observesRequests() {
		l.cfg.Observer.RequestFinished(ctx, RequestInfo{
			UserID:  r.userID,
			Method:  method,
			Path:    path,
			Status:  statusOf(resp),
			Latency: l.cfg.Clock.Now().Sub(started),
			Durable: true,
			Err:     err,
		})
	}
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
	if l.cfg.Queue.RetryOn != nil && l.cfg.Queue.RetryOn(resp) {
		l.scheduleRetry( //nolint:contextcheck // bookkeeping must outlive a cancelled request ctx
			job{id: id, method: method, attempts: attempt, delivered: true},
			fmt.Errorf("pace: durable: response %d rejected by RetryOn", resp.statusCode))
		return resp, nil
	}

	if cerr := l.completeJob(ctx, id, resp); cerr != nil {
		// The response is in hand but could not be recorded. Log at Error, not
		// Warn: this is lost data, and the job is now ambiguous.
		l.cfg.Logger.Error("pace: durable: record result", "job", id, "err", cerr)
	} else {
		l.observeJob(JobInfo{ID: id, UserID: r.userID, Method: method, Phase: JobCompleted, Attempt: attempt})
	}
	return resp, nil
}

// Response wraps an HTTP response. All fields are immutable after construction.
type Response struct {
	statusCode int
	status     string
	body       []byte
	header     http.Header
	// clock is the Limiter's, so RetryAfter's relative answer is measured
	// against the same time source as everything else pace reports. Reading
	// time.Now here would make one method in the package ignore Config.Clock.
	clock Clock
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
		// A hostile server can send a number whose nanoseconds overflow int64
		// and wrap negative, which a caller comparing against a threshold
		// would read as "retry immediately". Cap it instead.
		if secs > maxRetryAfterSeconds {
			return time.Duration(math.MaxInt64), true
		}
		return time.Duration(secs) * time.Second, true
	}
	when, err := http.ParseTime(v)
	if err != nil {
		return 0, false
	}
	// The header carries an absolute time; report it relative to now, and
	// never negative — a date already past means "retry immediately".
	return max(0, when.Sub(r.now())), true
}

// maxRetryAfterSeconds is the largest Retry-After value that still fits in a
// time.Duration.
const maxRetryAfterSeconds = int(math.MaxInt64 / int64(time.Second))

// now reads the Limiter's clock, defaulting to the real one for a Response
// built outside a Limiter.
func (r *Response) now() time.Time {
	if r.clock == nil {
		return time.Now()
	}
	return r.clock.Now()
}

// Status returns the HTTP status string (e.g. "200 OK").
func (r *Response) Status() string { return r.status }

// Body returns the fully-read response body.
func (r *Response) Body() []byte { return r.body }

// Header returns the response headers.
func (r *Response) Header() http.Header { return r.header }

// statusOf reports a response's status, or zero when there was none.
func statusOf(resp *Response) int {
	if resp == nil {
		return 0
	}
	return resp.statusCode
}

// httpStatusOf is statusOf for the raw response Stream hands back.
func httpStatusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}
