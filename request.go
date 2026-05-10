package pace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/jaeminst/pace/internal/store"
)

// Request is a chainable HTTP request builder. Obtain one via [Manager.Request]
// (rate-limit token already consumed) or [Manager.Durable] (durable execution).
// Call one of Get, Post, Put, Delete, or Patch to execute.
type Request struct {
	ctx     context.Context
	client  *http.Client
	baseURL string
	headers map[string]string
	body    []byte

	// non-nil only for requests created by Manager.Durable
	mgr          *Manager
	userID       string
	endpointName string
	durableID    string
	durableErr   error // deferred error from Durable() setup
}

func newRequest(ctx context.Context, client *http.Client, baseURL string) *Request {
	return &Request{ctx: ctx, client: client, baseURL: baseURL, headers: make(map[string]string)}
}

func newDurableRequest(ctx context.Context, m *Manager, userID, endpointName, id string, e *ep) *Request {
	return &Request{
		ctx:          ctx,
		client:       e.client,
		baseURL:      e.cfg.BaseURL,
		headers:      make(map[string]string),
		mgr:          m,
		userID:       userID,
		endpointName: endpointName,
		durableID:    id,
	}
}

// SetHeader adds or replaces an HTTP header. It returns r for chaining.
func (r *Request) SetHeader(key, value string) *Request {
	r.headers[key] = value
	return r
}

// SetBody sets the request body. It returns r for chaining.
func (r *Request) SetBody(body []byte) *Request { r.body = body; return r }

// Get executes an HTTP GET to path (appended to the endpoint BaseURL).
func (r *Request) Get(path string) (*Response, error) { return r.do(http.MethodGet, path) }

// Post executes an HTTP POST to path.
func (r *Request) Post(path string) (*Response, error) { return r.do(http.MethodPost, path) }

// Put executes an HTTP PUT to path.
func (r *Request) Put(path string) (*Response, error) { return r.do(http.MethodPut, path) }

// Delete executes an HTTP DELETE to path.
func (r *Request) Delete(path string) (*Response, error) { return r.do(http.MethodDelete, path) }

// Patch executes an HTTP PATCH to path.
func (r *Request) Patch(path string) (*Response, error) { return r.do(http.MethodPatch, path) }

func (r *Request) do(method, path string) (*Response, error) {
	if r.durableErr != nil {
		return nil, r.durableErr
	}
	if r.durableID != "" {
		return r.doDurable(method, path)
	}
	return r.execute(method, path)
}

// execute performs the HTTP round-trip. For regular requests the rate-limit
// token was already consumed by Manager.Request; for durable requests it is
// consumed inside doDurable before execute is called.
func (r *Request) execute(method, path string) (*Response, error) {
	var bodyReader io.Reader
	if r.body != nil {
		bodyReader = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(r.ctx, method, r.baseURL+path, bodyReader)
	if err != nil {
		return nil, fmt.Errorf("pace: build request: %w", err)
	}
	for k, v := range r.headers {
		req.Header.Set(k, v)
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close() //nolint:errcheck
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

// doDurable executes the request with exactly-once semantics: persists the job
// to SQLite before execution and caches the result afterwards. Concurrent calls
// with the same ID share one in-flight execution (singleflight).
func (r *Request) doDurable(method, path string) (*Response, error) {
	m := r.mgr
	id := r.durableID

	// Check inflight first: avoids a DB round-trip for concurrent duplicates.
	m.inflightMu.Lock()
	if f, exists := m.inflight[id]; exists {
		m.inflightMu.Unlock()
		return await(r.ctx, f)
	}
	m.inflightMu.Unlock()

	// Check DB for a result cached by a previous run.
	result, ok, err := m.sqliteStore.Get(id)
	if err != nil {
		return nil, fmt.Errorf("pace: durable: %w", err)
	}
	if ok {
		return toResponse(result), nil
	}

	// Double-check inflight under lock before becoming leader.
	m.inflightMu.Lock()
	if f, exists := m.inflight[id]; exists {
		m.inflightMu.Unlock()
		return await(r.ctx, f)
	}
	f := &future{done: make(chan struct{})}
	m.inflight[id] = f
	m.inflightMu.Unlock()

	defer func() {
		m.inflightMu.Lock()
		delete(m.inflight, id)
		m.inflightMu.Unlock()
		close(f.done)
	}()

	if hook := m._testHookDurableBeforeEnqueue; hook != nil {
		hook()
	}
	if err := m.sqliteStore.Enqueue(store.Job{
		ID:       id,
		UserID:   r.userID,
		Endpoint: r.endpointName,
		Method:   method,
		Path:     path,
		Headers:  r.headers,
		Body:     r.body,
	}); err != nil {
		f.err = fmt.Errorf("pace: durable: enqueue: %w", err)
		return nil, f.err
	}

	// Acquire rate-limit token via Manager.Request, which also handles
	// ErrClosed, Shutdown checks, activeWg, and the OnThrottle callback.
	inner, err := m.Request(r.ctx, r.userID, r.endpointName)
	if err != nil {
		f.err = err
		return nil, f.err
	}
	for k, v := range r.headers {
		inner.SetHeader(k, v)
	}
	inner.SetBody(r.body)

	resp, err := inner.execute(method, path)
	if err != nil {
		f.err = err
		return nil, f.err
	}

	if cerr := m.sqliteStore.Complete(id, store.Result{
		StatusCode: resp.statusCode,
		Status:     resp.status,
		Headers:    resp.header,
		Body:       resp.body,
	}); cerr != nil {
		m.logger.Warn("pace: durable: complete", "id", id, "err", cerr)
	}

	f.resp = resp
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
