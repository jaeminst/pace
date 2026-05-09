package pace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// Request is a chainable HTTP request builder. Obtain one via [Manager.Request];
// the rate-limit token is already consumed when Request returns.
type Request struct {
	baseURL string
	headers map[string]string
	body    []byte
	client  *http.Client
	ctx     context.Context
}

func newRequest(ctx context.Context, client *http.Client, baseURL string) *Request {
	return &Request{
		ctx:     ctx,
		client:  client,
		baseURL: baseURL,
		headers: make(map[string]string),
	}
}

// SetHeader adds or replaces an HTTP header. It returns r for chaining.
func (r *Request) SetHeader(key, value string) *Request {
	r.headers[key] = value
	return r
}

// SetBody sets the request body. It returns r for chaining.
func (r *Request) SetBody(body []byte) *Request {
	r.body = body
	return r
}

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
