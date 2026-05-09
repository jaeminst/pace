package pace

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
)

// Request is a chainable HTTP request builder.
// Obtain one via Manager.Request; the rate-limit token is already consumed.
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

func (r *Request) SetHeader(key, value string) *Request {
	r.headers[key] = value
	return r
}

func (r *Request) SetBody(body []byte) *Request {
	r.body = body
	return r
}

func (r *Request) Get(path string) (*Response, error)    { return r.do(http.MethodGet, path) }
func (r *Request) Post(path string) (*Response, error)   { return r.do(http.MethodPost, path) }
func (r *Request) Put(path string) (*Response, error)    { return r.do(http.MethodPut, path) }
func (r *Request) Delete(path string) (*Response, error) { return r.do(http.MethodDelete, path) }
func (r *Request) Patch(path string) (*Response, error)  { return r.do(http.MethodPatch, path) }

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
	defer resp.Body.Close()
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

// Response wraps an HTTP response with value-type accessors.
type Response struct {
	statusCode int
	status     string
	body       []byte
	header     http.Header
}

func (r *Response) Status() string      { return r.status }
func (r *Response) StatusCode() int     { return r.statusCode }
func (r *Response) Body() []byte        { return r.body }
func (r *Response) Header() http.Header { return r.header }
