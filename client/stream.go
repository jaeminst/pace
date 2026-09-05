// stream.go is the path that hands the caller an unread body, and the buffering
// the rest of the request path does instead.
//
// The two live together because they are the same decision seen from either
// side: Stream transfers ownership of the response and its lifetime to the
// caller, and readBody is what happens when pace keeps it.

package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/jaeminst/pace/limiter"
)

// Stream acquires a token and executes the request, returning the raw
// [http.Response] with its body unread. The caller owns that body and must
// close it.
//
// Use it for responses too large to hold in memory. Nothing else in pace hands
// back an unread body, so config.Config.MaxResponseBytes does not apply here — the
// whole point is that the body is never buffered.
//
// config.Config.RequestTimeout does not apply either, for the same reason. A context
// deadline does not end when the headers arrive; it stays armed until the body
// is closed, so imposing one here would cut off exactly the long download
// Stream exists to enable. The hang it would otherwise catch — a server that
// accepts the connection and never answers — is yours to bound with
// [http.Transport.ResponseHeaderTimeout] on the transport you pass in, which
// limits the wait for headers without limiting the body. Set it: neither
// [http.DefaultTransport] nor a clone of it has one.
//
// observe.Observer.RequestFinished fires when this call returns, with the response
// headers in hand; its Latency therefore excludes the time the caller spends
// reading the body, which pace does not observe.
func (r *Request) Stream(ctx context.Context, method, path string) (*http.Response, error) {
	if r.err != nil {
		return nil, r.err
	}
	l := r.pool

	// The caller reads the body after this returns, so the request context has
	// to outlive this function. Ownership of both the context and the in-flight
	// registration passes to the returned body, which releases them on Close —
	// which is why Enter's func is passed on here rather than deferred.
	reqCtx, done, ok := l.lim.Enter(ctx)
	if !ok {
		return nil, limiter.ErrClosed
	}

	httpReq, err := r.build(reqCtx, method, path)
	if err != nil {
		done()
		return nil, err
	}
	if err := l.lim.Acquire(reqCtx, r.key); err != nil {
		done()
		return nil, err
	}

	// Counted and reported exactly as send does it: a streamed request is still
	// a request, and leaving it out made Stats.Requests and Stats.Errors count
	// different populations.
	started := l.lim.StartTiming()
	resp, err := l.httpClientFor(r.key).Do(httpReq)
	l.lim.FinishRequest(ctx, started, r.key, method, path, httpStatusOf(resp), err)
	if err != nil {
		done()
		return nil, err
	}
	resp.Body = &releasingBody{ReadCloser: resp.Body, release: done}
	return resp, nil
}

// readBody buffers the response, refusing to exceed maxBytes.
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

// ErrBodyTooLarge is returned when a response body exceeds
// config.Config.MaxResponseBytes. It is declared here rather than in the engine
// because the engine does not read bodies.
var ErrBodyTooLarge = errors.New("pace: response body too large")
