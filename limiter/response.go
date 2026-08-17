package limiter

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"
)

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
