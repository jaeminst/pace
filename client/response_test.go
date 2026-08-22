// response_internal_test.go tests the Response type: the two Retry-After forms,
// the JSON decode, and the status helpers. It is white-box because a Response
// is built by an unexported constructor — nothing outside this package
// assembles one.

package client

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// at is the instant every clock-dependent test measures against. A fixed one is
// the whole reason these tests live here: reached through a Limiter and a live
// server there was no clock to inject, so the HTTP-date cases could only assert
// a tolerance band.
var at = time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

func frozen() time.Time { return at }

// TestAClocklessResponseStillAnswers covers the zero value. A Response built
// without a clock — which is what a caller constructing one for a test gets —
// must still answer RetryAfter rather than panic.
func TestAClocklessResponseStillAnswers(t *testing.T) {
	r := newResponse(0, "", nil, http.Header{"Retry-After": []string{"30"}}, nil)
	if got, ok := r.RetryAfter(); !ok || got != 30*time.Second {
		t.Errorf("RetryAfter on a clockless Response = (%v, %v), want (30s, true)", got, ok)
	}
	if r.clock().IsZero() {
		t.Error("clock() on a clockless Response returned the zero time")
	}
}

// TestOK is the 2xx test. A non-2xx response is not an error in this library,
// so OK is what a caller branches on, and it is a range check that has to be
// right at both ends.
//
// 1xx is in the table because it can be. Reached through a live server it could
// not be: net/http swallows an informational response and follows it with the
// implicit 200, which the version of this test that lived in limiter/ said so
// in a comment and then skipped.
func TestOK(t *testing.T) {
	tests := map[int]bool{
		100: false, 199: false,
		200: true, 204: true, 299: true,
		300: false, 404: false, 500: false,
		0: false,
	}
	for code, want := range tests {
		if got := newResponse(code, "", nil, nil, frozen).OK(); got != want {
			t.Errorf("OK() for status %d = %v, want %v", code, got, want)
		}
	}
}

// TestAccessorsReturnWhatWasBuilt. They are one-line getters, which is exactly
// why nothing checked them until this file: each is obviously right and would
// be silently wrong if two of them were ever swapped.
func TestAccessorsReturnWhatWasBuilt(t *testing.T) {
	body := []byte("created")
	header := http.Header{"X-Custom": []string{"hello"}}
	r := newResponse(http.StatusCreated, "201 Created", body, header, frozen)

	if got := r.StatusCode(); got != http.StatusCreated {
		t.Errorf("StatusCode() = %d, want %d", got, http.StatusCreated)
	}
	if got := r.Status(); got != "201 Created" {
		t.Errorf("Status() = %q, want %q", got, "201 Created")
	}
	if got := string(r.Body()); got != "created" {
		t.Errorf("Body() = %q, want %q", got, "created")
	}
	if got := r.Header().Get("X-Custom"); got != "hello" {
		t.Errorf("Header().Get(X-Custom) = %q, want %q", got, "hello")
	}
}

func TestJSONDecodesTheBody(t *testing.T) {
	r := newResponse(http.StatusOK, "200 OK", []byte(`{"name":"alice","count":3}`), nil, frozen)

	var got struct {
		Name  string `json:"name"`
		Count int    `json:"count"`
	}
	if err := r.JSON(&got); err != nil {
		t.Fatalf("JSON = %v, want nil", err)
	}
	if got.Name != "alice" || got.Count != 3 {
		t.Errorf("decoded %+v, want {alice 3}", got)
	}
}

// TestJSONReportsAMismatch: the decode error is wrapped rather than returned
// bare, so a caller reading the message knows which library produced it.
func TestJSONReportsAMismatch(t *testing.T) {
	r := newResponse(http.StatusOK, "200 OK", []byte(`{"name":"alice"}`), nil, frozen)

	err := r.JSON(&struct{ Name int }{})
	if err == nil {
		t.Fatal("JSON accepted a body that does not fit the target type")
	}
	if want := "pace: decode response body"; !strings.Contains(err.Error(), want) {
		t.Errorf("JSON error = %q, want it to mention %q", err, want)
	}
}

// TestRetryAfterDeltaSeconds is the common form of the header, and the number
// this library's readers care most about: upstream stating its own limit beats
// any guess pace could make.
func TestRetryAfterDeltaSeconds(t *testing.T) {
	tests := []struct {
		name   string
		header string
		want   time.Duration
		ok     bool
	}{
		{"absent", "", 0, false},
		{"delta seconds", "120", 2 * time.Minute, true},
		{"zero seconds", "0", 0, true},
		{"negative seconds", "-5", 0, false},
		{"garbage", "soon", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{}
			if tt.header != "" {
				header.Set("Retry-After", tt.header)
			}
			got, ok := newResponse(http.StatusTooManyRequests, "", nil, header, frozen).RetryAfter()
			if ok != tt.ok {
				t.Fatalf("RetryAfter ok = %v, want %v", ok, tt.ok)
			}
			if ok && got != tt.want {
				t.Errorf("RetryAfter = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestRetryAfterCapsAnOverflowingValue: a hostile server can send a number
// whose nanoseconds overflow int64 and wrap negative, which a caller comparing
// against a threshold would read as "retry immediately". It is capped instead.
func TestRetryAfterCapsAnOverflowingValue(t *testing.T) {
	huge := strconv.Itoa(maxRetryAfterSeconds + 1)
	header := http.Header{"Retry-After": []string{huge}}

	got, ok := newResponse(http.StatusTooManyRequests, "", nil, header, frozen).RetryAfter()
	if !ok {
		t.Fatal("RetryAfter refused a large but well-formed value")
	}
	if got != time.Duration(math.MaxInt64) {
		t.Errorf("RetryAfter = %v, want the capped maximum", got)
	}
	if got < 0 {
		t.Error("RetryAfter wrapped negative, which reads as retry immediately")
	}
}

// TestRetryAfterHTTPDate: the header's other legal form. Because the clock is
// injected here, the future case asserts an exact duration rather than the
// tolerance band it needed when this test ran against a live server.
func TestRetryAfterHTTPDate(t *testing.T) {
	tests := []struct {
		name string
		when time.Time
		want time.Duration
	}{
		{"future", at.Add(time.Hour), time.Hour},
		{"one second out", at.Add(time.Second), time.Second},
		{"now", at, 0},
		// An absolute time already past means "retry immediately", not a
		// negative wait.
		{"past", at.Add(-time.Hour), 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			header := http.Header{"Retry-After": []string{tt.when.UTC().Format(http.TimeFormat)}}
			got, ok := newResponse(http.StatusServiceUnavailable, "", nil, header, frozen).RetryAfter()
			if !ok {
				t.Fatal("RetryAfter did not parse an HTTP-date")
			}
			if got != tt.want {
				t.Errorf("RetryAfter = %v, want exactly %v", got, tt.want)
			}
		})
	}
}

// FuzzRetryAfter covers the one header pace interprets, parsed two different
// ways, from a server that may be hostile. Whatever it says, the answer has to
// be a duration a caller can act on: never negative, never absurd.
func FuzzRetryAfter(f *testing.F) {
	f.Add("30")
	f.Add("0")
	f.Add("-1")
	f.Add("")
	f.Add("Wed, 21 Oct 2015 07:28:00 GMT")
	f.Add("9223372036854775807")
	f.Add("not a number")

	f.Fuzz(func(t *testing.T, header string) {
		r := newResponse(0, "", nil, http.Header{"Retry-After": []string{header}}, time.Now)
		got, ok := r.RetryAfter()
		if !ok {
			if got != 0 {
				t.Errorf("RetryAfter = (%v, false) for %q, want a zero duration when not ok", got, header)
			}
			return
		}
		if got < 0 {
			t.Errorf("RetryAfter = %v for %q, want a non-negative duration", got, header)
		}
	})
}
