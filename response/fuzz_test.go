package response

import (
	"net/http"
	"testing"
	"time"
)

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
		r := New(0, "", nil, http.Header{"Retry-After": []string{header}}, time.Now)
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
