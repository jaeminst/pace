package response_test

import (
	"fmt"
	"net/http"

	"github.com/jaeminst/pace/response"
)

// ExampleResponse_RetryAfter reads upstream's own statement of its limit, which
// beats any guess a client could make. It is the number this library's readers
// care most about: you throttle outbound requests because upstream limits you,
// and this is upstream saying by how much.
//
// A non-2xx response is not an error in pace — the round-trip succeeded — so
// check [Response.OK] and then ask.
func ExampleResponse_RetryAfter() {
	// A Limiter builds this for you; constructed here so the example has no
	// server in it.
	resp := response.New(
		http.StatusTooManyRequests,
		"429 Too Many Requests",
		nil,
		http.Header{"Retry-After": []string{"30"}},
		nil,
	)

	if !resp.OK() {
		if after, ok := resp.RetryAfter(); ok {
			fmt.Printf("upstream asked us to wait %v\n", after)
		}
	}
	// Output:
	// upstream asked us to wait 30s
}
