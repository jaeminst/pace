// report.go is the two lines of translation between a round-trip's outcome and
// the number recorded for it.
//
// They are separate functions because the request path has two shapes: a
// buffered [*Response] and the raw [http.Response] that Stream hands back. A
// nil one means the round-trip never produced a status at all — a transport
// failure — and zero is what the engine reports for that.

package client

import "net/http"

// statusOf reports a response's status, or zero when there was none.
func statusOf(resp *Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode()
}

// httpStatusOf is statusOf for the raw response Stream hands back.
func httpStatusOf(resp *http.Response) int {
	if resp == nil {
		return 0
	}
	return resp.StatusCode
}
