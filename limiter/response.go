package limiter

import (
	"net/http"

	"github.com/jaeminst/pace/response"
)

// statusOf reports a response's status, or zero when there was none.
func statusOf(resp *response.Response) int {
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
