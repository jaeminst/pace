package transport_test

import (
	"fmt"
	"time"

	"github.com/jaeminst/pace/transport"
)

// ExampleNew tunes connection behaviour. A zero Config behaves like
// http.DefaultTransport, so the environment proxy is kept and HTTP/2 is still
// attempted when a TLSConfig is supplied.
func ExampleNew() {
	tr := transport.New(transport.Config{
		DialTimeout:         5 * time.Second,
		TLSHandshakeTimeout: 3 * time.Second,
		MaxIdleConnsPerHost: 10,
	})
	fmt.Println("proxy honoured:", tr.Proxy != nil)
	fmt.Println("http/2 attempted:", tr.ForceAttemptHTTP2)
	// Output:
	// proxy honoured: true
	// http/2 attempted: true
}
