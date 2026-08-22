package observe_test

import (
	"context"
	"fmt"
	"time"

	"github.com/jaeminst/pace/observe"
)

// ExampleObserver feeds metrics without polling. It is a struct of optional
// functions, so new events can be added without breaking your code: a hook you
// did not set is a hook pace skips.
//
// Supply one as github.com/jaeminst/pace.Config.Observer. This example fires
// the hooks by hand, because what an Observer *is* — and which fields you have
// to fill in — is the thing worth showing; the Limiter driving them is
// documented where the Limiter is.
func ExampleObserver() {
	obs := &observe.Observer{
		RequestFinished: func(_ context.Context, i observe.RequestInfo) {
			fmt.Printf("%s %s -> %d\n", i.Method, i.Path, i.Status)
		},
		Throttled: func(_ context.Context, i observe.ThrottleInfo) {
			fmt.Printf("throttled %s for %v (limit %v/s)\n", i.UserID, i.Delay, i.Limit)
		},
		// UserEvicted is left nil, which is how you say "not interested".
	}

	ctx := context.Background()
	obs.RequestFinished(ctx, observe.RequestInfo{Method: "GET", Path: "/items", Status: 200})
	obs.Throttled(ctx, observe.ThrottleInfo{UserID: "alice", Delay: 2 * time.Second, Limit: 1, Burst: 5})

	// Output:
	// GET /items -> 200
	// throttled alice for 2s (limit 1/s)
}
