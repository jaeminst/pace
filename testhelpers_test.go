package pace_test

import (
	"context"

	"github.com/jaeminst/pace"
)

// durableDo runs a durable request end to end, folding Durable's setup error
// into the result. Durable reports configuration mistakes (no queue, empty ID)
// separately from execution errors; most tests care about one error value, so
// this keeps them reading as a single call. It is safe to use from a goroutine,
// unlike a helper that would call t.Fatal.
func durableDo(ctx context.Context, c *pace.Client, id, method, path string) (*pace.Response, error) {
	req, err := c.Durable(id)
	if err != nil {
		return nil, err
	}
	return req.Do(ctx, method, path)
}
