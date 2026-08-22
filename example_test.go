// example_test.go holds the example godoc renders under this package's own
// identifier. There is one, because this package declares one thing a caller
// configures — everything else they touch is named in
// github.com/jaeminst/pace/limiter, and the examples for it are there.
package pace_test

import (
	"fmt"
	"log"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/limiter"
)

// ExampleConfig_quotaFor grades users against a default. An unlisted user gets
// the zero Quota, which selects Config.Rate and Config.Burst — so a map is a
// complete implementation, with no "if missing" branch to write.
func ExampleConfig_quotaFor() {
	tiers := map[string]limiter.Quota{
		"acme-corp": {Rate: limiter.PerMinute(600), Burst: 50},
		"trial-42":  {Rate: limiter.PerMinute(6)}, // Burst falls back to Config.Burst
	}

	lim, err := pace.New(pace.Config{
		BaseURL:  "https://api.example.com",
		Rate:     limiter.PerMinute(60), // the default tier
		Burst:    5,
		QuotaFor: func(userID string) limiter.Quota { return tiers[userID] },
	})
	if err != nil {
		log.Fatal(err)
	}
	defer lim.Close()

	for _, id := range []string{"acme-corp", "trial-42", "someone-else"} {
		q := lim.Client(id).Quota()
		fmt.Printf("%s: %v burst %d\n", id, q.Rate, q.Burst)
	}
	// Output:
	// acme-corp: 10/s burst 50
	// trial-42: 6/min burst 5
	// someone-else: 1/s burst 5
}

// ExampleResponse_RetryAfter reads upstream's own statement of its limit, which
// beats any guess a client could make. It is the number this library's readers
// care most about: you throttle outbound requests because upstream limits you,
// and this is upstream saying by how much.
//
// A non-2xx response is not an error in pace — the round-trip succeeded — so
// check [github.com/jaeminst/pace/limiter.Response.OK] and then ask.
