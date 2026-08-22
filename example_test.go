// example_test.go holds the examples godoc renders under this package's own
// identifiers. One of them used to live in limiter/, where it rendered under
// limiter.Config — a type with no QuotaFor field at all.
package pace_test

import (
	"fmt"
	"log"

	"github.com/jaeminst/pace"
)

// ExampleConfig_quotaFor grades users against a default. An unlisted user gets
// the zero Quota, which selects Config.Rate and Config.Burst — so a map is a
// complete implementation, with no "if missing" branch to write.
func ExampleConfig_quotaFor() {
	tiers := map[string]pace.Quota{
		"acme-corp": {Rate: pace.PerMinute(600), Burst: 50},
		"trial-42":  {Rate: pace.PerMinute(6)}, // Burst falls back to Config.Burst
	}

	lim, err := pace.New(pace.Config{
		BaseURL:  "https://api.example.com",
		Rate:     pace.PerMinute(60), // the default tier
		Burst:    5,
		QuotaFor: func(userID string) pace.Quota { return tiers[userID] },
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
