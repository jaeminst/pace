// example_test.go holds the example godoc renders under this package's own
// identifier. There is one, because this package declares one thing a caller
// configures — everything else they touch is named in
// github.com/jaeminst/pace/limiter, and the examples for it are there.
package config_test

import (
	"fmt"
	"log"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
)

// ExampleConfig grades keys into tiers. Config.Quota is the rate, written down
// where Resolve can check it; [WithQuotaFor] is handed that value as its
// default, so an unlisted key needs no rule about which of the two wins.
func ExampleConfig() {
	tiers := map[string]bucket.Quota{
		"acme-corp": {Rate: bucket.PerMinute(600), Burst: 50},
		"trial-42":  {Rate: bucket.PerMinute(6), Burst: 5},
	}

	lim, err := client.New(config.Config{
		BaseURL: "https://api.example.com",
		Quota:   bucket.Quota{Rate: bucket.PerMinute(60), Burst: 5},
	}, config.WithQuotaFor(func(key string, def bucket.Quota) bucket.Quota {
		if q, ok := tiers[key]; ok {
			return q
		}
		return def
	}))
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
// check [github.com/jaeminst/pace/client.Response.OK] and then ask.
