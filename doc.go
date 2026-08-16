// Package pace provides per-user outbound HTTP rate limiting.
//
// Each user gets an independent token bucket, so one user's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the user count.
//
// Bind a user at creation time via [Config.Name]:
//
//	alice, err := pace.New(pace.Config{
//	    Name:    "alice",
//	    BaseURL: "https://api.example.com",
//	    Rate:    pace.PerMinute(60),
//	})
//	if err != nil { log.Fatal(err) }
//	defer alice.Close()
//
//	resp, err := alice.Get(ctx, "/items/42")
//
// Or derive additional users from the same rate-limiter via [Client.For]:
//
//	bob := alice.For("bob")
//	resp, err = bob.Get(ctx, "/items/99")
package pace
