// Package pace provides per-user outbound HTTP rate limiting.
//
// Each user gets an independent token bucket, so one user's traffic never
// affects another's quota. A single background goroutine handles idle-user GC;
// the number of goroutines does not grow with the user count.
//
//	client, err := pace.New(pace.Config{
//	    BaseURL:       "https://api.example.com",
//	    RatePerMinute: 60,
//	})
//	if err != nil { log.Fatal(err) }
//	defer client.Close()
//
//	resp, err := client.Get(ctx, "user-123", "/items/42")
package pace
