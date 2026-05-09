// Package pace provides per-user, per-endpoint outbound HTTP rate limiting.
//
// Each user gets an independent token bucket per endpoint, so one user's traffic
// never affects another's quota. A single background goroutine handles idle-user
// GC; the number of goroutines does not grow with the user count.
//
//	mgr, err := pace.New(pace.Config{
//	    Endpoints: map[string]pace.EndpointConfig{
//	        "api": {BaseURL: "https://api.example.com", RatePerMinute: 60},
//	    },
//	})
//	if err != nil { log.Fatal(err) }
//	defer mgr.Close()
//
//	resp, err := mgr.Get(ctx, "user-123", "api", "/items/42")
package pace
