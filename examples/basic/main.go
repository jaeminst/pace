// basic demonstrates per-user rate limiting with pace.
// Run it against a local echo server or any real HTTP endpoint.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"time"

	"github.com/jaeminst/pace"
)

func main() {
	// Stand up a minimal echo server for the demo.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintln(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.Endpoint{
			"api": {
				BaseURL:       srv.URL,
				RatePerMinute: 2, // 2 req/min → 1 token every 30s
				Burst:         2, // allow 2 back-to-back requests
			},
		},
		IdleExpiry: 5 * time.Minute,
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic
	}
	defer mgr.Close()

	ctx := context.Background()

	// Alice and Bob each get independent buckets.
	for _, user := range []string{"alice", "bob"} {
		resp, err := mgr.Get(ctx, user, "api", "/hello")
		if err != nil {
			log.Fatalf("%s: %v", user, err)
		}
		fmt.Printf("%s → %d\n", user, resp.StatusCode())
	}

	// Alice's second request uses the burst allowance.
	resp, err := mgr.Get(ctx, "alice", "api", "/hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice (burst) → %d\n", resp.StatusCode())

	// Alice is now throttled. Time out quickly to show the block.
	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = mgr.Get(ctxTimeout, "alice", "api", "/hello")
	if err != nil {
		fmt.Printf("alice (throttled) → %v\n", err)
	}

	// Bob still has his own tokens — unaffected by Alice.
	resp, err = mgr.Get(ctx, "bob", "api", "/hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob (unaffected) → %d\n", resp.StatusCode())
}
