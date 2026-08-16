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

	client, err := pace.New(pace.Config{
		BaseURL:    srv.URL,
		Rate:       pace.PerMinute(2), // 2 req/min → 1 token every 30s
		Burst:      2,                 // allow 2 back-to-back requests
		IdleExpiry: 5 * time.Minute,
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic // exitAfterDefer: the pending defer is released explicitly on the line above
	}
	defer client.Close()

	ctx := context.Background()

	// Alice and Bob each get independent buckets.
	for _, name := range []string{"alice", "bob"} {
		resp, err := client.For(name).Get(ctx, "/hello")
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		fmt.Printf("%s → %d\n", name, resp.StatusCode())
	}

	alice := client.For("alice")

	// Alice's second request uses the burst allowance.
	resp, err := alice.Get(ctx, "/hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("alice (burst) → %d\n", resp.StatusCode())

	// Alice is now throttled. Time out quickly to show the block.
	ctxTimeout, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	_, err = alice.Get(ctxTimeout, "/hello")
	if err != nil {
		fmt.Printf("alice (throttled) → %v\n", err)
	}

	// Bob still has his own tokens — unaffected by Alice.
	resp, err = client.For("bob").Get(ctx, "/hello")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("bob (unaffected) → %d\n", resp.StatusCode())
}
