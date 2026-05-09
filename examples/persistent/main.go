// persistent demonstrates SQLite-backed token persistence across Manager restarts.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"time"

	"github.com/jaeminst/pace"
)

func main() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dbPath := filepath.Join(os.TempDir(), "pace-demo.db")
	defer os.Remove(dbPath)

	cfg := pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {
				BaseURL:       srv.URL,
				RatePerMinute: 6, // 1 token every 10 s
				Burst:         1,
			},
		},
		DBPath: dbPath,
	}

	// --- First Manager instance ---
	mgr1, err := pace.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	if _, err := mgr1.Get(ctx, "alice", "api", "/"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("mgr1: alice consumed her token")
	mgr1.Close() // persists ≈0 tokens to SQLite
	fmt.Printf("mgr1: state saved to %s\n", dbPath)

	// --- Second Manager instance (simulates process restart) ---
	mgr2, err := pace.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer mgr2.Close()

	// Alice should still be throttled — token count was restored from DB.
	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = mgr2.Get(ctxTimeout, "alice", "api", "/")
	if err != nil {
		fmt.Printf("mgr2: alice still throttled after restart → %v\n", err)
	} else {
		fmt.Println("mgr2: alice was NOT throttled (unexpected)")
	}
}
