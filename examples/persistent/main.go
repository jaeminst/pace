// persistent demonstrates SQLite-backed token persistence across Client restarts.
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
	defer os.Remove(dbPath) //nolint:errcheck // best-effort cleanup of a demo temp file

	cfg := pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6), // 1 token every 10 s
		Burst:   1,
		DBPath:  dbPath,
	}

	// --- First Client instance ---
	client1, err := pace.New(cfg)
	if err != nil {
		srv.Close()
		_ = os.Remove(dbPath)
		log.Fatal(err) //nolint:gocritic // exitAfterDefer: the pending defer is released explicitly on the line above
	}

	ctx := context.Background()
	if _, err := client1.For("alice").Get(ctx, "/"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("client1: alice consumed her token")
	client1.Close() // persists ≈0 tokens to SQLite
	fmt.Printf("client1: state saved to %s\n", dbPath)

	// --- Second Client instance (simulates process restart) ---
	client2, err := pace.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client2.Close()

	// Alice should still be throttled — token count was restored from DB.
	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = client2.For("alice").Get(ctxTimeout, "/")
	if err != nil {
		fmt.Printf("client2: alice still throttled after restart → %v\n", err)
	} else {
		fmt.Println("client2: alice was NOT throttled (unexpected)")
	}
}
