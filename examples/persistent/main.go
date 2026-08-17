// persistent demonstrates SQLite-backed token persistence across restarts.
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
	"github.com/jaeminst/pace/limit"
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
		Rate:    limit.PerMinute(6), // 1 token every 10 s
		Burst:   1,
		DBPath:  dbPath,
	}

	// --- First Limiter instance ---
	lim1, err := pace.New(cfg)
	if err != nil {
		srv.Close()
		_ = os.Remove(dbPath)
		log.Fatal(err) //nolint:gocritic // exitAfterDefer: the pending defer is released explicitly on the line above
	}

	ctx := context.Background()
	if _, err := lim1.Client("alice").Get(ctx, "/"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("lim1: alice consumed her token")

	// Close reports whether the flush to SQLite succeeded. With persistence
	// configured that error is worth reading: losing it means losing the token
	// accounting this example is about.
	if err := lim1.Close(); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("lim1: state saved to %s\n", dbPath)

	// --- Second Limiter instance (simulates process restart) ---
	lim2, err := pace.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = lim2.Close() }()

	// Alice should still be throttled — token count was restored from DB.
	ctxTimeout, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	_, err = lim2.Client("alice").Get(ctxTimeout, "/")
	if err != nil {
		fmt.Printf("lim2: alice still throttled after restart → %v\n", err)
	} else {
		fmt.Println("lim2: alice was NOT throttled (unexpected)")
	}
}
