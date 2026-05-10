package pace_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jaeminst/pace"
)

func newBenchServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func newBenchManager(b *testing.B, srv *httptest.Server, rate, burst int) *pace.Manager {
	b.Helper()
	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.Endpoint{
			"api": {BaseURL: srv.URL, RatePerMinute: rate, Burst: burst},
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	return mgr
}

// BenchmarkManager_Request_HotPath measures the steady-state cost of a request
// from a user whose bucket already exists and has tokens available.
func BenchmarkManager_Request_HotPath(b *testing.B) {
	srv := newBenchServer()
	defer srv.Close()
	mgr := newBenchManager(b, srv, 1_000_000, b.N+1)
	defer mgr.Close()
	ctx := context.Background()
	// warm up — ensure shard entry exists
	if _, err := mgr.Request(ctx, "user-hot", "api"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := range b.N {
		_ = i
		req, err := mgr.Request(ctx, "user-hot", "api")
		if err != nil {
			b.Fatal(err)
		}
		// exercise the HTTP round-trip in addition to the rate-limit machinery
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkManager_Request_NewUser measures the cold-path cost of first-ever
// request for a user: shard lookup miss + bucket creation under write lock.
func BenchmarkManager_Request_NewUser(b *testing.B) {
	srv := newBenchServer()
	defer srv.Close()
	mgr := newBenchManager(b, srv, 1_000_000, b.N+1)
	defer mgr.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := range b.N {
		userID := fmt.Sprintf("new-user-%d", i)
		req, err := mgr.Request(ctx, userID, "api")
		if err != nil {
			b.Fatal(err)
		}
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkManager_ConcurrentUsers_256 measures throughput when 256 goroutines
// each operate on a distinct user ID simultaneously.
func BenchmarkManager_ConcurrentUsers_256(b *testing.B) {
	const goroutines = 256
	srv := newBenchServer()
	defer srv.Close()
	mgr := newBenchManager(b, srv, 1_000_000, b.N+1)
	defer mgr.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.SetParallelism(goroutines)
	b.RunParallel(func(pb *testing.PB) {
		userID := fmt.Sprintf("concurrent-user-%p", pb)
		for pb.Next() {
			req, err := mgr.Request(ctx, userID, "api")
			if err != nil {
				b.Error(err)
				return
			}
			if _, err := req.Get("/"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
