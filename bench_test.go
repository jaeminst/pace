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

func newBenchClient(b *testing.B, srv *httptest.Server, rate, burst int) *pace.Client {
	b.Helper()
	client, err := pace.New(pace.Config{
		BaseURL:       srv.URL,
		RatePerMinute: rate,
		Burst:         burst,
	})
	if err != nil {
		b.Fatal(err)
	}
	return client
}

// BenchmarkClient_Request_HotPath measures the steady-state cost of a request
// from a user whose bucket already exists and has tokens available.
func BenchmarkClient_Request_HotPath(b *testing.B) {
	srv := newBenchServer()
	defer srv.Close()
	client := newBenchClient(b, srv, 1_000_000, b.N+1)
	defer client.Close()
	ctx := context.Background()
	// warm up — ensure shard entry exists
	if _, err := client.Request(ctx, "user-hot"); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := range b.N {
		_ = i
		req, err := client.Request(ctx, "user-hot")
		if err != nil {
			b.Fatal(err)
		}
		// exercise the HTTP round-trip in addition to the rate-limit machinery
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClient_Request_NewUser measures the cold-path cost of first-ever
// request for a user: shard lookup miss + bucket creation under write lock.
func BenchmarkClient_Request_NewUser(b *testing.B) {
	srv := newBenchServer()
	defer srv.Close()
	client := newBenchClient(b, srv, 1_000_000, b.N+1)
	defer client.Close()
	ctx := context.Background()
	b.ResetTimer()
	for i := range b.N {
		userID := fmt.Sprintf("new-user-%d", i)
		req, err := client.Request(ctx, userID)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkClient_ConcurrentUsers_256 measures throughput when 256 goroutines
// each operate on a distinct user ID simultaneously.
func BenchmarkClient_ConcurrentUsers_256(b *testing.B) {
	const goroutines = 256
	srv := newBenchServer()
	defer srv.Close()
	client := newBenchClient(b, srv, 1_000_000, b.N+1)
	defer client.Close()
	ctx := context.Background()
	b.ResetTimer()
	b.SetParallelism(goroutines)
	b.RunParallel(func(pb *testing.PB) {
		userID := fmt.Sprintf("concurrent-user-%p", pb)
		for pb.Next() {
			req, err := client.Request(ctx, userID)
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
