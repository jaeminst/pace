package pace_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jaeminst/pace"
)

// benchRate is high enough that no benchmark ever waits for a token:
// 1e9/min yields a 60ns refill interval and a burst no b.Loop() run exhausts.
var benchRate = pace.PerMinute(benchBurst)

// benchBurst is large enough that no b.Loop() run drains it.
const benchBurst = 1_000_000_000

func newBenchServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func newBenchClient(b *testing.B, baseURL string, rate pace.Limit, burst int) *pace.Client {
	b.Helper()
	client, err := pace.New(pace.Config{
		BaseURL: baseURL,
		Rate:    rate,
		Burst:   burst,
	})
	if err != nil {
		b.Fatal(err)
	}
	return client
}

// stubTransport answers every request from memory, with no socket involved.
type stubTransport struct{}

func (stubTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{},
		Body:       io.NopCloser(strings.NewReader("")),
		Request:    req,
	}, nil
}

// BenchmarkRequest_NoHTTP measures the full pace request path — shard lookup,
// token acquisition, URL build, response buffering — with the network stubbed
// out. This is the number to track over time; the _E2E benchmarks below are
// dominated by loopback TCP and mostly measure the kernel.
func BenchmarkRequest_NoHTTP(b *testing.B) {
	client, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      benchRate,
		Burst:     benchBurst,
		Transport: stubTransport{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	hot := client.For("user-hot")
	if _, err := hot.Request(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		req, err := hot.Request(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCaller_Request_HotPath_E2E measures the steady-state cost of a
// request from a user whose bucket already exists and has tokens available,
// including a real HTTP round-trip over loopback.
func BenchmarkCaller_Request_HotPath_E2E(b *testing.B) {
	srv := newBenchServer()
	defer srv.Close()
	client := newBenchClient(b, srv.URL, benchRate, benchBurst)
	defer client.Close()
	ctx := context.Background()
	hot := client.For("user-hot")
	// warm up — ensure shard entry exists
	if _, err := hot.Request(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		req, err := hot.Request(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCaller_Request_NewUser_E2E measures the cold-path cost of a
// first-ever request for a user: shard lookup miss + bucket creation under the
// write lock, plus a real HTTP round-trip.
func BenchmarkCaller_Request_NewUser_E2E(b *testing.B) {
	srv := newBenchServer()
	defer srv.Close()
	client := newBenchClient(b, srv.URL, benchRate, benchBurst)
	defer client.Close()
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		req, err := client.For(fmt.Sprintf("new-user-%d", i)).Request(ctx)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := req.Get("/"); err != nil {
			b.Fatal(err)
		}
		i++
	}
}

// BenchmarkConcurrentUsers_256 measures throughput when 256 goroutines each
// operate on a distinct user ID simultaneously. What this is meant to expose is
// shard-lock contention, so it deliberately stubs the network out: pointing 256
// goroutines at an httptest server measures the host's TCP accept backlog
// instead, and overflows it outright on Windows.
func BenchmarkConcurrentUsers_256(b *testing.B) {
	const goroutines = 256
	client, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      benchRate,
		Burst:     benchBurst,
		Transport: stubTransport{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.SetParallelism(goroutines)
	b.RunParallel(func(pb *testing.PB) {
		caller := client.For(fmt.Sprintf("concurrent-user-%p", pb))
		for pb.Next() {
			req, err := caller.Request(ctx)
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
