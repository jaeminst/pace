package limiter_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/rate"
	"github.com/jaeminst/pace/store/memory"
)

// benchRate is high enough that no benchmark ever waits for a token:
// 1e9/min yields a 60ns refill interval and a burst no b.Loop() run exhausts.
var benchRate = rate.PerMinute(benchBurst)

// benchBurst is large enough that no b.Loop() run drains it.
const benchBurst = 1_000_000_000

func newBenchServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
}

func newBenchLimiter(b *testing.B, baseURL string, rate rate.Limit, burst int) *pace.Limiter {
	b.Helper()
	lim, err := pace.New(pace.Config{
		BaseURL: baseURL,
		Rate:    rate,
		Burst:   burst,
	})
	if err != nil {
		b.Fatal(err)
	}
	return lim
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
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      benchRate,
		Burst:     benchBurst,
		Transport: stubTransport{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer lim.Close()

	ctx := context.Background()
	hot := lim.Client("user-hot")
	if err := hot.Wait(ctx); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if _, err := hot.Request().Get(ctx, "/"); err != nil {
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
	lim := newBenchLimiter(b, srv.URL, benchRate, benchBurst)
	defer lim.Close()
	ctx := context.Background()
	hot := lim.Client("user-hot")
	// warm up — ensure shard entry exists
	if err := hot.Wait(ctx); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := hot.Request().Get(ctx, "/"); err != nil {
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
	lim := newBenchLimiter(b, srv.URL, benchRate, benchBurst)
	defer lim.Close()
	ctx := context.Background()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		if _, err := lim.Client(fmt.Sprintf("new-user-%d", i)).Get(ctx, "/"); err != nil {
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
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      benchRate,
		Burst:     benchBurst,
		Transport: stubTransport{},
	})
	if err != nil {
		b.Fatal(err)
	}
	defer lim.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.SetParallelism(goroutines)
	b.RunParallel(func(pb *testing.PB) {
		caller := lim.Client(fmt.Sprintf("concurrent-user-%p", pb))
		for pb.Next() {
			if _, err := caller.Request().Get(ctx, "/"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

// BenchmarkSweepWithStore measures the idle-user sweep with persistence
// configured: a real Limiter, a store, and the Limiter's own flush — batching,
// chunking and the BatchStore assertion included.
//
// It lives here rather than beside the registry's own sweep benchmark because
// the registry does not own persistence. Supplying it a hand-written Flush
// there would measure that adapter rather than the one callers get.
//
// The store is in-memory, so what this measures is pace's flush path rather
// than any backend's write latency — which is the part pace is responsible
// for. A number from here is not comparable with one taken against a database.
func BenchmarkSweepWithStore(b *testing.B) {
	const users = 2_000

	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    benchRate,
		Burst:   benchBurst,
		// Long enough that the ticker never fires during the benchmark; the
		// sweep under test is the one CollectIdle drives.
		GCInterval: time.Hour,
		IdleExpiry: time.Nanosecond,
		Store:      memory.New(),
		Logger:     slog.New(slog.DiscardHandler),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = lim.Close() })

	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		for i := range users {
			lim.Client(fmt.Sprintf("sweep-user-%d", i)).Allow(ctx)
		}
		// IdleExpiry is a nanosecond, so anything created above is already
		// expired by the time the sweep reads the clock.
		b.StartTimer()
		pace.CollectIdle(lim)
	}
}
