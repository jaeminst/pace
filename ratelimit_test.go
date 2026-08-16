package pace_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

func TestTokens_ExistingUser(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// consume one token
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	tokens := tokensOf(client.Client("alice"))
	if tokens >= 3 {
		t.Fatalf("expected tokens < 3 after one request, got %v", tokens)
	}
}

func TestTokens_UnknownUser(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:0",
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	n, ok := client.Client("nobody").Tokens()
	if ok || n != 0 {
		t.Fatalf("Tokens() for an unseen user = (%v, %v), want (0, false)", n, ok)
	}
}

func TestBurstCeiling(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// First request: consumes the only burst token
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// Second request: no token, should block; use tight timeout
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err = client.Client("alice").Get(ctx, "/")
	if err == nil {
		t.Fatal("expected second request to block/fail with burst=1")
	}
}

func TestThrottledHook_CalledWhenBlocked(t *testing.T) {
	srv := newEchoServer(t)
	var called atomic.Int32
	client, err := pace.New(pace.Config{
		BaseURL:  srv.URL,
		Rate:     pace.PerMinute(60),
		Burst:    1,
		Observer: &pace.Observer{Throttled: func(context.Context, pace.ThrottleInfo) { called.Add(1) }},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Exhaust the burst token
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	// No token is available, so this request must report as throttled.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = client.Client("alice").Get(ctx, "/")

	if called.Load() == 0 {
		t.Fatal("the Throttled hook was not called for a request that had to wait")
	}
}

func TestThrottledHook_NotCalledWhenAvailable(t *testing.T) {
	srv := newEchoServer(t)
	var called atomic.Int32
	client, err := pace.New(pace.Config{
		BaseURL:  srv.URL,
		Rate:     pace.PerMinute(60),
		Burst:    5,
		Observer: &pace.Observer{Throttled: func(context.Context, pace.ThrottleInfo) { called.Add(1) }},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// A token is available, so nothing should report as throttled.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if called.Load() != 0 {
		t.Fatalf("the Throttled hook fired %d times for a request that had a token", called.Load())
	}
}
