package limiter_test

import (
	"context"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/store"
)

func TestNew_CustomStore_NoopLoad(t *testing.T) {
	// Config.Store with a no-op backend: userFor calls Load on it.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limiter.PerMinute(6000),
		Burst:   5,
		Store:   &noopStore{},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}

func TestNew_CustomStore_WithSavedState(t *testing.T) {
	// Config.Store returns saved state so the wrapper.Load conversion path runs.
	srv := newEchoServer(t)
	defer srv.Close()

	now := time.Now()
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limiter.PerMinute(60),
		Burst:   3,
		Store: &loadStateStore{state: store.State{
			Tokens: 1.5, LastUsed: now,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// User is loaded from the custom store — should have tokens available.
	if _, err := client.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
}
