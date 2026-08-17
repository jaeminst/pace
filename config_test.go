package pace_test

import (
	"context"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

func TestNew_ZeroConfig(t *testing.T) {
	_, err := pace.New(pace.Config{})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
	}
}

func TestNew_ZeroRate(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    pace.PerMinute(0),
	})
	if err == nil {
		t.Fatal("want error for zero Rate")
	}
}

func TestNew_EmptyBaseURL(t *testing.T) {
	_, err := pace.New(pace.Config{
		BaseURL: "",
		Rate:    pace.PerMinute(60),
	})
	if err == nil {
		t.Fatal("want error for empty BaseURL")
	}
}

func TestNew_StoreOpenFailure(t *testing.T) {
	// Point DBPath at a directory that doesn't exist to make store.OpenStore fail.
	_, err := pace.New(pace.Config{
		BaseURL: "http://x",
		Rate:    pace.PerMinute(60),
		DBPath:  "/nonexistent/directory/pace.db",
	})
	if err == nil {
		t.Fatal("expected error when store cannot be opened")
	}
}

func TestNew_CustomStore_NoopLoad(t *testing.T) {
	// Config.Store with a no-op backend: userFor calls Load on it.
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
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
		Rate:    pace.PerMinute(60),
		Burst:   3,
		Store: &loadStateStore{state: pace.State{
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
