package limiter_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/rate"
)

func TestConcurrentUsers(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6000),
		Burst:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 20
	errs := make(chan error, n)
	ctx := context.Background()
	for i := range n {
		go func(id int) {
			_, err := client.Client(fmt.Sprintf("user-%d", id)).Get(ctx, "/")
			errs <- err
		}(i)
	}
	for range n {
		if err := <-errs; err != nil {
			t.Errorf("concurrent call: %v", err)
		}
	}
}

func TestConcurrentSameUser(t *testing.T) {
	srv := newEchoServer(t)
	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(6000),
		Burst:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _ = client.Client("shared-user").Get(context.Background(), "/")
		}()
	}
	wg.Wait()
}
