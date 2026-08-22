package limiter_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jaeminst/pace/bucket"

	"github.com/jaeminst/pace/client"
	"github.com/jaeminst/pace/config"
)

func TestConcurrentUsers(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()

	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(6000),
		Burst:   10,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	const n = 20
	errs := make(chan error, n)
	ctx := context.Background()
	for i := range n {
		go func(id int) {
			_, err := pool.Client(fmt.Sprintf("user-%d", id)).Get(ctx, "/")
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
	pool, err := client.New(config.Config{
		BaseURL: srv.URL,
		Rate:    bucket.PerMinute(6000),
		Burst:   100,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	const n = 10
	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			_, _ = pool.Client("shared-user").Get(context.Background(), "/")
		}()
	}
	wg.Wait()
}
