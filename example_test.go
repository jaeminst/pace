package pace_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/jaeminst/pace"
)

func ExampleClient_Get() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   10,
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic // exitAfterDefer: the pending defer is released explicitly on the line above
	}
	defer func() { _ = lim.Close() }()

	resp, err := lim.Client("user-123").Get(context.Background(), "/items/42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %d\n", resp.StatusCode())
	// Output:
	// status: 200
}

func ExampleClient_Request() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60),
		Burst:   10,
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic // exitAfterDefer: the pending defer is released explicitly on the line above
	}
	defer func() { _ = lim.Close() }()

	// Building the request costs nothing; the rate-limit token is taken when
	// Post runs, so an abandoned builder does not burn the user's quota.
	resp, err := lim.Client("user-456").Request().
		SetHeader("X-Request-ID", "req-001").
		SetBody([]byte(`{"action":"create"}`)).
		Post(context.Background(), "/resources")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %d, request-id: %s\n", resp.StatusCode(), resp.Header().Get("X-Request-ID"))
	// Output:
	// status: 201, request-id: req-001
}
