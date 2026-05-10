package pace_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/jaeminst/pace"
)

func ExampleCaller_Get() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL:       srv.URL,
		RatePerMinute: 60,
		Burst:         10,
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic
	}
	defer client.Close()

	resp, err := client.For("user-123").Get(context.Background(), "/items/42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %d\n", resp.StatusCode())
	// Output:
	// status: 200
}

func ExampleCaller_Request() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	client, err := pace.New(pace.Config{
		BaseURL:       srv.URL,
		RatePerMinute: 60,
		Burst:         10,
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic
	}
	defer client.Close()

	req, err := client.For("user-456").Request(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	resp, err := req.
		SetHeader("X-Request-ID", "req-001").
		SetBody([]byte(`{"action":"create"}`)).
		Post("/resources")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %d, request-id: %s\n", resp.StatusCode(), resp.Header().Get("X-Request-ID"))
	// Output:
	// status: 201, request-id: req-001
}
