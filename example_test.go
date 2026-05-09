package pace_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"

	"github.com/jaeminst/pace"
)

func ExampleManager_Get() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintln(w, `{"status":"ok"}`)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 10},
		},
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic
	}
	defer mgr.Close()

	resp, err := mgr.Get(context.Background(), "user-123", "api", "/items/42")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("status: %d\n", resp.StatusCode())
	// Output:
	// status: 200
}

func ExampleManager_Request() {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Request-ID", r.Header.Get("X-Request-ID"))
		w.WriteHeader(http.StatusCreated)
	}))
	defer srv.Close()

	mgr, err := pace.New(pace.Config{
		Endpoints: map[string]pace.EndpointConfig{
			"api": {BaseURL: srv.URL, RatePerMinute: 60, Burst: 10},
		},
	})
	if err != nil {
		srv.Close()
		log.Fatal(err) //nolint:gocritic
	}
	defer mgr.Close()

	req, err := mgr.Request(context.Background(), "user-456", "api")
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
