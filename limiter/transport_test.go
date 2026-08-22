package limiter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/transport"
)

func TestNewTransportUsableWithClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limiter.PerMinute(6000),
		Transport: transport.New(transport.Config{
			DialTimeout:         2 * time.Second,
			TLSHandshakeTimeout: 2 * time.Second,
			MaxIdleConnsPerHost: 4,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()

	resp, err := lim.Client("u").Get(context.Background(), "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode())
	}
}
