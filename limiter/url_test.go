package limiter_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jaeminst/pace"
	"github.com/jaeminst/pace/rate"
)

// urlEcho reports back the exact target the server received.
func urlEcho(t *testing.T) (*httptest.Server, func() string) {
	t.Helper()
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.URL.RequestURI()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() string { return got }
}

func TestBaseURLIsValidatedAtNew(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		wantErr bool
	}{
		{"plain http", "http://api.example.com", false},
		{"https with path", "https://api.example.com/v1", false},
		{"with port", "http://127.0.0.1:8080", false},
		{"relative", "/api/v1", true},
		{"no scheme", "api.example.com", true},
		{"unsupported scheme", "ftp://files.example.com", true},
		{"no host", "http://", true},
		{"unparsable", "http://%zz", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := pace.New(pace.Config{BaseURL: tt.baseURL, Rate: rate.PerMinute(60)})
			if tt.wantErr {
				var ce *pace.ConfigError
				if !errors.As(err, &ce) || ce.Field != "BaseURL" {
					t.Fatalf("New(%q) = %v, want a ConfigError on BaseURL", tt.baseURL, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("New(%q) = %v, want nil", tt.baseURL, err)
			}
		})
	}
}

func TestAddQueryKeepsMultipleValues(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	_, err := lim.Client("alice").Request().
		AddQuery("tag", "red").
		AddQuery("tag", "blue").
		Get(context.Background(), "/items")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.ParseRequestURI(got())
	tags := parsed.Query()["tag"]
	if len(tags) != 2 || tags[0] != "red" || tags[1] != "blue" {
		t.Errorf("tag = %q, want both values", tags)
	}
}

func TestSetQueryValuesReplacesWholesale(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	_, err := lim.Client("alice").Request().
		SetQuery("dropped", "yes").
		SetQueryValues(url.Values{"kept": {"1"}}).
		Get(context.Background(), "/items")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.ParseRequestURI(got())
	q := parsed.Query()
	if q.Has("dropped") {
		t.Error("SetQueryValues did not replace the earlier parameters")
	}
	if q.Get("kept") != "1" {
		t.Errorf("query = %v, want kept=1", q)
	}
}

// TestBaseURLWithoutAHostnameIsRejected: "http://:" and "http://:8080" both
// have a non-empty url.URL.Host and no hostname at all, so the original check
// let them through and produced a Limiter whose every request went nowhere.
func TestBaseURLWithoutAHostnameIsRejected(t *testing.T) {
	for _, base := range []string{"http://:", "http://:8080"} {
		_, err := pace.New(pace.Config{BaseURL: base, Rate: rate.PerMinute(60)})
		var ce *pace.ConfigError
		if !errors.As(err, &ce) || ce.Field != "BaseURL" {
			t.Errorf("New(%q) = %v, want a ConfigError on BaseURL", base, err)
		}
	}
}
