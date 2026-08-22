package limiter_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
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
