package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/jaeminst/pace/internal/pace"
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
			_, err := pace.New(pace.Config{BaseURL: tt.baseURL, Rate: pace.PerMinute(60)})
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

// TestPathWithInlineQueryIsPreserved is why the path is concatenated rather
// than joined with url.URL.JoinPath: JoinPath escapes its argument, so a query
// string written inline would arrive as part of the path.
func TestPathWithInlineQueryIsPreserved(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	if _, err := lim.Client("alice").Get(context.Background(), "/items?limit=10&sort=name"); err != nil {
		t.Fatal(err)
	}
	if want := "/items?limit=10&sort=name"; got() != want {
		t.Errorf("server received %q, want %q", got(), want)
	}
}

func TestBaseURLPathIsPrefixed(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL+"/v1")

	if _, err := lim.Client("alice").Get(context.Background(), "/items"); err != nil {
		t.Fatal(err)
	}
	if want := "/v1/items"; got() != want {
		t.Errorf("server received %q, want %q", got(), want)
	}
}

// TestTrailingSlashDoesNotDoubleUp: concatenation's one visibly wrong case.
func TestTrailingSlashDoesNotDoubleUp(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL+"/")

	if _, err := lim.Client("alice").Get(context.Background(), "/items"); err != nil {
		t.Fatal(err)
	}
	if want := "/items"; got() != want {
		t.Errorf("server received %q, want %q", got(), want)
	}
}

func TestSetQueryEscapesValues(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	_, err := lim.Client("alice").Request().
		SetQuery("q", "hello world & more").
		Get(context.Background(), "/search")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.ParseRequestURI(got())
	if err != nil {
		t.Fatalf("the server received an unparsable URI %q: %v", got(), err)
	}
	if v := parsed.Query().Get("q"); v != "hello world & more" {
		t.Errorf("q = %q, want the original string round-tripped", v)
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

// TestSetQueryMergesWithInlineQuery: parameters set on the builder join the
// ones already in the path rather than replacing them.
func TestSetQueryMergesWithInlineQuery(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	_, err := lim.Client("alice").Request().
		SetQuery("page", "2").
		Get(context.Background(), "/items?limit=10")
	if err != nil {
		t.Fatal(err)
	}
	parsed, _ := url.ParseRequestURI(got())
	q := parsed.Query()
	if q.Get("limit") != "10" || q.Get("page") != "2" {
		t.Errorf("query = %v, want both limit=10 and page=2", q)
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

// TestRelativePathCannotRetargetTheHost is the regression guard for a
// request-forgery primitive that fuzzing found. A path not starting with "/"
// used to be concatenated straight onto the base, so against a base with no
// path of its own it ran into the host: "https://api.example.com" plus
// ".evil.com/x" is a request to a host the caller never named. With any part of
// the path coming from user input, that is exploitable.
func TestRelativePathCannotRetargetTheHost(t *testing.T) {
	srv, got := urlEcho(t)
	lim, _ := newTestLimiterOn(t, srv.URL)

	// A relative path is joined, not run into the authority.
	if _, err := lim.Client("alice").Get(context.Background(), "items"); err != nil {
		t.Fatal(err)
	}
	if want := "/items"; got() != want {
		t.Errorf("server received %q, want %q", got(), want)
	}

	// The shape an attacker would reach for. It must stay a path.
	if _, err := lim.Client("alice").Get(context.Background(), ".evil.example.com/steal"); err != nil {
		t.Fatal(err)
	}
	if want := "/.evil.example.com/steal"; got() != want {
		t.Errorf("server received %q, want %q — the request must not leave the base host", got(), want)
	}
}

// TestBaseURLWithoutAHostnameIsRejected: "http://:" and "http://:8080" both
// have a non-empty url.URL.Host and no hostname at all, so the original check
// let them through and produced a Limiter whose every request went nowhere.
func TestBaseURLWithoutAHostnameIsRejected(t *testing.T) {
	for _, base := range []string{"http://:", "http://:8080"} {
		_, err := pace.New(pace.Config{BaseURL: base, Rate: pace.PerMinute(60)})
		var ce *pace.ConfigError
		if !errors.As(err, &ce) || ce.Field != "BaseURL" {
			t.Errorf("New(%q) = %v, want a ConfigError on BaseURL", base, err)
		}
	}
}
