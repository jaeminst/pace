// cookies_test.go covers Config.CookieJar: that a jar is actually reached, that
// no jar still means no cookies, and the sharing the field's doc promises.

package client_test

import (
	"context"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/jaeminst/pace/bucket"
	"github.com/jaeminst/pace/config"
)

// cookieServer sets a session cookie on the first request it sees and records
// what each later request sent back.
//
// It records the raw header rather than the parsed cookie so that "sent no
// cookie" and "sent an empty one" cannot be confused: the first is the absence
// of a header and the second is a header with nothing in it, and only one of
// them is what a nil jar should produce.
func cookieServer(t *testing.T) (*httptest.Server, func() []string) {
	t.Helper()
	var (
		mu   sync.Mutex
		seen []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, r.Header.Get("Cookie"))
		mu.Unlock()
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc123", Path: "/"})
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), seen...)
	}
}

func jarConfig(t *testing.T, url string, jar http.CookieJar) config.Config {
	t.Helper()
	return config.Config{
		BaseURL:   url,
		Quota:     bucket.NewQuota("6000/m", 100),
		CookieJar: jar,
	}
}

// TestCookieJarStoresAndReplays is the whole feature: pace hands the jar to
// http.Client, which stores what the upstream sets and puts it back on the next
// request. Without the field there was no way to reach Client.Jar at all.
func TestCookieJarStoresAndReplays(t *testing.T) {
	srv, seen := cookieServer(t)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := build(t, jarConfig(t, srv.URL, jar))

	alice := pool.Client("alice")
	for range 2 {
		if _, err := alice.Get(context.Background(), "/"); err != nil {
			t.Fatal(err)
		}
	}

	got := seen()
	if len(got) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(got))
	}
	if got[0] != "" {
		t.Errorf("first request already carried %q; there was nothing to carry yet", got[0])
	}
	if got[1] != "session=abc123" {
		t.Errorf("second request carried %q, want session=abc123", got[1])
	}
}

// TestNoCookieJarSendsNoCookies pins the default. A patch release that started
// storing cookies for callers who never asked would be a behaviour change
// wearing a bug-fix number.
func TestNoCookieJarSendsNoCookies(t *testing.T) {
	srv, seen := cookieServer(t)
	pool := build(t, jarConfig(t, srv.URL, nil))

	alice := pool.Client("alice")
	for range 2 {
		if _, err := alice.Get(context.Background(), "/"); err != nil {
			t.Fatal(err)
		}
	}

	for i, h := range seen() {
		if h != "" {
			t.Errorf("request %d carried %q with no jar configured", i, h)
		}
	}
}

// TestCookieJarIsSharedByEveryKey pins what the field's doc warns about: a Pool
// owns one http.Client, so a cookie set while serving one key goes back out for
// another.
//
// It is here so the warning cannot quietly stop being true. If someone makes
// jars per key, this test fails and the doc has to be rewritten in the same
// commit — which is the point.
func TestCookieJarIsSharedByEveryKey(t *testing.T) {
	srv, seen := cookieServer(t)
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := build(t, jarConfig(t, srv.URL, jar))

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Client("bob").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	got := seen()
	if len(got) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(got))
	}
	if got[1] != "session=abc123" {
		t.Errorf("bob's request carried %q; the doc says the jar is shared, so it should carry alice's cookie", got[1])
	}
}

// TestCookieJarSurvivesARedirect: cookies and redirects interact, and pace does
// not implement either — http.Client.Do does. This asserts pace has not got
// between them, since a request path that used Transport.RoundTrip instead
// would silently drop both.
func TestCookieJarSurvivesARedirect(t *testing.T) {
	var (
		mu   sync.Mutex
		seen string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/start":
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "abc123", Path: "/"})
			http.Redirect(w, r, "/end", http.StatusFound)
		default:
			mu.Lock()
			seen = r.Header.Get("Cookie")
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer srv.Close()

	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := build(t, jarConfig(t, srv.URL, jar))

	resp, err := pool.Client("alice").Get(context.Background(), "/start")
	if err != nil {
		t.Fatal(err)
	}
	if !resp.OK() {
		t.Fatalf("status %d after the redirect, want 2xx", resp.StatusCode())
	}

	mu.Lock()
	defer mu.Unlock()
	if seen != "session=abc123" {
		t.Errorf("the redirected request carried %q, want the cookie set by the first hop", seen)
	}
}
