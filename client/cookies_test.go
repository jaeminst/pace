// cookies_test.go covers cookies at both scopes: Config.CookieJar, one jar for
// the whole Pool, and config.WithCookieJarFor, which scopes them to a key. The
// tests assert on the raw Cookie header, so "sent none" and "sent the wrong
// key's" cannot be confused.

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

// TestCookieJarIsSharedByEveryKey pins what the field's doc warns about:
// without a config.WithCookieJarFor hook, a Pool owns one http.Client, so a
// cookie set while serving one key goes back out for another.
//
// It is here so the warning cannot quietly stop being true. Per-key jars exist
// — that is the hook — but they are opt-in; if this default ever changes, this
// test fails and the doc has to be rewritten in the same commit.
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

// perKeyJars is a WithCookieJarFor hook over a fixed table: read-only after
// construction, so it is trivially safe for the request goroutines that call
// it. A key not in the table gets whatever the test put in `absent`.
func perKeyJars(t *testing.T, keys []string, absent func(def http.CookieJar) http.CookieJar) func(string, http.CookieJar) http.CookieJar {
	t.Helper()
	jars := make(map[string]http.CookieJar, len(keys))
	for _, k := range keys {
		j, err := cookiejar.New(nil)
		if err != nil {
			t.Fatal(err)
		}
		jars[k] = j
	}
	return func(key string, def http.CookieJar) http.CookieJar {
		if j, ok := jars[key]; ok {
			return j
		}
		return absent(def)
	}
}

// TestCookieJarForIsolatesKeys is the feature: with the hook, a cookie the
// upstream sets while serving one key is never replayed for another — each
// key's second request carries its own session, and its first carries nothing.
func TestCookieJarForIsolatesKeys(t *testing.T) {
	srv, seen := cookieServer(t)
	pool := buildWith(t, jarConfig(t, srv.URL, nil),
		config.WithCookieJarFor(perKeyJars(t, []string{"alice", "bob"}, func(def http.CookieJar) http.CookieJar { return def })))

	// alice acquires a session; bob's first request must not inherit it.
	for _, key := range []string{"alice", "bob", "alice", "bob"} {
		if _, err := pool.Client(key).Get(context.Background(), "/"); err != nil {
			t.Fatal(err)
		}
	}

	got := seen()
	want := []string{"", "", "session=abc123", "session=abc123"}
	if len(got) != len(want) {
		t.Fatalf("server saw %d requests, want %d", len(got), len(want))
	}
	if got[1] != "" {
		t.Errorf("bob's first request carried %q — alice's session leaked across keys", got[1])
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("request %d carried %q, want %q", i, got[i], want[i])
		}
	}
}

// TestCookieJarForIsHandedTheDefault: the def argument is Config.CookieJar, so
// a hook that returns it reproduces the shared behaviour exactly. This is what
// makes the hook the only cookie decision — there is no precedence rule,
// because the value being overridden arrives as an argument.
func TestCookieJarForIsHandedTheDefault(t *testing.T) {
	srv, seen := cookieServer(t)
	shared, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	pool := buildWith(t, jarConfig(t, srv.URL, shared),
		config.WithCookieJarFor(func(_ string, def http.CookieJar) http.CookieJar {
			if def != http.CookieJar(shared) {
				t.Errorf("def is not Config.CookieJar")
			}
			return def
		}))

	if _, err := pool.Client("alice").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Client("bob").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}

	if got := seen(); got[1] != "session=abc123" {
		t.Errorf("bob's request carried %q; returning def should share alice's session", got[1])
	}
}

// TestCookieJarForNilMeansNoCookies: nil from the hook is "no cookies for this
// key" — the same meaning a nil Config.CookieJar has for the Pool — while other
// keys keep theirs.
func TestCookieJarForNilMeansNoCookies(t *testing.T) {
	srv, seen := cookieServer(t)
	pool := buildWith(t, jarConfig(t, srv.URL, nil),
		config.WithCookieJarFor(perKeyJars(t, []string{"alice"}, func(http.CookieJar) http.CookieJar { return nil })))

	for _, key := range []string{"alice", "anon", "alice", "anon"} {
		if _, err := pool.Client(key).Get(context.Background(), "/"); err != nil {
			t.Fatal(err)
		}
	}

	got := seen()
	if got[2] != "session=abc123" {
		t.Errorf("alice's second request carried %q, want her session", got[2])
	}
	for _, i := range []int{1, 3} {
		if got[i] != "" {
			t.Errorf("anon's request %d carried %q; a nil jar stores nothing", i, got[i])
		}
	}
}

// TestCookieJarForAppliesToStream: Stream bypasses RequestTimeout and
// MaxResponseBytes by design, so it is worth pinning that it does not bypass
// the jar — a streamed request is still a request, and it goes out on the same
// per-key client.
func TestCookieJarForAppliesToStream(t *testing.T) {
	srv, seen := cookieServer(t)
	pool := buildWith(t, jarConfig(t, srv.URL, nil),
		config.WithCookieJarFor(perKeyJars(t, []string{"alice"}, func(def http.CookieJar) http.CookieJar { return def })))

	for range 2 {
		resp, err := pool.Client("alice").Request().Stream(context.Background(), http.MethodGet, "/")
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatal(err)
		}
	}

	got := seen()
	if len(got) != 2 {
		t.Fatalf("server saw %d requests, want 2", len(got))
	}
	if got[1] != "session=abc123" {
		t.Errorf("the second streamed request carried %q, want the session from the first", got[1])
	}
}
