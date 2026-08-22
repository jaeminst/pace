package urlx

import (
	"net/url"
	"strings"
	"testing"
)

// TestBuildJoinsTheSeam covers the three ways base and path can meet. Two of
// them need fixing up and the third must be left alone, and getting any of them
// wrong is visible in every request the Limiter sends.
func TestBuildJoinsTheSeam(t *testing.T) {
	tests := []struct {
		name string
		base string
		path string
		want string
	}{
		{"neither has a slash", "https://api.example.com", "items", "https://api.example.com/items"},
		{"path has one", "https://api.example.com", "/items", "https://api.example.com/items"},
		{"base has one", "https://api.example.com/", "items", "https://api.example.com/items"},
		{"both have one", "https://api.example.com/", "/items", "https://api.example.com/items"},
		{"base carries a path", "https://api.example.com/v1", "/items", "https://api.example.com/v1/items"},
		{"base path and no slash", "https://api.example.com/v1", "items", "https://api.example.com/v1/items"},
		{"empty path", "https://api.example.com", "", "https://api.example.com/"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Build(tt.base, tt.path, nil)
			if err != nil {
				t.Fatalf("Build(%q, %q) = %v, want nil", tt.base, tt.path, err)
			}
			if got != tt.want {
				t.Errorf("Build(%q, %q) = %q, want %q", tt.base, tt.path, got, tt.want)
			}
		})
	}
}

// TestBuildKeepsAnInlineQuery is why the path is concatenated rather than
// joined with url.URL.JoinPath: JoinPath escapes its argument, so "?" would
// arrive as "%3F" and the whole query would become part of the path.
func TestBuildKeepsAnInlineQuery(t *testing.T) {
	got, err := Build("https://api.example.com", "/items?limit=10&sort=name", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := "https://api.example.com/items?limit=10&sort=name"
	if got != want {
		t.Fatalf("Build = %q, want %q; an inline query must not be escaped into the path", got, want)
	}
}

// TestBuildDoesNotLetAPathRetargetTheHost is the regression guard for a
// request-forgery primitive that fuzzing found. A path not starting with "/"
// used to be concatenated straight onto the base, so against a base with no
// path of its own it ran into the authority: "https://api.example.com" plus
// ".evil.com/x" is a request to a host the caller never named. With any part of
// the path coming from key input, that is exploitable.
func TestBuildDoesNotLetAPathRetargetTheHost(t *testing.T) {
	const base = "https://api.example.com"
	for _, path := range []string{".evil.example.com/steal", "@evil.example.com/steal", "evil.example.com"} {
		got, err := Build(base, path, nil)
		if err != nil {
			t.Fatalf("Build(%q, %q) = %v", base, path, err)
		}
		u, err := url.Parse(got)
		if err != nil {
			t.Fatalf("Build produced an unparsable URL %q: %v", got, err)
		}
		if u.Host != "api.example.com" {
			t.Errorf("Build(%q, %q) = %q, whose host is %q; the request must not leave the base host",
				base, path, got, u.Host)
		}
	}
}

// TestBuildMergesExtraQuery: values supplied separately join whatever the path
// already carried rather than replacing it. The Limiter's builder passes them
// here, so a replace would silently drop parameters a caller wrote inline.
func TestBuildMergesExtraQuery(t *testing.T) {
	got, err := Build("https://api.example.com", "/items?limit=10", url.Values{"page": {"2"}})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	if q.Get("limit") != "10" {
		t.Errorf("limit = %q, want 10; the inline query was dropped", q.Get("limit"))
	}
	if q.Get("page") != "2" {
		t.Errorf("page = %q, want 2", q.Get("page"))
	}
}

// TestBuildEscapesExtraQuery: whatever a caller supplied comes back byte for
// byte after the round trip through Encode.
func TestBuildEscapesExtraQuery(t *testing.T) {
	const raw = "hello world & more=x?y#z"
	got, err := Build("https://api.example.com", "/search", url.Values{"q": {raw}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(got, " ") {
		t.Errorf("Build = %q, which contains a raw space", got)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatalf("Build produced an unparsable URL %q: %v", got, err)
	}
	if v := u.Query().Get("q"); v != raw {
		t.Errorf("q = %q, want the original string round-tripped", v)
	}
}

// TestBuildKeepsEveryValueOfAKey: Encode must not collapse a repeated key.
func TestBuildKeepsEveryValueOfAKey(t *testing.T) {
	got, err := Build("https://api.example.com", "/items", url.Values{"tag": {"red", "blue"}})
	if err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(got)
	if err != nil {
		t.Fatal(err)
	}
	if tags := u.Query()["tag"]; len(tags) != 2 || tags[0] != "red" || tags[1] != "blue" {
		t.Errorf("tag = %q, want both values in order", tags)
	}
}

// TestBuildReportsAnUnparsableResult: the extra-query path has to re-parse what
// concatenation produced, and that parse can fail on a base no caller should
// have got past Validate. It must report rather than return a half-built URL.
func TestBuildReportsAnUnparsableResult(t *testing.T) {
	got, err := Build("http://%zz", "/items", url.Values{"a": {"1"}})
	if err == nil {
		t.Fatalf("Build = %q, want an error for an unparsable base", got)
	}
	if got != "" {
		t.Errorf("Build returned %q alongside an error, want the empty string", got)
	}
}

// TestBuildSkipsTheParseWhenThereIsNothingToMerge: with no extra values the
// concatenated string is returned as it is, which is what keeps an inline query
// unescaped — url.Parse followed by String would normalise it.
func TestBuildSkipsTheParseWhenThereIsNothingToMerge(t *testing.T) {
	for _, extra := range []url.Values{nil, {}} {
		got, err := Build("http://%zz", "/items", extra)
		if err != nil {
			t.Fatalf("Build with %d extra values = %v, want nil: the parse must be skipped", len(extra), err)
		}
		if want := "http://%zz/items"; got != want {
			t.Errorf("Build = %q, want %q", got, want)
		}
	}
}

// TestValidateAcceptsWhatPaceCanSend.
func TestValidateAcceptsWhatPaceCanSend(t *testing.T) {
	for _, base := range []string{
		"http://api.example.com",
		"https://api.example.com",
		"https://api.example.com/v1",
		"http://127.0.0.1:8080",
		"https://key:pass@api.example.com",
	} {
		if err := Validate(base); err != nil {
			t.Errorf("Validate(%q) = %v, want nil", base, err)
		}
	}
}

// TestValidateRejectsWithAReason checks each refusal separately, because the
// message is what a caller sees at startup and a single "invalid URL" would
// tell them nothing about which rule they broke.
func TestValidateRejectsWithAReason(t *testing.T) {
	tests := []struct {
		name string
		base string
		want string
	}{
		{"unparsable", "http://%zz", "invalid URL escape"},
		{"relative", "/api/v1", "must be absolute"},
		{"no scheme", "api.example.com", "must be absolute"},
		{"unsupported scheme", "ftp://files.example.com", "unsupported scheme"},
		{"no host", "http://", "missing host"},
		// "http://:8080" has a non-empty url.URL.Host and no hostname at all.
		// The original check read Host and let it through, producing a Limiter
		// whose every request went nowhere. Found by fuzzing Build.
		{"port but no hostname", "http://:8080", "missing host"},
		{"colon but no hostname", "http://:", "missing host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := Validate(tt.base)
			if err == nil {
				t.Fatalf("Validate(%q) = nil, want an error", tt.base)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Validate(%q) = %q, want it to mention %q", tt.base, err, tt.want)
			}
		})
	}
}
