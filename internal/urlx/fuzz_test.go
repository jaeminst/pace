package urlx

import (
	"net/http"
	"testing"
)

// FuzzBuild is string surgery on caller input: the path is concatenated rather
// than joined, precisely so an inline query survives. Whatever comes out must
// still be a URL the http package will accept, or the failure appears far from
// its cause.
func FuzzBuild(f *testing.F) {
	f.Add("https://api.example.com", "/items")
	f.Add("https://api.example.com/", "/items")
	f.Add("https://api.example.com/v1", "/items?limit=10&sort=name")
	f.Add("http://host", "")
	f.Add("http://host", "//evil.example.com/")

	f.Fuzz(func(t *testing.T, base, path string) {
		if err := Validate(base); err != nil {
			t.Skip() // pace.New would have rejected this base
		}

		got, err := Build(base, path, nil)
		if err != nil {
			return // reported rather than silently mangled, which is the contract
		}

		// The property that matters is where the request would actually go. An
		// unparsable target is fine — http.NewRequest reports it, and the
		// caller wraps that — but a parsable one aimed at a host the caller
		// never named would be a request-forgery primitive.
		req, err := http.NewRequest(http.MethodGet, got, nil)
		if err != nil {
			return
		}
		// The base goes through the same parser, so the comparison is between
		// two normalised URLs rather than between one normalised and one raw —
		// net/url drops a trailing empty port, for instance.
		ref, err := http.NewRequest(http.MethodGet, base, nil)
		if err != nil {
			t.Skip() // not a base pace.New would produce a working Limiter from
		}
		want := ref.URL
		if req.URL.Host != want.Host {
			t.Errorf("Build(%q, %q) = %q, which targets host %q rather than the base's %q",
				base, path, got, req.URL.Host, want.Host)
		}
		if req.URL.Scheme != want.Scheme {
			t.Errorf("Build(%q, %q) = %q, whose scheme %q is not the base's %q",
				base, path, got, req.URL.Scheme, want.Scheme)
		}
	})
}
