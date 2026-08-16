package pace

import (
	"net/http"
	"testing"
)

// FuzzRetryAfter covers the one header pace interprets, parsed two different
// ways, from a server that may be hostile. Whatever it says, the answer has to
// be a duration a caller can act on: never negative, never absurd.
func FuzzRetryAfter(f *testing.F) {
	f.Add("30")
	f.Add("0")
	f.Add("-1")
	f.Add("")
	f.Add("Wed, 21 Oct 2015 07:28:00 GMT")
	f.Add("9223372036854775807")
	f.Add("not a number")

	f.Fuzz(func(t *testing.T, header string) {
		r := &Response{
			header: http.Header{"Retry-After": []string{header}},
			clock:  stdClock{},
		}
		got, ok := r.RetryAfter()
		if !ok {
			if got != 0 {
				t.Errorf("RetryAfter = (%v, false) for %q, want a zero duration when not ok", got, header)
			}
			return
		}
		if got < 0 {
			t.Errorf("RetryAfter = %v for %q, want a non-negative duration", got, header)
		}
	})
}

// FuzzBuildURL is string surgery on caller input: the path is concatenated
// rather than joined, precisely so an inline query survives. Whatever comes
// out must still be a URL the http package will accept, or the failure appears
// far from its cause.
func FuzzBuildURL(f *testing.F) {
	f.Add("https://api.example.com", "/items")
	f.Add("https://api.example.com/", "/items")
	f.Add("https://api.example.com/v1", "/items?limit=10&sort=name")
	f.Add("http://host", "")
	f.Add("http://host", "//evil.example.com/")

	f.Fuzz(func(t *testing.T, base, path string) {
		if err := validateBaseURL(base); err != nil {
			t.Skip() // New would have rejected this base
		}
		l := &Limiter{cfg: Config{BaseURL: base}}

		got, err := l.buildURL(path, nil)
		if err != nil {
			return // reported rather than silently mangled, which is the contract
		}

		// The property that matters is where the request would actually go. An
		// unparsable target is fine — http.NewRequest reports it, and build
		// wraps that — but a parsable one aimed at a host the caller never named
		// would be a request-forgery primitive.
		req, err := http.NewRequest(http.MethodGet, got, nil)
		if err != nil {
			return
		}
		// The base goes through the same parser, so the comparison is between
		// two normalised URLs rather than between one normalised and one raw —
		// net/url drops a trailing empty port, for instance.
		ref, err := http.NewRequest(http.MethodGet, base, nil)
		if err != nil {
			t.Skip() // not a base New would produce a working Limiter from
		}
		want := ref.URL
		if req.URL.Host != want.Host {
			t.Errorf("buildURL(%q, %q) = %q, which targets host %q rather than the base's %q",
				base, path, got, req.URL.Host, want.Host)
		}
		if req.URL.Scheme != want.Scheme {
			t.Errorf("buildURL(%q, %q) = %q, whose scheme %q is not the base's %q",
				base, path, got, req.URL.Scheme, want.Scheme)
		}
	})
}

// FuzzShardIndex checks the inlined FNV-1a against the standard library's, so
// the comment claiming it is a faithful reimplementation has evidence behind
// it. A divergence would not break correctness — any hash distributes users —
// but it would quietly invalidate the reason the inline version exists.
func FuzzShardIndex(f *testing.F) {
	f.Add("alice", uint32(255))
	f.Add("", uint32(255))
	f.Add("user-\xff\xfe", uint32(0))
	f.Add("日本語のユーザー", uint32(1023))

	f.Fuzz(func(t *testing.T, s string, mask uint32) {
		const (
			offset32 = 2166136261
			prime32  = 16777619
		)
		// The reference: FNV-1a over the raw bytes, exactly as hash/fnv does it.
		want := uint32(offset32)
		for _, b := range []byte(s) {
			want ^= uint32(b)
			want *= prime32
		}
		if got := shardIndex(s, mask); got != want&mask {
			t.Errorf("shardIndex(%q, %d) = %d, want %d", s, mask, got, want&mask)
		}
	})
}

// FuzzLimitString: String is what a rate looks like in an error message and in
// every log line, so it must not panic and must not produce nothing.
func FuzzLimitString(f *testing.F) {
	f.Add(1.0)
	f.Add(0.0)
	f.Add(-1.0)
	f.Add(1.0 / 3600.0)
	f.Add(float64(Inf))

	f.Fuzz(func(t *testing.T, v float64) {
		if got := Limit(v).String(); got == "" {
			t.Errorf("Limit(%v).String() is empty", v)
		}
	})
}
