package pace

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// validateBaseURL rejects a base that cannot produce a usable request URL.
//
// Checking it at New turns a typo into one clear error at startup, instead of
// an opaque failure from http.NewRequest on every call afterwards.
func validateBaseURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if !u.IsAbs() {
		return errors.New("must be absolute, including a scheme")
	}
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("unsupported scheme %q, want http or https", u.Scheme)
	}
	// Hostname rather than Host: "http://:" and "http://:8080" both have a
	// non-empty Host and no hostname at all, and a base like that produces
	// requests aimed at nothing while passing every other check. Found by
	// fuzzing buildURL.
	if u.Hostname() == "" {
		return errors.New("missing host")
	}
	return nil
}

// buildURL joins path onto the base and merges any query values set on the
// request.
//
// The path is concatenated rather than resolved with url.URL.JoinPath, which
// would percent-encode a query string written inline — "/items?limit=10" is
// common and would become "/items%3Flimit=10". What concatenation gets wrong is
// the seam, in both directions, so both are normalised.
//
// The missing slash is the one that matters. Against a base with no path of its
// own, a path that does not start with "/" runs straight into the host:
// "https://api.example.com" + ".evil.com/x" is a request to a host the caller
// never named. With any part of the path coming from user input that is a
// request-forgery primitive, so a separator is inserted rather than trusted to
// be there. Found by fuzzing.
func (l *Limiter) buildURL(path string, extra url.Values) (string, error) {
	base := l.cfg.BaseURL
	var full string
	switch {
	case strings.HasSuffix(base, "/") && strings.HasPrefix(path, "/"):
		full = base + path[1:]
	case !strings.HasSuffix(base, "/") && !strings.HasPrefix(path, "/"):
		full = base + "/" + path
	default:
		full = base + path
	}
	if len(extra) == 0 {
		return full, nil
	}
	u, err := url.Parse(full)
	if err != nil {
		return "", fmt.Errorf("pace: build request: %w", err)
	}
	q := u.Query()
	for k, vs := range extra {
		for _, v := range vs {
			q.Add(k, v)
		}
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
