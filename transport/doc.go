// Package transport tunes the HTTP connection behaviour behind a Limiter.
//
// [New] returns an *http.Transport built from a [Config], for
// github.com/jaeminst/pace.Config.Transport. It exists because the zero
// http.Transport is not http.DefaultTransport: reaching for a bare one to set a
// single timeout silently drops the environment proxy and HTTP/2. Every field
// here documents the default it falls back to, and those defaults are
// DefaultTransport's.
package transport
