package pace

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/url"
	"time"
)

// TransportConfig holds tuneable knobs for the underlying HTTP transport.
// Pass the result of [NewTransport] to [Config.Transport].
//
// Zero values fall back to the defaults listed in each field's comment, chosen
// so that the zero TransportConfig behaves like [http.DefaultTransport] rather
// than like a bare [http.Transport].
type TransportConfig struct {
	// DialTimeout is the maximum time allowed to establish a TCP connection.
	// Default: 30s.
	DialTimeout time.Duration

	// KeepAlive is the interval between TCP keep-alive probes.
	// Set to -1 to disable keep-alives entirely.
	// Default: 30s.
	KeepAlive time.Duration

	// TLSHandshakeTimeout is the maximum time allowed to complete a TLS
	// handshake. Default: 10s.
	TLSHandshakeTimeout time.Duration

	// ResponseHeaderTimeout is the maximum time to wait for a server's
	// response headers after the request is fully sent. Default: 30s.
	//
	// Set it to -1 to wait indefinitely. Leaving it on catches a server that
	// accepts the connection and then never answers, which no other timeout
	// here covers; it does not limit how long a slow body may take to arrive.
	ResponseHeaderTimeout time.Duration

	// ExpectContinueTimeout is how long to wait for a 100 Continue before
	// sending the request body. Default: 1s.
	ExpectContinueTimeout time.Duration

	// MaxIdleConns is the maximum number of idle (keep-alive) connections
	// across all hosts. Zero uses Go's default (100).
	MaxIdleConns int

	// MaxIdleConnsPerHost is the maximum number of idle connections to keep
	// per-host. Zero uses Go's default (2).
	MaxIdleConnsPerHost int

	// MaxConnsPerHost caps total connections per host, idle or in use.
	// Zero means no limit.
	MaxConnsPerHost int

	// IdleConnTimeout is how long an idle keep-alive connection stays open
	// before being closed. Zero uses Go's default (90s).
	IdleConnTimeout time.Duration

	// Proxy selects a proxy for a request. Nil defaults to
	// [http.ProxyFromEnvironment], matching [http.DefaultTransport]; supply a
	// function returning (nil, nil) to bypass proxies entirely.
	//
	// A bare http.Transport has no proxy support, so leaving this unset used to
	// mean that reaching for NewTransport to change one timeout silently
	// dropped HTTP_PROXY, HTTPS_PROXY, and NO_PROXY.
	Proxy func(*http.Request) (*url.URL, error)

	// TLSConfig is an optional custom TLS configuration.
	// Nil uses the default TLS settings.
	TLSConfig *tls.Config

	// DisableHTTP2 turns off automatic HTTP/2.
	//
	// HTTP/2 is attempted by default, including when TLSConfig is set. That
	// exception is the point: net/http disables automatic HTTP/2 as soon as a
	// transport carries a custom TLSClientConfig, so a mutual-TLS setup would
	// otherwise downgrade to HTTP/1.1 without saying so.
	DisableHTTP2 bool

	// DisableCompression turns off transparent gzip.
	DisableCompression bool
}

// NewTransport returns an *http.Transport configured from cfg.
// Use it to set connection timeouts, TLS settings, and keep-alive behaviour
// before passing the result to [Config.Transport]:
//
//	lim, err := pace.New(pace.Config{
//	    BaseURL: "https://api.example.com",
//	    Rate:    pace.PerMinute(60),
//	    Transport: pace.NewTransport(pace.TransportConfig{
//	        DialTimeout:         5 * time.Second,
//	        TLSHandshakeTimeout: 3 * time.Second,
//	        MaxIdleConnsPerHost: 10,
//	    }),
//	})
func NewTransport(cfg TransportConfig) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   orDefault(cfg.DialTimeout, 30*time.Second),
		KeepAlive: orDefaultAllowingNegative(cfg.KeepAlive, 30*time.Second),
	}
	proxy := cfg.Proxy
	if proxy == nil {
		proxy = http.ProxyFromEnvironment
	}
	return &http.Transport{
		Proxy:                 proxy,
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   orDefault(cfg.TLSHandshakeTimeout, 10*time.Second),
		ResponseHeaderTimeout: disableable(cfg.ResponseHeaderTimeout, 30*time.Second),
		ExpectContinueTimeout: disableable(cfg.ExpectContinueTimeout, time.Second),
		MaxIdleConns:          orDefaultInt(cfg.MaxIdleConns, 100),
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		IdleConnTimeout:       orDefault(cfg.IdleConnTimeout, 90*time.Second),
		TLSClientConfig:       cfg.TLSConfig,
		ForceAttemptHTTP2:     !cfg.DisableHTTP2,
		DisableCompression:    cfg.DisableCompression,
	}
}

func orDefault(d, fallback time.Duration) time.Duration {
	if d <= 0 {
		return fallback
	}
	return d
}

func orDefaultInt(n, fallback int) int {
	if n <= 0 {
		return fallback
	}
	return n
}

// orDefaultAllowingNegative treats a negative value as "off", which is how
// net.Dialer.KeepAlive spells disabled.
func orDefaultAllowingNegative(d, fallback time.Duration) time.Duration {
	if d == 0 {
		return fallback
	}
	return d
}

// disableable treats a negative value as "no timeout", so a caller can turn off
// a default that is otherwise on.
func disableable(d, fallback time.Duration) time.Duration {
	switch {
	case d < 0:
		return 0
	case d == 0:
		return fallback
	default:
		return d
	}
}
