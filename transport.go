package pace

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"
)

// TransportConfig holds tuneable knobs for the underlying HTTP transport.
// Pass the result of [NewTransport] to [Config.Transport].
//
// Zero values fall back to the defaults listed in each field's comment.
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
	// response headers after the request is fully sent (including body).
	// Zero disables the timeout. Default: 0 (disabled).
	ResponseHeaderTimeout time.Duration

	// MaxIdleConns is the maximum number of idle (keep-alive) connections
	// across all hosts. Zero uses Go's default (100).
	MaxIdleConns int

	// MaxIdleConnsPerHost is the maximum number of idle connections to keep
	// per-host. Zero uses Go's default (2).
	MaxIdleConnsPerHost int

	// IdleConnTimeout is how long an idle keep-alive connection stays open
	// before being closed. Zero uses Go's default (90s).
	IdleConnTimeout time.Duration

	// TLSConfig is an optional custom TLS configuration.
	// Nil uses the default TLS settings.
	TLSConfig *tls.Config
}

// NewTransport returns an *http.Transport configured from cfg.
// Use it to set connection timeouts, TLS settings, and keep-alive behaviour
// before passing the result to [Config.Transport]:
//
//	client, err := pace.New(pace.Config{
//	    BaseURL:   "https://api.example.com",
//	    Transport: pace.NewTransport(pace.TransportConfig{
//	        DialTimeout:           5 * time.Second,
//	        TLSHandshakeTimeout:   3 * time.Second,
//	        ResponseHeaderTimeout: 10 * time.Second,
//	        KeepAlive:             30 * time.Second,
//	        MaxIdleConnsPerHost:   10,
//	    }),
//	})
func NewTransport(cfg TransportConfig) *http.Transport {
	dialTimeout := cfg.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 30 * time.Second
	}
	keepAlive := cfg.KeepAlive
	if keepAlive == 0 {
		keepAlive = 30 * time.Second
	}
	tlsTimeout := cfg.TLSHandshakeTimeout
	if tlsTimeout <= 0 {
		tlsTimeout = 10 * time.Second
	}
	idleTimeout := cfg.IdleConnTimeout
	if idleTimeout <= 0 {
		idleTimeout = 90 * time.Second
	}
	maxIdle := cfg.MaxIdleConns
	if maxIdle <= 0 {
		maxIdle = 100
	}

	dialer := &net.Dialer{
		Timeout:   dialTimeout,
		KeepAlive: keepAlive,
	}
	return &http.Transport{
		DialContext:           dialer.DialContext,
		TLSHandshakeTimeout:   tlsTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		MaxIdleConns:          maxIdle,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       idleTimeout,
		TLSClientConfig:       cfg.TLSConfig,
	}
}
