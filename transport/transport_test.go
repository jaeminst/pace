package transport

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func defaultTransport(t *testing.T) *http.Transport {
	t.Helper()
	tr, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		t.Fatalf("http.DefaultTransport is %T, not *http.Transport", http.DefaultTransport)
	}
	return tr
}

func TestNewTransportKeepsProxySupport(t *testing.T) {
	if defaultTransport(t).Proxy == nil {
		t.Skip("http.DefaultTransport has no Proxy; nothing to preserve")
	}
	if New(Config{}).Proxy == nil {
		t.Error("NewTransport drops the environment proxy that http.DefaultTransport honours")
	}
}

func TestNewTransportProxyCanBeOverridden(t *testing.T) {
	want := &url.URL{Scheme: "http", Host: "proxy.example:8080"}
	tr := New(Config{
		Proxy: func(*http.Request) (*url.URL, error) { return want, nil },
	})
	got, err := tr.Proxy(&http.Request{URL: &url.URL{Scheme: "https", Host: "api.example"}})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Host != want.Host {
		t.Errorf("Proxy returned %v, want %v", got, want)
	}
}

func TestNewTransportProxyCanBeDisabled(t *testing.T) {
	tr := New(Config{
		Proxy: func(*http.Request) (*url.URL, error) { return nil, nil },
	})
	got, err := tr.Proxy(&http.Request{URL: &url.URL{Scheme: "https", Host: "api.example"}})
	if err != nil || got != nil {
		t.Errorf("Proxy returned (%v, %v), want (nil, nil)", got, err)
	}
}

// TestNewTransportAttemptsHTTP2WithTLSConfig covers the second regression.
// Setting TLSClientConfig on an http.Transport turns off automatic HTTP/2
// unless ForceAttemptHTTP2 is set, so the documented mutual-TLS setup was
// silently downgrading to HTTP/1.1.
func TestNewTransportAttemptsHTTP2WithTLSConfig(t *testing.T) {
	tr := New(Config{
		TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	})
	if !tr.ForceAttemptHTTP2 {
		t.Error("HTTP/2 is disabled when TLSConfig is set; the mTLS setup silently downgrades to HTTP/1.1")
	}
}

func TestNewTransportHTTP2CanBeDisabled(t *testing.T) {
	tr := New(Config{DisableHTTP2: true})
	if tr.ForceAttemptHTTP2 {
		t.Error("DisableHTTP2 did not take effect")
	}
}

func TestNewTransportDefaults(t *testing.T) {
	tr := New(Config{})
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"TLSHandshakeTimeout", tr.TLSHandshakeTimeout, 10 * time.Second},
		{"IdleConnTimeout", tr.IdleConnTimeout, 90 * time.Second},
		{"MaxIdleConns", tr.MaxIdleConns, 100},
		{"ExpectContinueTimeout", tr.ExpectContinueTimeout, time.Second},
		{"ForceAttemptHTTP2", tr.ForceAttemptHTTP2, true},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	// A server that accepts a connection and then never answers should not
	// hold a request open forever.
	if tr.ResponseHeaderTimeout == 0 {
		t.Error("ResponseHeaderTimeout is disabled by default; a black-holed server holds the request open indefinitely")
	}
}

func TestNewTransportCustomValues(t *testing.T) {
	cfg := Config{
		DialTimeout:           2 * time.Second,
		KeepAlive:             15 * time.Second,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
		ExpectContinueTimeout: 5 * time.Second,
		MaxIdleConns:          7,
		MaxIdleConnsPerHost:   3,
		MaxConnsPerHost:       9,
		IdleConnTimeout:       6 * time.Second,
		DisableCompression:    true,
	}
	tr := New(cfg)
	checks := []struct {
		name string
		got  any
		want any
	}{
		{"TLSHandshakeTimeout", tr.TLSHandshakeTimeout, cfg.TLSHandshakeTimeout},
		{"ResponseHeaderTimeout", tr.ResponseHeaderTimeout, cfg.ResponseHeaderTimeout},
		{"ExpectContinueTimeout", tr.ExpectContinueTimeout, cfg.ExpectContinueTimeout},
		{"MaxIdleConns", tr.MaxIdleConns, cfg.MaxIdleConns},
		{"MaxIdleConnsPerHost", tr.MaxIdleConnsPerHost, cfg.MaxIdleConnsPerHost},
		{"MaxConnsPerHost", tr.MaxConnsPerHost, cfg.MaxConnsPerHost},
		{"IdleConnTimeout", tr.IdleConnTimeout, cfg.IdleConnTimeout},
		{"DisableCompression", tr.DisableCompression, cfg.DisableCompression},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}
