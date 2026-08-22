package shared

import "testing"

// TestErrorPolicyString: the policy names appear in logs and in a caller's own
// configuration dumps, so they are part of what this package promises. The
// default arm matters most — a value from a future version, or from a caller
// who wrote ErrorPolicy(2) by hand, must render as something rather than as an
// empty string in the middle of a log line.
func TestErrorPolicyString(t *testing.T) {
	for policy, want := range map[ErrorPolicy]string{
		FallbackLocal:   "fallback-local",
		Deny:            "deny",
		Allow:           "allow",
		ErrorPolicy(99): "unknown",
	} {
		if got := policy.String(); got != want {
			t.Errorf("ErrorPolicy(%d).String() = %q, want %q", policy, got, want)
		}
	}
}

// TestErrorPolicyZeroIsFallbackLocal pins the constant a caller gets by leaving
// Config.OnError unset. The zero value has to be the conservative policy: it is
// what every Limiter uses until somebody chooses otherwise, and Allow — which
// admits without limit when the backend is down — is one iota away.
func TestErrorPolicyZeroIsFallbackLocal(t *testing.T) {
	var zero ErrorPolicy
	if zero != FallbackLocal {
		t.Errorf("the zero ErrorPolicy is %v, want FallbackLocal", zero)
	}
	if (Config{}).OnError != FallbackLocal {
		t.Error("the zero Config does not default to FallbackLocal")
	}
}
