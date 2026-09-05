// httpfree_test.go holds the line the type system used to hold.
//
// This package used to take a config.Spec — ten fields, none of them about
// HTTP — so it was structurally incapable of reading a base URL or a transport.
// It takes the caller's config.Config now, which is far more
// readable at the call site and hands it all fifteen fields, five of which
// describe HTTP and are none of its business:
//
//	BaseURL  Transport  CookieJar  RequestTimeout  MaxResponseBytes
//
// Nothing stops a future edit from reading one. This does.

package limiter_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestTheEngineReadsNoHTTPConfig scans this package's own source for the five
// Config fields that belong to the request path.
//
// A source scan is a blunt instrument and it is the right one here: the property
// is "this identifier appears nowhere", which no type or linter expresses. It
// runs in milliseconds and fails with the file and the field.
func TestTheEngineReadsNoHTTPConfig(t *testing.T) {
	httpOnly := []string{"BaseURL", "Transport", "CookieJar", "RequestTimeout", "MaxResponseBytes"}

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	scanned := 0
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(filepath.Clean(name))
		if err != nil {
			t.Fatal(err)
		}
		scanned++
		for _, line := range strings.Split(string(src), "\n") {
			if strings.HasPrefix(strings.TrimSpace(line), "//") {
				continue // prose may name them; only code may not
			}
			for _, f := range httpOnly {
				if strings.Contains(line, "."+f) {
					t.Errorf("%s reads Config.%s, which belongs to the request path:\n\t%s",
						name, f, strings.TrimSpace(line))
				}
			}
		}
	}
	// A scan that finds no files would pass silently and prove nothing.
	if scanned < 5 {
		t.Fatalf("scanned only %d non-test files; the glob is wrong", scanned)
	}
}
