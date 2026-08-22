package quotatest_test

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/shared"
	"github.com/jaeminst/pace/shared/quotatest"
)

// The suite's whole value is failing a bad backend, and until this file existed
// it had only ever been run against a good one. A Suite whose check bodies
// were all `return` would have been indistinguishable from the real thing.
//
// Each case below is a backend broken in exactly one way a plausible
// implementation gets wrong, asserted to fail. A check that stops catching its
// own break has quietly become decoration.
//
// It found one immediately: HonoursContextCancellation used to wait for Take to
// return and assert nothing about what it returned, so a backend that ignored
// the context entirely passed as long as it was fast.

// breaks maps a name to the one-line deviation that produces it.
var breaks = map[string]func(*gcra){
	"grants beyond the burst":                func(g *gcra) { g.unlimited = true },
	"consumes on refusal":                    func(g *gcra) { g.chargeRefusals = true },
	"ignores the namespace":                  func(g *gcra) { g.ignoreNamespace = true },
	"ignores the context":                    func(g *gcra) { g.ignoreContext = true },
	"reports a RetryAfter that is too short": func(g *gcra) { g.shortRetryAfter = true },
	"leaks one key's spend into another":     func(g *gcra) { g.oneBucket = true },
}

// brokenEnv names the backend a re-executed child should run the suite against.
const brokenEnv = "PACETEST_BROKEN_BACKEND"

func TestSuiteAcceptsACorrectBackend(t *testing.T) {
	quotatest.Suite(t, func(*testing.T) shared.Backend { return newGCRA(nil) })
}

// TestSuiteRejectsBrokenBackends asserts the suite fails each break.
//
// It re-executes this test binary rather than calling Suite inline: a
// failing sub-test fails its parent no matter what the parent does with the
// result, so "assert that a test fails" needs a separate process. That is the
// standard Go answer to this, and the exit status is the assertion.
func TestSuiteRejectsBrokenBackends(t *testing.T) {
	if name := os.Getenv(brokenEnv); name != "" {
		brk, ok := breaks[name]
		if !ok {
			t.Fatalf("unknown break %q", name)
		}
		quotatest.Suite(t, func(*testing.T) shared.Backend { return newGCRA(brk) })
		return
	}

	for name := range breaks {
		t.Run(name, func(t *testing.T) {
			cmd := exec.Command(os.Args[0], "-test.run=^TestSuiteRejectsBrokenBackends$", "-test.timeout=120s")
			cmd.Env = append(os.Environ(), brokenEnv+"="+name)
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Errorf("Suite passed a backend that %s.\nSuite output:\n%s", name, out)
			}
		})
	}
}

// gcra is the correct in-memory backend, with a switch for each way of being
// wrong. One type rather than six, so every break reads as a one-line deviation
// from a working implementation rather than as an unrelated fake.
type gcra struct {
	mu     sync.Mutex
	tokens map[string]float64
	seen   map[string]time.Time

	unlimited       bool
	chargeRefusals  bool
	ignoreNamespace bool
	ignoreContext   bool
	shortRetryAfter bool
	oneBucket       bool
}

func newGCRA(brk func(*gcra)) *gcra {
	g := &gcra{tokens: map[string]float64{}, seen: map[string]time.Time{}}
	if brk != nil {
		brk(g)
	}
	return g
}

func (g *gcra) Take(ctx context.Context, r shared.TakeRequest) (shared.Grant, error) {
	if !g.ignoreContext {
		if err := ctx.Err(); err != nil {
			return shared.Grant{}, err
		}
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	if g.unlimited {
		return shared.Grant{OK: true}, nil
	}

	key := r.Namespace + "\x00" + r.Key
	switch {
	case g.oneBucket:
		key = "everyone"
	case g.ignoreNamespace:
		key = r.Key
	}

	now := time.Now()
	perSec := r.Rate
	burst := float64(r.Burst)
	if last, ok := g.seen[key]; ok {
		g.tokens[key] = min(burst, g.tokens[key]+now.Sub(last).Seconds()*perSec)
	} else {
		g.tokens[key] = burst
	}
	g.seen[key] = now

	want := float64(r.Tokens)
	if g.tokens[key] < want {
		if g.chargeRefusals {
			g.tokens[key] -= want
		}
		var after time.Duration
		if perSec > 0 {
			after = time.Duration((want - g.tokens[key]) / perSec * float64(time.Second))
			if g.shortRetryAfter {
				after /= 1000
			}
		}
		left := g.tokens[key]
		return shared.Grant{RetryAfter: after, Tokens: &left}, nil
	}
	g.tokens[key] -= want
	left := g.tokens[key]
	return shared.Grant{OK: true, Tokens: &left}, nil
}
