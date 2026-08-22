package storetest_test

import (
	"context"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/jaeminst/pace/store"
	"github.com/jaeminst/pace/store/storetest"
)

// The suite's whole value is failing a bad backend, and a Suite whose check
// bodies were all `return` would be indistinguishable from the real thing until
// somebody wrote a wrong store and watched it pass.
//
// Each case below is a backend broken in exactly one way a plausible
// implementation gets wrong, asserted to fail. A check that stops catching its
// own break has quietly become decoration.
//
// This mirrors shared/quotatest/quotatest_test.go, deliberately: the two
// contracts are checked the same way so that a reader who has understood one
// has understood both.

// breaks maps a name to the one-line deviation that produces it.
var breaks = map[string]func(*mapStore){
	"reports a miss as an error":         func(m *mapStore) { m.missIsError = true },
	"truncates LastUsed to seconds":      func(m *mapStore) { m.truncate = true },
	"ignores the context":                func(m *mapStore) { m.ignoreContext = true },
	"keeps the first Save, not the last": func(m *mapStore) { m.writeOnce = true },
	"shares one key across keys":         func(m *mapStore) { m.oneKey = true },
	"drops rows from SaveBatch":          func(m *mapStore) { m.batchKeepsFirst = true },
}

// brokenEnv names the backend a re-executed child should run the suite against.
const brokenEnv = "PACETEST_BROKEN_STORE"

func TestSuiteAcceptsACorrectBackend(t *testing.T) {
	storetest.Suite(t, func(*testing.T) store.Store { return newMapStore(nil) })
}

// TestSuiteRejectsBrokenBackends asserts the suite fails each break.
//
// It re-executes this test binary rather than calling Suite inline: a failing
// sub-test fails its parent no matter what the parent does with the result, so
// "assert that a test fails" needs a separate process. The exit status is the
// assertion.
func TestSuiteRejectsBrokenBackends(t *testing.T) {
	if name := os.Getenv(brokenEnv); name != "" {
		brk, ok := breaks[name]
		if !ok {
			t.Fatalf("unknown break %q", name)
		}
		storetest.Suite(t, func(*testing.T) store.Store { return newMapStore(brk) })
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

// mapStore is the correct in-memory backend, with a switch for each way of
// being wrong. One type rather than six, so every break reads as a one-line
// deviation from a working implementation rather than as an unrelated fake.
//
// It is not store/memory: that one is the shipped reference implementation and
// has no business carrying test switches.
type mapStore struct {
	mu    sync.Mutex
	state map[string]store.State

	missIsError     bool
	truncate        bool
	ignoreContext   bool
	writeOnce       bool
	oneKey          bool
	batchKeepsFirst bool
}

var (
	_ store.Store      = (*mapStore)(nil)
	_ store.BatchStore = (*mapStore)(nil)
)

func newMapStore(brk func(*mapStore)) *mapStore {
	m := &mapStore{state: make(map[string]store.State)}
	if brk != nil {
		brk(m)
	}
	return m
}

// errMissing stands in for the sql.ErrNoRows a broken backend returns.
type errMissing struct{}

func (errMissing) Error() string { return "no rows" }

func (m *mapStore) key(key string) string {
	if m.oneKey {
		return ""
	}
	return key
}

func (m *mapStore) ctxErr(ctx context.Context) error {
	if m.ignoreContext {
		return nil
	}
	return ctx.Err()
}

func (m *mapStore) Save(ctx context.Context, key string, st store.State) error {
	if err := m.ctxErr(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.put(key, st)
	return nil
}

func (m *mapStore) SaveBatch(ctx context.Context, states []store.KeyState) error {
	if err := m.ctxErr(ctx); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for i, u := range states {
		if m.batchKeepsFirst && i > 0 {
			break
		}
		m.put(u.Key, u.State)
	}
	return nil
}

// put holds the two write-side deviations, so Save and SaveBatch cannot drift.
func (m *mapStore) put(key string, st store.State) {
	mapKey := m.key(key)
	if m.writeOnce {
		if _, exists := m.state[mapKey]; exists {
			return
		}
	}
	if m.truncate {
		st.LastUsed = st.LastUsed.Truncate(time.Second)
	}
	m.state[mapKey] = st
}

func (m *mapStore) Load(ctx context.Context, key string) (store.State, bool, error) {
	if err := m.ctxErr(ctx); err != nil {
		return store.State{}, false, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	st, ok := m.state[m.key(key)]
	if !ok && m.missIsError {
		return store.State{}, false, errMissing{}
	}
	return st, ok, nil
}
