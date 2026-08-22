// Package storetest is the conformance suite for a [store.Store] backend.
//
// pace ships no backend of its own — a Redis or Postgres implementation would
// be a second module to version, tag and support, for a feature whose value
// depends entirely on your infrastructure. What it ships instead is the
// contract, executable:
//
//	func TestMyRedisStore(t *testing.T) {
//	    storetest.Suite(t, func(t *testing.T) store.Store {
//	        return myredis.New(startRedis(t))
//	    })
//	}
//
// The suite asserts the properties pace relies on and cannot check at run time.
// Two of them are easy to get wrong and silent when they are: a miss must not be
// an error, and LastUsed must survive the round trip to the nanosecond, because
// a bucket is restored from it.
//
// A backend that also implements [store.BatchStore] is checked for that too; one
// that does not is skipped rather than failed.
package storetest

import (
	"context"
	"testing"
	"time"

	"github.com/jaeminst/pace/store"
)

// Factory builds a backend for one test. Each call must return a backend with
// no state carried over from a previous one — a fresh Redis database, a fresh
// key prefix, whatever isolation the implementation offers. Registering cleanup
// on t is the usual way.
type Factory func(t *testing.T) store.Store

// SuiteOption tunes a conformance run. None are defined yet; the variadic is
// here so that adding one later is not a signature change, which this package's
// own compatibility promise would forbid after v1.
type SuiteOption func(*suiteConfig)

// suiteConfig is unexported so its fields are not frozen alongside the option
// type. Options are the only way in.
type suiteConfig struct{}

// Suite runs every conformance check against backends built by newStore.
//
// Each check states the property in its failure message, so a failure names the
// guarantee that was broken rather than the assertion that noticed.
func Suite(t *testing.T, newStore Factory, opts ...SuiteOption) {
	t.Helper()
	var cfg suiteConfig
	for _, o := range opts {
		o(&cfg)
	}
	for _, tc := range []struct {
		name string
		fn   func(*testing.T, Factory)
	}{
		{"MissIsNotAnError", missIsNotAnError},
		{"RoundTripsState", roundTripsState},
		{"LastUsedKeepsNanoseconds", lastUsedKeepsNanoseconds},
		{"SaveOverwrites", saveOverwrites},
		{"UsersAreIndependent", usersAreIndependent},
		{"HonoursContextCancellation", honoursContextCancellation},
		{"SaveBatchMatchesSave", saveBatchMatchesSave},
		{"ConcurrentUseIsSafe", concurrentUseIsSafe},
	} {
		t.Run(tc.name, func(t *testing.T) { tc.fn(t, newStore) })
	}
}

// save is Save with the error folded into a fatal.
func save(t *testing.T, s store.Store, userID string, st store.State) {
	t.Helper()
	if err := s.Save(context.Background(), userID, st); err != nil {
		t.Fatalf("Save(%q) = %v, want nil", userID, err)
	}
}

// load is Load with the error folded into a fatal.
func load(t *testing.T, s store.Store, userID string) (store.State, bool) {
	t.Helper()
	st, found, err := s.Load(context.Background(), userID)
	if err != nil {
		t.Fatalf("Load(%q) = %v, want nil", userID, err)
	}
	return st, found
}

// missIsNotAnError: a user nobody has saved reports found == false and no error.
//
// This is the property pace leans on hardest and the one a backend most often
// gets wrong, because sql.ErrNoRows and redis.Nil both want to be returned. A
// store that reports a miss as a failure makes every first-ever request log a
// warning, and pace cannot tell that apart from a backend that is genuinely
// down.
func missIsNotAnError(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	st, found, err := s.Load(context.Background(), "nobody")
	if err != nil {
		t.Fatalf("Load of an unsaved user = %v; a miss is not a failure", err)
	}
	if found {
		t.Fatalf("Load of an unsaved user reported found = true, with %+v", st)
	}
}

// roundTripsState: what Save was given is what Load gives back.
//
// pace rebuilds a token bucket from both fields together — Tokens is how many
// were left, LastUsed is when — so a backend that keeps one and drops the other
// hands back a bucket that was never real.
func roundTripsState(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	want := store.State{Tokens: 2.5, LastUsed: time.Unix(1700000000, 123456789).UTC()}
	save(t, s, "alice", want)

	got, found := load(t, s, "alice")
	if !found {
		t.Fatal("Load did not find a user that was just saved")
	}
	if got.Tokens != want.Tokens {
		t.Errorf("Tokens = %v, want %v", got.Tokens, want.Tokens)
	}
	if !got.LastUsed.Equal(want.LastUsed) {
		t.Errorf("LastUsed = %v, want %v", got.LastUsed, want.LastUsed)
	}
}

// lastUsedKeepsNanoseconds: the timestamp must not be rounded.
//
// It is checked separately from the round trip because truncation is the failure
// that looks like success. A backend storing whole seconds passes every other
// check here, and then pace restores a bucket up to a second stale — which at a
// high rate is the difference between a full burst and none.
func lastUsedKeepsNanoseconds(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	want := time.Unix(1700000000, 987654321).UTC()
	save(t, s, "alice", store.State{Tokens: 1, LastUsed: want})

	got, found := load(t, s, "alice")
	if !found {
		t.Fatal("Load did not find a user that was just saved")
	}
	if !got.LastUsed.Equal(want) {
		t.Fatalf("LastUsed = %v, want %v; the timestamp must round-trip to the nanosecond, "+
			"because pace restores a bucket from it", got.LastUsed.UTC(), want)
	}
}

// saveOverwrites: the second Save wins.
//
// pace saves the same user over and over — on eviction, on every flush — and
// never deletes. A store that inserted rather than upserted would grow without
// bound and then hand back whichever copy it happened to find.
func saveOverwrites(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	at := time.Unix(1700000000, 0).UTC()
	save(t, s, "alice", store.State{Tokens: 5, LastUsed: at})
	save(t, s, "alice", store.State{Tokens: 1, LastUsed: at.Add(time.Second)})

	got, found := load(t, s, "alice")
	if !found {
		t.Fatal("Load did not find a user that was just saved")
	}
	if got.Tokens != 1 {
		t.Errorf("Tokens = %v after re-saving 1, want 1; the later Save must win", got.Tokens)
	}
}

// usersAreIndependent: one user's state must not be visible as another's.
//
// The whole library is per-user isolation. A store that dropped the key would
// have every user reading whoever wrote last.
func usersAreIndependent(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	at := time.Unix(1700000000, 0).UTC()
	save(t, s, "alice", store.State{Tokens: 1, LastUsed: at})
	save(t, s, "bob", store.State{Tokens: 9, LastUsed: at})

	alice, found := load(t, s, "alice")
	if !found {
		t.Fatal("Load did not find alice")
	}
	if alice.Tokens != 1 {
		t.Errorf("alice has %v tokens, want 1; bob's write reached alice's key", alice.Tokens)
	}
}

// honoursContextCancellation: an expired context must not be ignored.
//
// pace bounds every call with StoreTimeout precisely so that a wedged backend
// cannot hold the request path. A store that ignores the context turns that
// timeout into a suggestion.
//
// A purely in-memory store may legitimately finish before it ever looks at the
// context; this checks the returned error rather than the timing, so such a
// store must still report the cancellation to pass.
func honoursContextCancellation(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := s.Save(ctx, "alice", store.State{Tokens: 1, LastUsed: time.Now()}); err == nil {
		t.Error("Save with a cancelled context returned nil; the context must be honoured")
	}
	if _, _, err := s.Load(ctx, "alice"); err == nil {
		t.Error("Load with a cancelled context returned nil; the context must be honoured")
	}
}

// saveBatchMatchesSave: the batch path must land the same state as the single
// one, or the flush at shutdown writes something different from every eviction
// that preceded it.
//
// Skipped for a store that does not implement [store.BatchStore]: the interface
// is discovered by assertion, and not implementing it is a valid choice.
func saveBatchMatchesSave(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	bs, ok := s.(store.BatchStore)
	if !ok {
		t.Skip("backend does not implement store.BatchStore")
	}

	at := time.Unix(1700000000, 424242424).UTC()
	batch := []store.UserState{
		{UserID: "alice", State: store.State{Tokens: 1.5, LastUsed: at}},
		{UserID: "bob", State: store.State{Tokens: 2.5, LastUsed: at}},
	}
	if err := bs.SaveBatch(context.Background(), batch); err != nil {
		t.Fatalf("SaveBatch = %v, want nil", err)
	}

	for _, want := range batch {
		got, found := load(t, s, want.UserID)
		if !found {
			t.Errorf("Load(%q) after SaveBatch found nothing; the batch must write every row",
				want.UserID)
			continue
		}
		if got.Tokens != want.State.Tokens || !got.LastUsed.Equal(want.State.LastUsed) {
			t.Errorf("Load(%q) = %+v after SaveBatch, want %+v; the batch path must land the "+
				"same state as Save", want.UserID, got, want.State)
		}
	}
}

// concurrentUseIsSafe: pace calls a store from the request path and from the GC
// sweep at the same time, holding no lock across either.
//
// This one is here for the race detector rather than for its assertions; run the
// suite with -race or it proves little.
func concurrentUseIsSafe(t *testing.T, newStore Factory) {
	t.Helper()
	s := newStore(t)
	const workers = 16

	at := time.Unix(1700000000, 0).UTC()
	start := make(chan struct{})
	done := make(chan error, workers)
	for i := range workers {
		go func() {
			<-start
			id := string(rune('a' + i%26))
			st := store.State{Tokens: float64(i), LastUsed: at}
			if err := s.Save(context.Background(), id, st); err != nil {
				done <- err
				return
			}
			_, _, err := s.Load(context.Background(), id)
			done <- err
		}()
	}
	close(start)
	for range workers {
		if err := <-done; err != nil {
			t.Fatalf("concurrent Save/Load = %v, want nil", err)
		}
	}
}
