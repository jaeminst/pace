package pace_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace/internal/pace"
)

// flakyTransport fails a fixed number of times before succeeding, with no
// socket involved.
type flakyTransport struct {
	mu           sync.Mutex
	failuresLeft int
	served       *atomic.Int64
}

func (f *flakyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.served.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failuresLeft > 0 {
		f.failuresLeft--
		return nil, errors.New("dial refused")
	}
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Header:     http.Header{},
		Body:       http.NoBody,
		Request:    req,
	}, nil
}

// waitFor polls until cond holds, or fails the test. Used only where the thing
// being waited on is a background goroutine reaching a state, which no channel
// in the public API exposes.
//
// The sleep here is a poll interval, not a synchronisation primitive: the test
// still fails if cond never holds, and never passes because the timing happened
// to work out.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// quietPolls waits for n queue polls to run to completion, and is how a test
// asserts that nothing further happened.
//
// Sleeping would only establish that time passed. Waiting for the poller to
// inspect the queue and find nothing establishes that it looked — so the test
// fails on a slow machine rather than passing because the retry it was watching
// for had not got around to firing yet.
func quietPolls(t *testing.T, lim *pace.Limiter, n int) {
	t.Helper()
	polled := make(chan struct{}, n)
	pace.SetAfterPollHook(lim, func() {
		select {
		case polled <- struct{}{}:
		default: // the test has what it needs
		}
	})
	t.Cleanup(func() { pace.SetAfterPollHook(lim, nil) })

	for i := range n {
		select {
		case <-polled:
		case <-time.After(10 * time.Second):
			t.Fatalf("the queue poller ran %d times in 10s, want %d", i, n)
		}
	}
}

func fastRetry(maxAttempts int) pace.RetryPolicy {
	return pace.RetryPolicy{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Millisecond,
		MaxDelay:    5 * time.Millisecond,
		NoJitter:    true,
	}
}

// TestRetryPolicyBackoffGrowsAndCaps covers the schedule itself, with jitter off
// so the numbers are exact.
func TestRetryPolicyBackoffGrowsAndCaps(t *testing.T) {
	p := pace.RetryPolicy{
		BaseDelay:  100 * time.Millisecond,
		MaxDelay:   time.Second,
		Multiplier: 2,
		NoJitter:   true,
	}
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond}, // treated as the first attempt
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, time.Second}, // capped
		{50, time.Second},
	}
	for _, tt := range tests {
		if got := pace.Backoff(p, tt.attempt); got != tt.want {
			t.Errorf("backoff(attempt=%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

// TestRetryPolicyJitterSpreadsRetries: an upstream outage stalls every job at
// once, so a fixed schedule sends them all back at the same instant. Jitter has
// to actually produce a spread for that to be avoided.
func TestRetryPolicyJitterSpreadsRetries(t *testing.T) {
	p := pace.RetryPolicy{BaseDelay: time.Second, MaxDelay: time.Minute, Multiplier: 2}
	seen := map[time.Duration]bool{}
	for range 50 {
		d := pace.Backoff(p, 3)
		if d < 0 || d > 4*time.Second {
			t.Fatalf("jittered backoff %v is outside [0, 4s]", d)
		}
		seen[d] = true
	}
	if len(seen) < 10 {
		t.Errorf("only %d distinct delays across 50 draws; jitter is not spreading", len(seen))
	}
}

func TestRetryPolicyDefaults(t *testing.T) {
	// The zero policy must be usable, not degenerate.
	if got := pace.Backoff(pace.RetryPolicy{}, 1); got < 0 || got > 500*time.Millisecond {
		t.Errorf("zero-policy backoff = %v, want within [0, 500ms]", got)
	}
}

// TestDurableRetriesTransportFailure: a request that never reached the server is
// retried in the background, and the caller does not wait for it.
func TestDurableRetriesTransportFailure(t *testing.T) {
	var attempts atomic.Int64
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      pace.PerMinute(6000),
		Burst:     100,
		DBPath:    filepath.Join(t.TempDir(), "q.db"),
		Transport: &flakyTransport{failuresLeft: 2, served: &attempts},
		Queue: pace.QueueConfig{
			PollInterval: 10 * time.Millisecond,
			Retry:        fastRetry(5),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	// The caller's own attempt fails and returns immediately.
	if _, err := durableDo(context.Background(), lim.Client("alice"), "flaky", http.MethodGet, "/"); err == nil {
		t.Fatal("the first attempt reported success despite a failing transport")
	}

	waitFor(t, "the background poller to retry until the transport recovers",
		func() bool { return attempts.Load() >= 3 })
}

// TestDurableDeadLettersAfterMaxAttempts: retries are bounded. A job that never
// succeeds ends up somewhere a human can see it, rather than looping forever.
func TestDurableDeadLettersAfterMaxAttempts(t *testing.T) {
	var attempts atomic.Int64
	dead := make(chan pace.DeadJob, 4)
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      pace.PerMinute(6000),
		Burst:     100,
		DBPath:    filepath.Join(t.TempDir(), "q.db"),
		Transport: &flakyTransport{failuresLeft: 1 << 30, served: &attempts},
		Queue: pace.QueueConfig{
			PollInterval: 10 * time.Millisecond,
			Retry:        fastRetry(3),
			OnDeadLetter: func(_ context.Context, j pace.DeadJob) { dead <- j },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	_, _ = durableDo(context.Background(), lim.Client("alice"), "doomed", http.MethodGet, "/")

	select {
	case j := <-dead:
		if j.ID != "doomed" {
			t.Errorf("dead job = %q, want %q", j.ID, "doomed")
		}
		if j.Reason == "" {
			t.Error("dead job carries no reason")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("the job was never dead-lettered; it was attempted %d times", attempts.Load())
	}

	// Retrying must actually stop.
	settled := attempts.Load()
	quietPolls(t, lim, 3)
	if got := attempts.Load(); got != settled {
		t.Errorf("attempts went from %d to %d after dead-lettering, want no further sends", settled, got)
	}

	jobs, err := lim.DeadJobs(context.Background(), pace.DeadJobQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != "doomed" {
		t.Errorf("DeadJobs = %+v, want the one abandoned job", jobs)
	}
}

// TestRetryOnDefaultsToNever: a response, of any status, means the request was
// delivered. pace does not interpret status codes unless asked to.
func TestRetryOnDefaultsToNever(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
		Queue: pace.QueueConfig{
			PollInterval: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	resp, err := durableDo(context.Background(), lim.Client("alice"), "five-hundred", http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusInternalServerError {
		t.Errorf("status = %d, want the 500 returned to the caller", resp.StatusCode())
	}
	quietPolls(t, lim, 3)
	if got := served.Load(); got != 1 {
		t.Errorf("a 500 was sent %d times with RetryOn unset, want 1 delivery", got)
	}
}

// TestRetryOnHookTriggersRetry: the caller's API knows which of its own
// responses are transient, so the decision is a hook rather than a status table
// baked into the library.
func TestRetryOnHookTriggersRetry(t *testing.T) {
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if served.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
		Queue: pace.QueueConfig{
			PollInterval: 10 * time.Millisecond,
			Retry:        fastRetry(5),
			RetryOn: func(_ context.Context, d pace.RetryDecision) bool {
				return d.Response.StatusCode() >= 500
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if _, err := durableDo(context.Background(), lim.Client("alice"), "flappy", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}

	waitFor(t, "RetryOn to drive retries until the server stops failing",
		func() bool { return served.Load() >= 3 })
}

// TestAmbiguousPostIsNotRetriedOnTransportError: with no idempotency key, a
// POST whose outcome is unknown must not be repeated by the retry loop either —
// the same rule that governs recovery after a restart.
func TestAmbiguousPostIsNotRetriedOnTransportError(t *testing.T) {
	var attempts atomic.Int64
	dead := make(chan pace.DeadJob, 4)
	lim, err := pace.New(pace.Config{
		BaseURL:   "http://stub.invalid",
		Rate:      pace.PerMinute(6000),
		Burst:     100,
		DBPath:    filepath.Join(t.TempDir(), "q.db"),
		Transport: &flakyTransport{failuresLeft: 1 << 30, served: &attempts},
		Queue: pace.QueueConfig{
			PollInterval:      10 * time.Millisecond,
			IdempotencyHeader: "-",
			Retry:             fastRetry(10),
			OnDeadLetter:      func(_ context.Context, j pace.DeadJob) { dead <- j },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	_, _ = durableDo(context.Background(), lim.Client("alice"), "unsafe", http.MethodPost, "/pay")

	select {
	case j := <-dead:
		if j.ID != "unsafe" {
			t.Errorf("dead job = %q, want %q", j.ID, "unsafe")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("an unsafe POST with an unknown outcome was not parked")
	}

	settled := attempts.Load()
	quietPolls(t, lim, 3)
	if got := attempts.Load(); got != settled {
		t.Errorf("the POST was sent again after being parked: %d then %d", settled, got)
	}
}

// TestQueueWorkersAreBounded: a backlog must not become one goroutine per job.
func TestQueueWorkersAreBounded(t *testing.T) {
	var concurrent, peak atomic.Int64
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := concurrent.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		<-release
		concurrent.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const workers = 3
	dbPath := filepath.Join(t.TempDir(), "q.db")
	for i := range 25 {
		seedQueuedJob(t, dbPath, jobID(i), "alice", http.MethodGet, "/")
	}

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(60000),
		Burst:   1000,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			Workers:      workers,
			PollInterval: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })

	waitFor(t, "workers to saturate", func() bool { return peak.Load() >= workers })
	close(release)

	if got := peak.Load(); got > workers {
		t.Errorf("%d requests were in flight at once, want at most %d", got, workers)
	}
}

func jobID(i int) string {
	return "job-" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}
