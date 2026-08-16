package pace_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jaeminst/pace"
)

type queueFixture struct {
	lim      *pace.Limiter
	dbPath   string
	baseURL  string
	requests *atomic.Int64
	keys     *sync.Map // Idempotency-Key -> struct{}
}

func newQueueFixture(t *testing.T, opts ...func(*pace.Config)) *queueFixture {
	t.Helper()
	var served atomic.Int64
	keys := &sync.Map{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		if k := r.Header.Get("Idempotency-Key"); k != "" {
			keys.Store(k, struct{}{})
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	dbPath := filepath.Join(t.TempDir(), "queue.db")
	cfg := pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		DBPath:  dbPath,
	}
	for _, o := range opts {
		o(&cfg)
	}
	lim, err := pace.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lim.Close() })
	pace.WaitReplay(lim)
	return &queueFixture{lim: lim, dbPath: dbPath, baseURL: srv.URL, requests: &served, keys: keys}
}

// TestDurableSendsIdempotencyKey covers the piece that makes at-least-once
// behave as exactly-once against a cooperating server: the job ID travels with
// the request, so a retry can be collapsed into the original delivery.
func TestDurableSendsIdempotencyKey(t *testing.T) {
	f := newQueueFixture(t)
	if _, err := durableDo(context.Background(), f.lim.Client("alice"), "job-1", http.MethodPost, "/"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.keys.Load("job-1"); !ok {
		t.Error("the server never saw an Idempotency-Key carrying the job ID")
	}
}

func TestDurableIdempotencyHeaderCanBeDisabled(t *testing.T) {
	f := newQueueFixture(t, func(c *pace.Config) { c.Queue.IdempotencyHeader = "-" })
	if _, err := durableDo(context.Background(), f.lim.Client("alice"), "job-1", http.MethodPost, "/"); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.keys.Load("job-1"); ok {
		t.Error("an Idempotency-Key was sent despite being disabled")
	}
}

func TestDurableCustomIdempotencyHeader(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Request-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
		Queue: pace.QueueConfig{
			IdempotencyHeader: "X-Request-Key",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if _, err := durableDo(context.Background(), lim.Client("alice"), "job-7", http.MethodPost, "/"); err != nil {
		t.Fatal(err)
	}
	if got != "job-7" {
		t.Errorf("X-Request-Key = %q, want %q", got, "job-7")
	}
}

// TestDurableSingleflightSharesOneSend covers concurrent callers inside one
// process: they collapse onto a single execution and all receive its result.
func TestDurableSingleflightSharesOneSend(t *testing.T) {
	f := newQueueFixture(t)

	const id = "contended"
	const racers = 16
	var wg sync.WaitGroup
	var ok atomic.Int64
	for range racers {
		wg.Go(func() {
			resp, err := durableDo(context.Background(), f.lim.Client("alice"), id, http.MethodPost, "/")
			if err != nil {
				t.Errorf("caller failed: %v", err)
				return
			}
			if resp.StatusCode() != http.StatusOK {
				t.Errorf("status = %d, want 200", resp.StatusCode())
				return
			}
			ok.Add(1)
		})
	}
	wg.Wait()

	if got := f.requests.Load(); got != 1 {
		t.Errorf("the server received %d copies of the request, want exactly 1", got)
	}
	if got := ok.Load(); got != racers {
		t.Errorf("%d of %d callers got the shared result, want all", got, racers)
	}
}

// TestDurableClaimRejectsWhenHeldElsewhere covers the case the in-process
// singleflight cannot see: another worker — a second process on the same
// database, or a replay goroutine that has not yet registered — already owns
// the job. INSERT OR IGNORE deduplicates the row; only the claim deduplicates
// the send.
func TestDurableClaimRejectsWhenHeldElsewhere(t *testing.T) {
	f := newQueueFixture(t, func(c *pace.Config) { c.Queue.JobLease = time.Hour })

	const id = "held-elsewhere"
	if err := pace.Enqueue(f.lim, id, "alice", http.MethodPost, "/"); err != nil {
		t.Fatal(err)
	}
	// Another worker takes it, with a lease that will not expire during the test.
	if err := pace.ClaimJob(f.lim, id, "another-process"); err != nil {
		t.Fatal(err)
	}

	_, err := durableDo(context.Background(), f.lim.Client("alice"), id, http.MethodPost, "/")
	if !errors.Is(err, pace.ErrJobClaimed) {
		t.Errorf("durable call = %v, want ErrJobClaimed", err)
	}
	if got := f.requests.Load(); got != 0 {
		t.Errorf("the server received %d requests for a job owned elsewhere, want 0", got)
	}
}

// TestAmbiguousJobIsParkedForUnsafeMethod covers the window that cannot be
// closed: a POST dispatched but never recorded. Retrying it would risk a
// duplicate charge, so under the default policy it is abandoned and reported
// rather than sent again.
func TestAmbiguousJobIsParkedForUnsafeMethod(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	// Leave a POST stranded mid-flight, exactly as a crash between dispatch and
	// recording the response would.
	strandSendingJob(t, dbPath, "stranded", "alice", http.MethodPost, "/pay")

	var dead []pace.DeadJob
	var mu sync.Mutex
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			IdempotencyHeader: "-", // no key, so a repeat is genuinely unsafe
			OnDeadLetter: func(_ context.Context, j pace.DeadJob) {
				mu.Lock()
				defer mu.Unlock()
				dead = append(dead, j)
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if got := served.Load(); got != 0 {
		t.Errorf("the stranded POST was re-sent %d times, want 0", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(dead) != 1 {
		t.Fatalf("OnDeadLetter fired %d times, want 1", len(dead))
	}
	if dead[0].ID != "stranded" || dead[0].Method != http.MethodPost {
		t.Errorf("dead job = %+v, want the stranded POST", dead[0])
	}
	if dead[0].Reason == "" {
		t.Error("dead job carries no reason")
	}
}

// TestAmbiguousJobIsRetriedForIdempotentMethod is the other half of the default
// policy: a GET is defined to be safe to repeat, so it is.
func TestAmbiguousJobIsRetriedForIdempotentMethod(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	strandSendingJob(t, dbPath, "stranded-get", "alice", http.MethodGet, "/things")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			IdempotencyHeader: "-",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if got := served.Load(); got != 1 {
		t.Errorf("the stranded GET was sent %d times, want 1", got)
	}
}

// TestAmbiguousPolicyRetryOverridesSafety lets a caller say that duplicate
// delivery is cheaper than loss, even for a POST.
func TestAmbiguousPolicyRetryOverridesSafety(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	strandSendingJob(t, dbPath, "stranded", "alice", http.MethodPost, "/pay")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			IdempotencyHeader: "-",
			AmbiguousPolicy:   pace.AmbiguousRetry,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if got := served.Load(); got != 1 {
		t.Errorf("AmbiguousRetry sent the POST %d times, want 1", got)
	}
}

// TestAmbiguousPolicyParkOverridesIdempotence is the mirror: a caller can
// refuse to repeat even a GET.
func TestAmbiguousPolicyParkOverridesIdempotence(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	strandSendingJob(t, dbPath, "stranded-get", "alice", http.MethodGet, "/things")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			AmbiguousPolicy: pace.AmbiguousPark,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if got := served.Load(); got != 0 {
		t.Errorf("AmbiguousPark sent the GET %d times, want 0", got)
	}
}

// TestAmbiguousAutoRetriesWhenIdempotencyKeyIsConfigured: with a key the server
// can collapse duplicates on, even a POST becomes safe to repeat.
func TestAmbiguousAutoRetriesWhenIdempotencyKeyIsConfigured(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	var served atomic.Int64
	var gotKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Add(1)
		gotKey = r.Header.Get("Idempotency-Key")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	strandSendingJob(t, dbPath, "stranded", "alice", http.MethodPost, "/pay")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if got := served.Load(); got != 1 {
		t.Errorf("the POST was sent %d times, want 1", got)
	}
	if gotKey != "stranded" {
		t.Errorf("Idempotency-Key = %q, want %q", gotKey, "stranded")
	}
}

func TestQueuedJobIsAlwaysReplayed(t *testing.T) {
	// A job that was persisted but never dispatched is unambiguous: send it.
	dbPath := filepath.Join(t.TempDir(), "q.db")
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		served.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	seedQueuedJob(t, dbPath, "queued-post", "alice", http.MethodPost, "/pay")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			AmbiguousPolicy:   pace.AmbiguousPark, // would park it if misclassified
			IdempotencyHeader: "-",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	if got := served.Load(); got != 1 {
		t.Errorf("a never-dispatched job was sent %d times, want 1", got)
	}
}

func TestAmbiguousPolicyString(t *testing.T) {
	tests := map[pace.AmbiguousPolicy]string{
		pace.AmbiguousAuto:       "auto",
		pace.AmbiguousRetry:      "retry",
		pace.AmbiguousPark:       "park",
		pace.AmbiguousPolicy(99): "unknown",
	}
	for p, want := range tests {
		if got := p.String(); got != want {
			t.Errorf("AmbiguousPolicy(%d).String() = %q, want %q", p, got, want)
		}
	}
}

func TestDurableCompletedJobReturnsCachedResult(t *testing.T) {
	f := newQueueFixture(t)
	ctx := context.Background()

	first, err := durableDo(ctx, f.lim.Client("alice"), "job-1", http.MethodPost, "/")
	if err != nil {
		t.Fatal(err)
	}
	second, err := durableDo(ctx, f.lim.Client("alice"), "job-1", http.MethodPost, "/")
	if err != nil {
		t.Fatal(err)
	}

	if got := f.requests.Load(); got != 1 {
		t.Errorf("the server was called %d times for one job ID, want 1", got)
	}
	if first.StatusCode() != second.StatusCode() {
		t.Errorf("cached result differs: %d then %d", first.StatusCode(), second.StatusCode())
	}
}

func TestJobLeaseExpiryAllowsRecovery(t *testing.T) {
	// A worker that crashed mid-send leaves a claim behind. Once the lease
	// expires the job must become claimable again, or a crash would strand it
	// until someone deleted the row by hand.
	//
	// Expiry is driven by a fake clock rather than a tiny JobLease: Windows'
	// wall clock is coarse enough that two Now() calls can land in the same
	// tick, in which case a nanosecond lease has not expired yet.
	clk := newFakeClock()
	f := newQueueFixture(t, func(c *pace.Config) {
		c.Queue.JobLease = time.Minute
		c.Clock = clk
	})

	const id = "leased"
	if err := pace.Enqueue(f.lim, id, "alice", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if err := pace.ClaimJob(f.lim, id, "some-dead-process"); err != nil {
		t.Fatal(err)
	}
	clk.advance(time.Hour)

	if _, err := durableDo(context.Background(), f.lim.Client("alice"), id, http.MethodGet, "/"); err != nil {
		t.Fatalf("a job with an expired lease could not be reclaimed: %v", err)
	}
	if got := f.requests.Load(); got != 1 {
		t.Errorf("the server saw %d requests, want 1", got)
	}
}

// TestDurableReleasedWhenNeverDispatched covers the distinction the queue rests
// on. A failure before the request goes out is unambiguous — the job returns to
// the queue and is retried — while a failure after it goes out is not, and must
// leave the job for AmbiguousPolicy to judge.
func TestDurableReleasedWhenNeverDispatched(t *testing.T) {
	f := newQueueFixture(t, func(c *pace.Config) {
		c.Rate = pace.PerMinute(6) // one token per 10s
		c.Burst = 1
	})
	alice := f.lim.Client("alice")
	ctx := context.Background()

	// Spend the burst, so the next durable call cannot get a token.
	if !alice.Allow(context.Background()) {
		t.Fatal("could not spend the initial burst")
	}

	deadlined, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err := durableDo(deadlined, alice, "starved", http.MethodPost, "/")
	var le *pace.LimitError
	if !errors.As(err, &le) {
		t.Fatalf("durable call = %v, want *pace.LimitError", err)
	}
	if got := f.requests.Load(); got != 0 {
		t.Fatalf("the server saw %d requests despite no token, want 0", got)
	}

	// The job was never dispatched, so it must be queued rather than left
	// mid-flight. Restarting with AmbiguousPark would drop it if it were
	// misclassified; here it must be sent.
	if err := f.lim.Close(); err != nil {
		t.Fatal(err)
	}
	lim2, err := pace.New(pace.Config{
		BaseURL: f.baseURL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  f.dbPath,
		Queue: pace.QueueConfig{
			AmbiguousPolicy: pace.AmbiguousPark,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim2.Close()
	pace.WaitReplay(lim2)

	if got := f.requests.Load(); got != 1 {
		t.Errorf("the released job was sent %d times on restart, want 1", got)
	}
}

func TestDeadJobsIsReadableAfterRestart(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	strandSendingJob(t, dbPath, "abandoned", "alice", http.MethodPost, "/pay")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    pace.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: pace.QueueConfig{
			IdempotencyHeader: "-",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	dead, err := lim.DeadJobs(context.Background(), pace.DeadJobQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 {
		t.Fatalf("DeadJobs returned %d jobs, want 1", len(dead))
	}
	if dead[0].ID != "abandoned" || dead[0].Reason == "" {
		t.Errorf("dead job = %+v, want the abandoned POST with a reason", dead[0])
	}
}

func TestDeadJobsWithoutQueue(t *testing.T) {
	lim, _ := newTestLimiter(t)
	if _, err := lim.DeadJobs(context.Background(), pace.DeadJobQuery{Limit: 10}); !errors.Is(err, pace.ErrNoQueue) {
		t.Errorf("DeadJobs without a queue = %v, want ErrNoQueue", err)
	}
}

// TestDeadJobsCanBeDrainedPastTheNewestPage is why DeadJobs takes a query. With
// only a limit, the table could be read from the top and nowhere else: anything
// past the newest N rows was unreachable through the public API, on the one
// table whose stated purpose is "the ones a human has to decide about" and the
// one table nothing bounds.
func TestDeadJobsCanBeDrainedPastTheNewestPage(t *testing.T) {
	const jobs = 7
	dbPath := filepath.Join(t.TempDir(), "q.db")
	for i := range jobs {
		// Unsafe method and no idempotency header, so replay parks each one.
		strandSendingJob(t, dbPath, fmt.Sprintf("dead-%02d", i), "alice", http.MethodPost, "/pay")
	}

	// A real clock: died_at is the cursor, and a frozen one gives every job the
	// same instant, which is the one case the cursor cannot separate. See
	// DeadJobQuery.Before.
	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		DBPath:  dbPath,
		Queue:   pace.QueueConfig{IdempotencyHeader: "-"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	ctx := context.Background()
	seen := map[string]bool{}
	var before time.Time
	for page := 0; page < jobs+2; page++ {
		got, err := lim.DeadJobs(ctx, pace.DeadJobQuery{Limit: 3, Before: before})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			break
		}
		for _, j := range got {
			if j.DiedAt.IsZero() {
				t.Fatalf("job %q has a zero DiedAt; paging is impossible without it", j.ID)
			}
			if seen[j.ID] {
				t.Fatalf("job %q appeared on two pages", j.ID)
			}
			seen[j.ID] = true
		}
		before = got[len(got)-1].DiedAt
	}

	if len(seen) != jobs {
		t.Errorf("paged through %d of %d dead jobs", len(seen), jobs)
	}
}

// TestDeadJobsFilterByUser: the other thing a bare limit could not express.
func TestDeadJobsFilterByUser(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "q.db")
	strandSendingJob(t, dbPath, "a-1", "alice", http.MethodPost, "/pay")
	strandSendingJob(t, dbPath, "b-1", "bob", http.MethodPost, "/pay")
	strandSendingJob(t, dbPath, "a-2", "alice", http.MethodPost, "/pay")

	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    pace.PerMinute(6000),
		Burst:   100,
		DBPath:  dbPath,
		Queue:   pace.QueueConfig{IdempotencyHeader: "-"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	got, err := lim.DeadJobs(context.Background(), pace.DeadJobQuery{UserID: "alice"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("DeadJobs for alice returned %d jobs, want 2", len(got))
	}
	for _, j := range got {
		if j.UserID != "alice" {
			t.Errorf("returned a job for %q under a UserID filter of alice", j.UserID)
		}
	}
}
