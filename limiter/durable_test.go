package limiter_test

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

	"github.com/jaeminst/pace/limit"
	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/queue"
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
		Rate:    limit.PerMinute(6000),
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  filepath.Join(t.TempDir(), "q.db"),
		Queue: queue.Config{
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

	var dead []queue.DeadJob
	var mu sync.Mutex
	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: queue.Config{
			IdempotencyHeader: "-", // no key, so a repeat is genuinely unsafe
			OnDeadLetter: func(_ context.Context, j queue.DeadJob) {
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: queue.Config{
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: queue.Config{
			IdempotencyHeader: "-",
			AmbiguousPolicy:   queue.AmbiguousRetry,
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: queue.Config{
			AmbiguousPolicy: queue.AmbiguousPark,
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
		Rate:    limit.PerMinute(600),
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: queue.Config{
			AmbiguousPolicy:   queue.AmbiguousPark,
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
	tests := map[queue.AmbiguousPolicy]string{
		queue.AmbiguousAuto:       "auto",
		queue.AmbiguousRetry:      "retry",
		queue.AmbiguousPark:       "park",
		queue.AmbiguousPolicy(99): "unknown",
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
		c.Rate = limit.PerMinute(6) // one token per 10s
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  f.dbPath,
		Queue: queue.Config{
			AmbiguousPolicy: queue.AmbiguousPark,
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
		Rate:    limit.PerMinute(600),
		Burst:   10,
		DBPath:  dbPath,
		Queue: queue.Config{
			IdempotencyHeader: "-",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	dead, err := lim.DeadJobs(context.Background(), queue.DeadJobQuery{})
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
	if _, err := lim.DeadJobs(context.Background(), queue.DeadJobQuery{Limit: 10}); !errors.Is(err, pace.ErrNoQueue) {
		t.Errorf("DeadJobs without a queue = %v, want ErrNoQueue", err)
	}
}

// TestDeadJobsCanBeDrainedPastTheNewestPage is why DeadJobs takes a query. With
// only a limit, the table could be read from the top and nowhere else: anything
// past the newest N rows was unreachable through the public API, on the one
// table whose stated purpose is "the ones a human has to decide about" and the
// one table nothing bounds.
// TestDeadJobsCanBeDrainedPastTheNewestPage pins that paging reaches every row.
//
// The clock is frozen on purpose. Replay parks all seven stranded jobs inside
// one loop, so on any clock they are liable to share a died_at, and a cursor
// carrying only the instant then steps over every row holding the boundary
// value. Freezing makes that certain rather than likely: before the cursor
// carried the id as well, this test lost rows on every run, and against a real
// clock it lost them on most.
func TestDeadJobsCanBeDrainedPastTheNewestPage(t *testing.T) {
	const jobs = 7
	dbPath := filepath.Join(t.TempDir(), "q.db")
	for i := range jobs {
		// Unsafe method and no idempotency header, so replay parks each one.
		strandSendingJob(t, dbPath, fmt.Sprintf("dead-%02d", i), "alice", http.MethodPost, "/pay")
	}

	lim, err := pace.New(pace.Config{
		BaseURL: "http://example.invalid",
		Rate:    limit.PerMinute(6000),
		Burst:   100,
		DBPath:  dbPath,
		Clock:   newFakeClock(),
		Queue:   queue.Config{IdempotencyHeader: "-"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	ctx := context.Background()
	seen := map[string]bool{}
	var before *queue.DeadJob
	for page := 0; page < jobs+2; page++ {
		got, err := lim.DeadJobs(ctx, queue.DeadJobQuery{Limit: 3, Before: before})
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 0 {
			break
		}
		for _, j := range got {
			if seen[j.ID] {
				t.Fatalf("job %q appeared on two pages", j.ID)
			}
			seen[j.ID] = true
		}
		before = &got[len(got)-1]
	}

	if len(seen) != jobs {
		t.Errorf("paged through %d of %d dead jobs; the cursor skipped %d that share an instant",
			len(seen), jobs, jobs-len(seen))
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
		Rate:    limit.PerMinute(6000),
		Burst:   100,
		DBPath:  dbPath,
		Queue:   queue.Config{IdempotencyHeader: "-"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	got, err := lim.DeadJobs(context.Background(), queue.DeadJobQuery{UserID: "alice"})
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

// --- Durable queue tests ---

func TestDurable_NoPersistence(t *testing.T) {
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    limit.PerMinute(60),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	_, err = durableDo(context.Background(), client.Client("u"), "job-1", http.MethodGet, "/")
	if !errors.Is(err, pace.ErrNoQueue) {
		t.Fatalf("expected ErrNoQueue, got %v", err)
	}
}

func TestDurable_NewJob(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "once.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	resp, err := durableDo(context.Background(), client.Client("alice"), "job-1", http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
}

func TestDurable_CachedResult(t *testing.T) {
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("cached"))
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "cached.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	// First call executes the HTTP request.
	if _, err := durableDo(context.Background(), client.Client("u"), "job-42", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	// Second call with same ID must return cached result without a new HTTP call.
	resp, err := durableDo(context.Background(), client.Client("u"), "job-42", http.MethodGet, "/")
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Body()) != "cached" {
		t.Fatalf("want cached body, got %q", resp.Body())
	}
	if callCount.Load() != 1 {
		t.Fatalf("expected 1 HTTP call, got %d", callCount.Load())
	}
}

func TestDurable_Singleflight(t *testing.T) {
	// Concurrent Durable calls with the same ID: only one HTTP request fires.
	ready := make(chan struct{})
	arrived := make(chan struct{})
	signalArrived := sync.OnceFunc(func() { close(arrived) })
	var callCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		callCount.Add(1)
		signalArrived()
		<-ready // hold the server until the leader is provably in flight
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "sf.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	const n = 5
	errs := make(chan error, n)
	for range n {
		go func() {
			_, err := durableDo(context.Background(), client.Client("u"), "sf-job", http.MethodGet, "/sf")
			errs <- err
		}()
	}
	<-arrived // one goroutine won the claim and is on the wire
	close(ready)

	for range n {
		if err := <-errs; err != nil {
			t.Errorf("Durable error: %v", err)
		}
	}
	if callCount.Load() != 1 {
		t.Errorf("expected 1 HTTP call, got %d", callCount.Load())
	}
}

func TestDurable_ReplayOnRestart(t *testing.T) {
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "replay.db")

	// Create client1, plant a pending job directly (simulating a crash before completion).
	client1, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	if err := pace.Enqueue(client1, "replay-job", "u", "GET", "/replay"); err != nil {
		t.Fatal(err)
	}
	client1.Close()

	// client2 starts fresh: replay should execute the planted job.
	client2, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client2.Close()
	pace.WaitReplay(client2) // blocks until the replayed job finishes

	// The result must now be cached; Durable returns without a new HTTP call.
	resp, err := durableDo(context.Background(), client2.Client("u"), "replay-job", http.MethodGet, "/replay")
	if err != nil {
		t.Fatalf("Durable after replay: %v", err)
	}
	if resp.StatusCode() != http.StatusOK {
		t.Fatalf("want 200, got %d", resp.StatusCode())
	}
}

func TestDurable_DefaultMethodGet(t *testing.T) {
	var gotMethod string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "meth.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	if _, err := durableDo(context.Background(), client.Client("u"), "j1", http.MethodGet, "/"); err != nil {
		t.Fatal(err)
	}
	if gotMethod != http.MethodGet {
		t.Fatalf("want GET, got %s", gotMethod)
	}
}

func TestDurable_LoadResultError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "lre.db")
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    limit.PerMinute(60),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)

	// Break the underlying DB so Get returns an error.
	pace.CloseLimiterStore(client)

	_, err = durableDo(context.Background(), client.Client("u"), "j", http.MethodGet, "/")
	if err == nil || errors.Is(err, pace.ErrNoQueue) {
		t.Fatalf("expected load result error, got %v", err)
	}
	client.Close()
}

func TestDurable_WaiterCtxCancelled(t *testing.T) {
	// Block the server so the leader stays in-flight; cancel the waiter's context.
	hold := make(chan struct{})
	arrived := make(chan struct{})
	signalArrived := sync.OnceFunc(func() { close(arrived) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		signalArrived()
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "wait.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	// Leader goroutine blocks on the server.
	go func() {
		_, _ = durableDo(context.Background(), client.Client("u"), "w-job", http.MethodGet, "/wait")
	}()
	<-arrived // the leader owns the job and is on the wire

	// Waiter goroutine with a cancellable context.
	ctx2, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := durableDo(ctx2, client.Client("u"), "w-job", http.MethodGet, "/wait")
		errCh <- err
	}()
	// No wait needed before cancelling: whether the waiter has reached await or
	// arrives to find the context already dead, both paths return Canceled.
	cancel()

	err = <-errCh
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	close(hold) // unblock the server so the leader exits
}

func TestDurable_WithHeaders(t *testing.T) {
	var gotHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHdr = r.Header.Get("X-Custom")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "hdr.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	hdrReq := client.Client("u").Durable("hdr-job")
	if _, err := hdrReq.SetHeader("X-Custom", "my-value").Get(context.Background(), "/"); err != nil {
		t.Fatal(err)
	}
	if gotHdr != "my-value" {
		t.Fatalf("want X-Custom=my-value, got %q", gotHdr)
	}
}

func TestDurable_HTTPTransportError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "txerr.db")
	client, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      limit.PerMinute(6000),
		Burst:     10,
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	_, err = durableDo(context.Background(), client.Client("u"), "tx-job", http.MethodGet, "/tx")
	if err == nil {
		t.Fatal("expected transport error from Durable")
	}
}

func TestDurable_CompleteJobError(t *testing.T) {
	// Close the DB while the HTTP call is in-flight so Complete fails.
	// Durable must still return the response (the failure is logged, not returned).
	hold := make(chan struct{})
	arrived := make(chan struct{})
	signalArrived := sync.OnceFunc(func() { close(arrived) })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		signalArrived()
		<-hold
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "cje.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)

	errCh := make(chan error, 1)
	go func() {
		_, err := durableDo(context.Background(), client.Client("u"), "cj-job", http.MethodGet, "/cj")
		errCh <- err
	}()

	// The request is on the wire, so the job is enqueued and claimed. Break the
	// store now, before Complete gets to run.
	<-arrived
	pace.CloseLimiterStore(client)
	close(hold) // let the server respond

	// Durable logs a warning but still returns the HTTP response.
	if err := <-errCh; err != nil {
		t.Fatalf("expected success despite Complete error, got %v", err)
	}
	client.Close()
}

func TestDurable_EnqueueError(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "enq.db")
	client, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    limit.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)

	// Hook closes the DB right before Enqueue runs.
	pace.SetDurableEnqueueHook(client, func() {
		pace.CloseLimiterStore(client)
		pace.SetDurableEnqueueHook(client, nil)
	})

	_, err = durableDo(context.Background(), client.Client("u"), "e-job", http.MethodGet, "/")
	if err == nil {
		t.Fatal("expected enqueue error")
	}
	client.Close()
}

func TestDurable_ReplayJobFails(t *testing.T) {
	// Plant a job that will fail on replay; replay logs a warning.
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "rjf.db")

	client1, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	// Plant a job for a path that won't connect on replay (use a bad port).
	if err := pace.Enqueue(client1, "fail-job", "u", "GET", "/"); err != nil {
		t.Fatal(err)
	}
	client1.Close()

	// client2 replays with a failing transport → replay logs a warning and continues.
	client2, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      limit.PerMinute(6000),
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client2) // waits for the failing goroutine to log and exit
	client2.Close()
}

func TestDurable_CtxCancelledBeforeRequest(t *testing.T) {
	// Cancel the caller's context inside the pre-enqueue hook so that
	// m.Request() sees a cancelled context and doDurable returns ctx.Err().
	srv := newEchoServer(t)
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "ctxcancel.db")

	client, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client)
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	pace.SetDurableEnqueueHook(client, func() {
		cancel()
		pace.SetDurableEnqueueHook(client, nil)
	})

	_, err = durableDo(ctx, client.Client("u"), "cc-job", http.MethodGet, "/")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDurable_ReplayExecuteFails(t *testing.T) {
	// Plant a pending job then replay it with a failing transport so
	// replay logs "pace: replay: execute".
	dbPath := filepath.Join(t.TempDir(), "rxf.db")

	client1, err := pace.New(pace.Config{
		BaseURL: "http://127.0.0.1:1",
		Rate:    limit.PerMinute(6000),
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	if err := pace.Enqueue(client1, "rxf-job", "u", "GET", "/rxf"); err != nil {
		t.Fatal(err)
	}
	client1.Close()

	client2, err := pace.New(pace.Config{
		BaseURL:   "http://127.0.0.1:1",
		Rate:      limit.PerMinute(6000),
		Transport: failTransport{err: errors.New("dial refused")},
		DBPath:    dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client2)
	client2.Close()
}

func TestDurable_ReplayWithHeaders(t *testing.T) {
	// Enqueue a Durable job with headers via a blocking server. Close client1
	// while the HTTP call is in-flight (server still holding); the cancelled
	// context leaves the job pending. client2 replays it, exercising the
	// header-copying loop inside replay().
	hold := make(chan struct{})
	arrived := make(chan struct{})
	signalArrived := sync.OnceFunc(func() { close(arrived) })
	var gotHdr atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHdr.Store(r.Header.Get("X-Replay"))
		signalArrived()
		<-hold // block until released
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	dbPath := filepath.Join(t.TempDir(), "rph.db")

	// client1: start a Durable call with a header; close while server blocks,
	// leaving the job pending in the DB.
	client1, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client1)

	go func() {
		_, _ = client1.Client("u").Durable("hdr-replay-job").
			SetHeader("X-Replay", "replayed").Get(context.Background(), "/hdr-replay")
	}()
	// The request is on the wire, so the job is enqueued and claimed.
	<-arrived
	// Close client1: cancels the in-flight HTTP context; job stays in pending_jobs.
	client1.Close()
	// Also unblock the server so it doesn't leak.
	close(hold)

	// client2: replay finds the pending job with headers and copies them.
	client2, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    limit.PerMinute(6000),
		Burst:   10,
		DBPath:  dbPath,
	})
	if err != nil {
		t.Fatal(err)
	}
	pace.WaitReplay(client2)
	defer client2.Close()
}
