package limiter_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	pace "github.com/jaeminst/pace/limiter"
	"github.com/jaeminst/pace/queue"
	"github.com/jaeminst/pace/rate"
)

// TestTwoLimitersSharingADatabaseSendEachJobOnce is the test for the claim
// README makes twice: that two processes sharing one database file will not
// send the same request twice.
//
// Nothing verified it before. wal_test.go covers the reader/writer split, and
// queue_test.go drives Claim directly with invented owner strings — neither
// exercises two live Limiters racing for the same rows through the whole poll,
// claim, send, complete cycle, which is what the claim is actually about.
//
// Two Limiters in one process is the strongest form available to a test: they
// share nothing but the file, exactly as separate processes would, and unlike
// separate processes a data race between them would be caught by -race.
func TestTwoLimitersSharingADatabaseSendEachJobOnce(t *testing.T) {
	const jobs = 40

	var mu sync.Mutex
	sends := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sends[r.URL.Path]++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dbPath := filepath.Join(t.TempDir(), "shared.db")
	for i := range jobs {
		seedQueuedJob(t, dbPath, fmt.Sprintf("job-%02d", i), "alice", http.MethodPost, fmt.Sprintf("/j/%02d", i))
	}

	newWorker := func() *pace.Limiter {
		t.Helper()
		lim, err := pace.New(pace.Config{
			BaseURL: srv.URL,
			Rate:    rate.PerMinute(60000),
			Burst:   1000,
			DBPath:  dbPath,
			Queue: queue.Config{
				PollInterval: time.Millisecond,
				Workers:      8,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		return lim
	}

	// Both start against a database already full of work, so they contend from
	// their very first poll rather than one draining the queue before the other
	// is awake.
	a, b := newWorker(), newWorker()
	defer b.Close()
	defer a.Close()

	deadline := time.After(30 * time.Second)
	for {
		mu.Lock()
		done := len(sends)
		mu.Unlock()
		if done == jobs {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("only %d of %d jobs were delivered", done, jobs)
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Give both pollers room to redeliver anything they wrongly believe is
	// still outstanding. Without the claim guard this is where a second copy
	// shows up.
	quietPolls(t, a, 3)
	quietPolls(t, b, 3)

	mu.Lock()
	defer mu.Unlock()
	var duplicated []string
	for path, n := range sends {
		if n != 1 {
			duplicated = append(duplicated, fmt.Sprintf("%s sent %d times", path, n))
		}
	}
	if len(duplicated) > 0 {
		t.Errorf("two Limiters sharing one database double-sent %d jobs: %v",
			len(duplicated), duplicated)
	}
	if len(sends) != jobs {
		t.Errorf("%d distinct jobs delivered, want %d", len(sends), jobs)
	}
}

// TestSecondLimiterPicksUpAStrandedJob is the other half of multi-process
// safety: exclusivity must not become a job nobody will ever touch again. A
// worker that dies mid-send leaves the row owned and 'sending', and only the
// lease expiring lets anyone else have it.
func TestSecondLimiterPicksUpAStrandedJob(t *testing.T) {
	var served sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served.Store(r.URL.Path, struct{}{})
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dbPath := filepath.Join(t.TempDir(), "stranded.db")
	// Left behind by a process that committed its intent to send and then died;
	// the lease is already expired, as it would be after any restart.
	strandSendingJob(t, dbPath, "orphan", "alice", http.MethodGet, "/orphan")

	lim, err := pace.New(pace.Config{
		BaseURL: srv.URL,
		Rate:    rate.PerMinute(60000),
		Burst:   100,
		DBPath:  dbPath,
		// A GET is safe to repeat, so the ambiguity resolves in favour of
		// delivering it. See AmbiguousPolicy.
		Queue: queue.Config{
			PollInterval:    time.Millisecond,
			AmbiguousPolicy: queue.AmbiguousAuto,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer lim.Close()
	pace.WaitReplay(lim)

	waitFor(t, "the surviving Limiter to take over the stranded job", func() bool {
		_, ok := served.Load("/orphan")
		return ok
	})

	if _, err := lim.DeadJobs(context.Background(), queue.DeadJobQuery{}); err != nil {
		t.Fatal(err)
	}
}
