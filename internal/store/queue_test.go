package store

import (
	"context"
	"net/http"
	"testing"
)

func newQueueStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func enqueue(t *testing.T, s *Store, id, method string) {
	t.Helper()
	if err := s.Enqueue(context.Background(), Job{
		ID: id, UserID: "alice", Method: method, Path: "/", Headers: http.Header{},
	}, nextEnqueueTime()); err != nil {
		t.Fatal(err)
	}
}

func jobByID(t *testing.T, s *Store, id string) (Job, bool) {
	t.Helper()
	jobs, err := s.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range jobs {
		if j.ID == id {
			return j, true
		}
	}
	return Job{}, false
}

func TestEnqueueIsIdempotent(t *testing.T) {
	s := newQueueStore(t)
	enqueue(t, s, "job-1", http.MethodGet)
	enqueue(t, s, "job-1", http.MethodGet)

	jobs, err := s.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("Pending returned %d jobs after two identical Enqueues, want 1", len(jobs))
	}
	if jobs[0].State != StateQueued {
		t.Errorf("state = %q, want %q", jobs[0].State, StateQueued)
	}
	if jobs[0].Attempts != 0 {
		t.Errorf("attempts = %d, want 0 before any claim", jobs[0].Attempts)
	}
}

// TestClaimIsExclusive is the guarantee the whole queue rests on: two workers
// racing for one job cannot both win, because the transition is a single
// conditional UPDATE rather than a read followed by a write.
func TestClaimIsExclusive(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)

	const now, lease = 1000, 5000
	first, err := s.Claim(ctx, "job-1", "worker-a", now, lease)
	if err != nil {
		t.Fatal(err)
	}
	if !first {
		t.Fatal("the first claim was refused")
	}

	second, err := s.Claim(ctx, "job-1", "worker-b", now, lease)
	if err != nil {
		t.Fatal(err)
	}
	if second {
		t.Error("a second worker claimed a job already owned, with a live lease")
	}

	j, ok := jobByID(t, s, "job-1")
	if !ok {
		t.Fatal("job disappeared")
	}
	if j.State != StateSending {
		t.Errorf("state = %q, want %q", j.State, StateSending)
	}
	if j.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 after one claim", j.Attempts)
	}
}

// TestClaimAfterLeaseExpiry: a worker that crashed mid-send must not strand the
// job forever.
func TestClaimAfterLeaseExpiry(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)

	if ok, err := s.Claim(ctx, "job-1", "crashed", 1000, 2000); err != nil || !ok {
		t.Fatalf("initial claim = (%v, %v)", ok, err)
	}
	// now is past the lease.
	ok, err := s.Claim(ctx, "job-1", "recovering", 3000, 9000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("a job whose lease expired could not be reclaimed")
	}
	j, _ := jobByID(t, s, "job-1")
	if j.Attempts != 2 {
		t.Errorf("attempts = %d, want 2 after a reclaim", j.Attempts)
	}
}

func TestClaimRespectsNextAttemptAt(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)

	if ok, err := s.Claim(ctx, "job-1", "w", 1000, 9000); err != nil || !ok {
		t.Fatalf("claim = (%v, %v)", ok, err)
	}
	if ok, err := s.Release(ctx, "job-1", "w", 1000, 5000, "transient failure"); err != nil || !ok {
		t.Fatalf("Release = (%v, %v)", ok, err)
	}
	// Too early: the job is not due yet.
	if ok, err := s.Claim(ctx, "job-1", "w", 4000, 9000); err != nil || ok {
		t.Errorf("Claim before next_attempt_at = (%v, %v), want (false, nil)", ok, err)
	}
	// Due now.
	if ok, err := s.Claim(ctx, "job-1", "w", 5000, 9000); err != nil || !ok {
		t.Errorf("Claim at next_attempt_at = (%v, %v), want (true, nil)", ok, err)
	}
}

func TestClaimMissingJob(t *testing.T) {
	s := newQueueStore(t)
	ok, err := s.Claim(context.Background(), "nope", "w", 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("claimed a job that does not exist")
	}
}

func TestReleaseReturnsJobToQueue(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)

	if ok, err := s.Claim(ctx, "job-1", "w", 1000, 5000); err != nil || !ok {
		t.Fatalf("claim = (%v, %v)", ok, err)
	}
	if ok, err := s.Release(ctx, "job-1", "w", 1500, 2000, "dial failed"); err != nil || !ok {
		t.Fatalf("Release = (%v, %v)", ok, err)
	}

	j, ok := jobByID(t, s, "job-1")
	if !ok {
		t.Fatal("job disappeared after Release")
	}
	if j.State != StateQueued {
		t.Errorf("state = %q, want %q after Release", j.State, StateQueued)
	}
	// The attempt is not undone: it happened.
	if j.Attempts != 1 {
		t.Errorf("attempts = %d, want 1 (Release must not rewind the count)", j.Attempts)
	}
}

// TestEnqueueDoesNotResurrectACompletedJob is the second duplicate-send guard,
// and the one INSERT OR IGNORE could never provide: Complete deletes the
// pending row, so after a job finishes there is no row left to conflict with.
//
// Two workers racing for the same job land here. The loser reads the result
// cache just before the winner writes it, finds nothing, and re-enqueues —
// producing a fresh 'queued' row for a request that has already been
// delivered, which the next poll then sends again.
func TestEnqueueDoesNotResurrectACompletedJob(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodPost)

	if ok, err := s.Claim(ctx, "job-1", "A", 1000, 9000); err != nil || !ok {
		t.Fatalf("claim = (%v, %v)", ok, err)
	}
	if err := s.Complete(ctx, "job-1", Result{StatusCode: 200, Headers: http.Header{}}, 2000); err != nil {
		t.Fatal(err)
	}
	if _, ok := jobByID(t, s, "job-1"); ok {
		t.Fatal("Complete left the job pending")
	}

	// Worker B, still holding the job it read before A finished.
	enqueue(t, s, "job-1", http.MethodPost)

	if _, ok := jobByID(t, s, "job-1"); ok {
		t.Error("a completed job was resurrected as pending; the next poll would send it again")
	}
	// The recorded outcome must survive untouched.
	if _, ok, err := s.Get(ctx, "job-1"); err != nil || !ok {
		t.Errorf("Get after the re-enqueue = (%v, %v), want the cached result", ok, err)
	}
}

// TestReleaseByAStaleOwnerIsRefused is the duplicate-send guard. Worker A
// claims a job and stalls long enough for its lease to expire; worker B
// reclaims it and starts sending. If A's late Release were honoured, the job
// would go back to 'queued' while B still has it in flight, and the next worker
// to claim it would send a second copy of a request B is delivering right now.
func TestReleaseByAStaleOwnerIsRefused(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodPost)

	if ok, err := s.Claim(ctx, "job-1", "A", 1000, 2000); err != nil || !ok {
		t.Fatalf("A's claim = (%v, %v)", ok, err)
	}
	// A's lease has expired; B takes over and holds it until 9000.
	if ok, err := s.Claim(ctx, "job-1", "B", 3000, 9000); err != nil || !ok {
		t.Fatalf("B's reclaim = (%v, %v)", ok, err)
	}

	// A finally notices its request failed and tries to hand the job back.
	released, err := s.Release(ctx, "job-1", "A", 4000, 4000, "dial failed")
	if err != nil {
		t.Fatal(err)
	}
	if released {
		t.Error("a stale owner released a job another worker is sending")
	}

	j, ok := jobByID(t, s, "job-1")
	if !ok {
		t.Fatal("job disappeared")
	}
	if j.State != StateSending {
		t.Errorf("state = %q, want %q: B is still sending it", j.State, StateSending)
	}
	// The decisive check: B's lease must still be in force, so nobody else can
	// claim the job while B works.
	if ok, err := s.Claim(ctx, "job-1", "C", 5000, 12000); err != nil || ok {
		t.Errorf("Claim by a third worker = (%v, %v), want (false, nil): B's lease runs to 9000", ok, err)
	}
}

// TestReleaseOfAQueuedJobIsRefused: only the worker that claimed a job may
// release it. A job sitting in 'queued' has no owner to release it.
func TestReleaseOfAQueuedJobIsRefused(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)

	released, err := s.Release(ctx, "job-1", "", 1000, 5000, "never claimed")
	if err != nil {
		t.Fatal(err)
	}
	if released {
		t.Error("released a job that was never claimed")
	}
}

func TestKillMovesJobToDeadLetter(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()

	h := http.Header{}
	h.Set("X-Custom", "kept")
	if err := s.Enqueue(ctx, Job{
		ID: "doomed", UserID: "alice", Method: http.MethodPost,
		Path: "/pay", Headers: h, Body: []byte("payload"),
	}, 1); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Claim(ctx, "doomed", "w", 1000, 5000); err != nil || !ok {
		t.Fatalf("claim = (%v, %v)", ok, err)
	}

	killed, ok, err := s.Kill(ctx, "doomed", "outcome unknown", 7000)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("Kill reported the job as absent")
	}
	if killed.Attempts != 1 || killed.Method != http.MethodPost {
		t.Errorf("killed job = %+v, want the claimed POST", killed)
	}

	if _, present := jobByID(t, s, "doomed"); present {
		t.Error("the job is still pending after being killed")
	}

	dead, err := s.Dead(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 {
		t.Fatalf("Dead returned %d jobs, want 1", len(dead))
	}
	if dead[0].Reason != "outcome unknown" {
		t.Errorf("reason = %q, want %q", dead[0].Reason, "outcome unknown")
	}
	if got := dead[0].Headers.Get("X-Custom"); got != "kept" {
		t.Errorf("headers were lost: X-Custom = %q", got)
	}
	if string(dead[0].Body) != "payload" {
		t.Errorf("body = %q, want %q", dead[0].Body, "payload")
	}
}

func TestKillMissingJob(t *testing.T) {
	s := newQueueStore(t)
	_, ok, err := s.Kill(context.Background(), "nope", "reason", 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("Kill reported success for a job that does not exist")
	}
}

func TestDeadIsEmptyByDefault(t *testing.T) {
	s := newQueueStore(t)
	dead, err := s.Dead(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 0 {
		t.Errorf("Dead returned %d jobs on a fresh database, want 0", len(dead))
	}
}

// TestCompleteIsAtomic: the result must be readable exactly when the pending
// row is gone, never one without the other.
func TestCompleteIsAtomic(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)

	h := http.Header{}
	h.Set("Content-Type", "application/json")
	if err := s.Complete(ctx, "job-1", Result{
		StatusCode: 201, Status: "201 Created", Headers: h, Body: []byte(`{"ok":true}`),
	}, 1); err != nil {
		t.Fatal(err)
	}

	if _, present := jobByID(t, s, "job-1"); present {
		t.Error("the pending row survived Complete")
	}
	res, ok, err := s.Get(ctx, "job-1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("the result was not recorded")
	}
	if res.StatusCode != 201 || string(res.Body) != `{"ok":true}` {
		t.Errorf("result = %+v, want the recorded 201", res)
	}
	if got := res.Headers.Get("Content-Type"); got != "application/json" {
		t.Errorf("result headers lost: Content-Type = %q", got)
	}
}

func TestGetMissingResult(t *testing.T) {
	s := newQueueStore(t)
	res, ok, err := s.Get(context.Background(), "never-ran")
	if err != nil {
		t.Fatal(err)
	}
	if ok || res != nil {
		t.Errorf("Get for an unknown job = (%v, %v), want (nil, false)", res, ok)
	}
}

func TestPendingOrdersOldestFirst(t *testing.T) {
	s := newQueueStore(t)
	for _, id := range []string{"a", "b", "c"} {
		enqueue(t, s, id, http.MethodGet)
	}
	jobs, err := s.Pending(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 3 {
		t.Fatalf("Pending returned %d jobs, want 3", len(jobs))
	}
	// created_at has nanosecond resolution and the inserts are sequential, so
	// insertion order is preserved.
	for i, want := range []string{"a", "b", "c"} {
		if jobs[i].ID != want {
			t.Errorf("jobs[%d].ID = %q, want %q", i, jobs[i].ID, want)
		}
	}
}

func TestOperationsAfterCloseReportErrors(t *testing.T) {
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := s.Enqueue(ctx, Job{ID: "x", Headers: http.Header{}}, 1); err == nil {
		t.Error("Enqueue on a closed store reported success")
	}
	if _, err := s.Pending(ctx); err == nil {
		t.Error("Pending on a closed store reported success")
	}
	if _, err := s.Claim(ctx, "x", "w", 1, 2); err == nil {
		t.Error("Claim on a closed store reported success")
	}
	if _, err := s.Release(ctx, "x", "w", 1, 1, ""); err == nil {
		t.Error("Release on a closed store reported success")
	}
	if _, _, err := s.Kill(ctx, "x", "r", 1); err == nil {
		t.Error("Kill on a closed store reported success")
	}
	if _, err := s.Dead(ctx, 1); err == nil {
		t.Error("Dead on a closed store reported success")
	}
	if _, _, err := s.Get(ctx, "x"); err == nil {
		t.Error("Get on a closed store reported success")
	}
	if err := s.Complete(ctx, "x", Result{Headers: http.Header{}}, 1); err == nil {
		t.Error("Complete on a closed store reported success")
	}
	if err := s.SaveBatch(ctx, []UserState{{UserID: "u"}}); err == nil {
		t.Error("SaveBatch on a closed store reported success")
	}
	if err := s.Save(ctx, "u", 1, 1); err == nil {
		t.Error("Save on a closed store reported success")
	}
	if _, _, err := s.Load(ctx, "u"); err == nil {
		t.Error("Load on a closed store reported success")
	}
}

// dropTable removes a table so that a statement partway through an operation
// fails. It reaches the error branches that only a mid-transaction failure can:
// the first statement succeeds, the second does not.
func dropTable(t *testing.T, s *Store, table string) {
	t.Helper()
	if _, err := s.wdb.Exec(`DROP TABLE ` + table); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteFailsWhenPendingTableIsGone(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)
	dropTable(t, s, "pending_jobs")

	// The result insert succeeds; deleting the pending row cannot.
	if err := s.Complete(ctx, "job-1", Result{Headers: http.Header{}}, 1); err == nil {
		t.Error("Complete reported success with pending_jobs missing")
	}
	// The transaction rolled back, so no partial result was recorded.
	if _, ok, err := s.Get(ctx, "job-1"); err != nil || ok {
		t.Errorf("Get after a rolled-back Complete = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestKillFailsWhenDeadTableIsGone(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)
	dropTable(t, s, "dead_jobs")

	if _, _, err := s.Kill(ctx, "job-1", "reason", 1); err == nil {
		t.Error("Kill reported success with dead_jobs missing")
	}
	// Rolled back: the job is still pending rather than lost.
	if _, present := jobByID(t, s, "job-1"); !present {
		t.Error("a failed Kill lost the job")
	}
}

func TestPendingFailsOnUndecodableHeaders(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	enqueue(t, s, "job-1", http.MethodGet)
	if _, err := s.wdb.ExecContext(ctx,
		`UPDATE pending_jobs SET headers = 'not json' WHERE id = 'job-1'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pending(ctx); err == nil {
		t.Error("Pending decoded a row whose headers are not JSON")
	}
}

func TestGetFailsOnUndecodableHeaders(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	if err := s.Complete(ctx, "job-1", Result{Headers: http.Header{}}, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := s.wdb.ExecContext(ctx,
		`UPDATE job_results SET headers = 'not json' WHERE id = 'job-1'`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Get(ctx, "job-1"); err == nil {
		t.Error("Get decoded a row whose headers are not JSON")
	}
}

func TestSaveBatchRollsBackOnFailure(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()
	if err := s.SaveBatch(ctx, []UserState{{UserID: "alice", Tokens: 1}}); err != nil {
		t.Fatal(err)
	}
	dropTable(t, s, "user_state")
	if err := s.SaveBatch(ctx, []UserState{{UserID: "bob", Tokens: 2}}); err == nil {
		t.Error("SaveBatch reported success with user_state missing")
	}
}

func TestSaveBatchEmptyIsNoOp(t *testing.T) {
	s := newQueueStore(t)
	if err := s.SaveBatch(context.Background(), nil); err != nil {
		t.Errorf("SaveBatch(nil) = %v, want nil", err)
	}
}

func TestPurgeResultsRemovesOnlyExpired(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()

	// Complete stamps completed_at with the wall clock, so rewrite it to
	// place each result on a known side of the cutoff.
	for _, id := range []string{"old-1", "old-2", "fresh"} {
		if err := s.Complete(ctx, id, Result{StatusCode: 200, Headers: http.Header{}}, 1); err != nil {
			t.Fatal(err)
		}
	}
	for id, at := range map[string]int64{"old-1": 100, "old-2": 200, "fresh": 5000} {
		if _, err := s.wdb.ExecContext(ctx,
			`UPDATE job_results SET completed_at = ? WHERE id = ?`, at, id); err != nil {
			t.Fatal(err)
		}
	}

	n, err := s.PurgeResults(ctx, 1000, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("PurgeResults removed %d rows, want 2", n)
	}
	for _, id := range []string{"old-1", "old-2"} {
		if _, ok, err := s.Get(ctx, id); err != nil || ok {
			t.Errorf("%s survived the purge", id)
		}
	}
	if _, ok, err := s.Get(ctx, "fresh"); err != nil || !ok {
		t.Errorf("a result newer than the cutoff was purged: (%v, %v)", ok, err)
	}
}

func TestPurgeResultsChunks(t *testing.T) {
	s := newQueueStore(t)
	ctx := context.Background()

	const results = 25
	for i := range results {
		id := "job-" + string(rune('a'+i))
		if err := s.Complete(ctx, id, Result{StatusCode: 200, Headers: http.Header{}}, 1); err != nil {
			t.Fatal(err)
		}
		if _, err := s.wdb.ExecContext(ctx,
			`UPDATE job_results SET completed_at = 1 WHERE id = ?`, id); err != nil {
			t.Fatal(err)
		}
	}

	// A chunk smaller than the population forces several passes.
	n, err := s.PurgeResults(ctx, 100, 4)
	if err != nil {
		t.Fatal(err)
	}
	if n != results {
		t.Errorf("PurgeResults removed %d rows across chunked passes, want %d", n, results)
	}
}

func TestPurgeResultsEmptyTable(t *testing.T) {
	s := newQueueStore(t)
	n, err := s.PurgeResults(context.Background(), 100, 10)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("PurgeResults removed %d rows from an empty table, want 0", n)
	}
}

func TestPurgeResultsOnClosedStore(t *testing.T) {
	s, err := OpenStore(tempDB(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := s.PurgeResults(context.Background(), 1, 10); err == nil {
		t.Error("PurgeResults on a closed store reported success")
	}
}

// nextEnqueueTime hands out strictly increasing timestamps, so that Pending's
// created_at ordering is deterministic rather than dependent on clock
// resolution.
var enqueueClock int64

func nextEnqueueTime() int64 {
	enqueueClock++
	return enqueueClock
}
